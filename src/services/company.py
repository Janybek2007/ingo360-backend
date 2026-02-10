from typing import TYPE_CHECKING, Any, Sequence

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError

from src.db.models import Company, ImportLogs, RegistrationApplication, User
from src.mapping.companies import company_mapping
from src.schemas.company import (
    CompanyCreate,
    CompanyUpdate,
    RegistrationApplicationCreate,
    RegistrationApplicationUpdate,
)
from src.utils.excel_parser import parse_excel_file
from src.utils.mapping import map_record
from src.websocket.connection_manager import connection_manager

from .base import BaseService, FilterSchemaType, ModelType
from ..utils.list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class CompanyService(BaseService[Company, CompanyCreate, CompanyUpdate]):
    async def get(
        self,
        session: "AsyncSession",
        item_id: int,
        load_options: list[Any] | None = None,
    ) -> ModelType | None:
        stmt = (
            select(
                Company,
                func.count(User.id).filter(User.is_active).label("active_users"),
            )
            .where(Company.id == item_id)
            .outerjoin(User, User.company_id == Company.id)
            .group_by(Company.id)
        )

        if load_options:
            stmt = stmt.options(*load_options)

        result = await session.execute(stmt)
        row = result.one_or_none()

        if row:
            company, active_users = row
            company.active_users = active_users
            return company

        return None

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: FilterSchemaType | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[ModelType]:
        stmt = select(self.model)
        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "active_users_limit": self.model.active_users_limit,
            "contract_number": self.model.contract_number,
            "ims_name": self.model.ims_name,
            "name": self.model.name,
            "status": self.model.is_active,
        }

        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.id.desc(),
        )
        if filters:
            if filters.search:
                search_term = f"%{filters.search}%"
                stmt = stmt.where(
                    or_(
                        self.model.name.ilike(search_term),
                        self.model.ims_name.ilike(search_term),
                        self.model.contract_number.ilike(search_term),
                    )
                )

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        companies = result.scalars().all()

        if not companies:
            return []

        company_ids = [c.id for c in companies]
        count_stmt = (
            select(User.company_id, func.count(User.id).label("active_users"))
            .where(User.company_id.in_(company_ids), User.is_active)
            .group_by(User.company_id)
        )

        count_result = await session.execute(count_stmt)
        counts_dict = {row[0]: row[1] for row in count_result.all()}

        for company in companies:
            company.active_users = counts_dict.get(company.id, 0)

        return companies

    async def import_excel(
        self, session: "AsyncSession", file: "UploadFile", user_id: int
    ):
        records = await parse_excel_file(file)

        import_log = ImportLogs(
            uploaded_by=user_id,
            target_table="Компании",
            records_count=len(records),
        )
        session.add(import_log)
        await session.flush()

        data_to_insert = []
        for r in records:
            relation_fields = {
                "import_log_id": import_log.id,
            }
            data_to_insert.append(map_record(r, company_mapping, relation_fields))
        await session.execute(insert(self.model), data_to_insert)
        await session.commit()

    @staticmethod
    async def get_active_company_users(
        session: "AsyncSession", company_id: int
    ) -> Sequence[int]:
        stmt = select(User.id).where(User.company_id == company_id, User.is_active)
        result = await session.execute(stmt)
        return result.scalars().all()

    async def update(
        self,
        session: "AsyncSession",
        item_id: int,
        obj_in: CompanyUpdate,
        load_options: list[Any] | None = None,
    ) -> Company:

        company = await self.get_or_404(session, item_id)
        update_data = obj_in.model_dump(exclude_unset=True)

        will_deactivate = (
            "is_active" in update_data
            and not update_data["is_active"]
            and company.is_active
        )

        will_activate = (
            "is_active" in update_data
            and update_data["is_active"]
            and not company.is_active
        )

        access_changes = self._check_access_changes(company, update_data)

        for field, value in update_data.items():
            setattr(company, field, value)

        try:
            if will_deactivate:
                await self._deactivate_company_users(session, company.id)

            if will_activate:
                await self._activate_company_users(session, company.id)

            await session.flush()

            if load_options:
                await session.refresh(company, load_options)
            else:
                await session.refresh(company)

            await session.commit()

            if will_deactivate:
                await self._notify_company_deactivation(session, company.id)

            if access_changes:
                await self._notify_access_changes(session, company.id, access_changes)

            return company

        except IntegrityError as e:
            await session.rollback()
            error_type = type(e.orig.__cause__).__name__

            if "ForeignKey" in error_type:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Связанная запись не найдена",
                )
            elif "Unique" in error_type:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"{self.model.__name__} с такими данными уже существует",
                )
            elif "NotNull" in error_type:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Обязательное поле не заполнено",
                )
            else:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Ошибка целостности данных",
                )
        except Exception:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Внутренняя ошибка сервера",
            )

    @staticmethod
    def _check_access_changes(company: Company, update_data: dict) -> list[str]:
        access_changes = []

        access_mapping = {
            "can_primary_sales": "primary",
            "can_secondary_sales": "secondary",
            "can_tertiary_sales": "tertiary",
            "can_visits": "visit",
            "can_market_analysis": "market",
        }

        for field, display_name in access_mapping.items():
            if field in update_data:
                old_value = getattr(company, field)
                new_value = update_data[field]

                if old_value and not new_value:
                    access_changes.append(display_name)

        return access_changes

    async def _notify_access_changes(
        self, session: "AsyncSession", company_id: int, access_changes: list[str]
    ):
        user_ids = await self.get_active_company_users(session, company_id)

        for access_type in access_changes:
            for user_id in user_ids:
                await connection_manager.send_company_access_revoked(
                    user_id, access_type
                )

    async def _notify_company_deactivation(
        self, session: "AsyncSession", company_id: int
    ):
        user_ids = await self.get_active_company_users(session, company_id)

        await connection_manager.notify_users(
            user_ids,
            notification_type="company_deactivated",
            message="Ваша компания была деактивирована",
        )

    @staticmethod
    async def _deactivate_company_users(session: "AsyncSession", company_id: int):
        stmt = (
            update(User)
            .where(User.company_id == company_id, User.is_active)
            .values(is_active=False)
        )

        await session.execute(stmt)

    @staticmethod
    async def _activate_company_users(session: "AsyncSession", company_id: int):
        stmt = (
            update(User)
            .where(User.company_id == company_id, User.is_active.is_(False))
            .values(is_active=True)
        )

        await session.execute(stmt)


class RegistrationApplicationService(
    BaseService[
        RegistrationApplication,
        RegistrationApplicationCreate,
        RegistrationApplicationUpdate,
    ]
): ...


company_service = CompanyService(Company)
registration_application_service = RegistrationApplicationService(
    RegistrationApplication
)
