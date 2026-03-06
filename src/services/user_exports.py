from typing import TYPE_CHECKING, AsyncIterator

from sqlalchemy import func, or_, select
from sqlalchemy.orm import joinedload

from src.db.models import User
from src.schemas.user import UserFilter
from src.utils.list_query_helper import ListQueryHelper, SearchSpec, StringTypedSpec

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class UserClientsExportService:
    def __init__(self, model=None):
        self.model = model

    async def iter_multi(
        self,
        session: "AsyncSession",
        *,
        filters: UserFilter | None = None,
        load_options: list | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[User]:
        stmt = (
            select(User)
            .where(~User.is_admin, ~User.is_operator, ~User.is_superuser)
            .options(joinedload(User.company))
        )

        full_name = func.trim(
            func.concat_ws(
                " ",
                User.last_name,
                User.first_name,
                User.patronymic,
            )
        )

        sort_map = {
            "full_name": full_name,
            "position": User.position,
            "company": User.company_id,
            "email": User.email,
            "is_active": User.is_active,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            User.created_at.desc(),
        )

        if filters:
            if filters.is_active is not None:
                stmt = stmt.where(User.is_active.is_(filters.is_active))

            if filters.search:
                search_term = f"%{filters.search}%"
                search_conditions = [
                    User.first_name.ilike(search_term),
                    User.patronymic.ilike(search_term),
                    User.last_name.ilike(search_term),
                    User.position.ilike(search_term),
                    User.email.ilike(search_term),
                ]
                stmt = stmt.where(or_(*search_conditions))

            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(full_name, filters.full_name),
                    StringTypedSpec(User.position, filters.position),
                    StringTypedSpec(User.email, filters.email),
                ],
            )
            stmt = ListQueryHelper.apply_in_or_null(
                stmt, User.company_id, filters.company_ids
            )

        stream = await session.stream_scalars(
            stmt.execution_options(yield_per=chunk_size)
        )
        async for item in stream:
            yield item


class UserAdminsOperatorsExportService:
    def __init__(self, model=None):
        self.model = model

    async def iter_multi(
        self,
        session: "AsyncSession",
        *,
        filters: UserFilter | None = None,
        load_options: list | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[User]:
        stmt = (
            select(User)
            .where(or_(User.is_admin, User.is_operator))
            .options(joinedload(User.company))
        )

        full_name = func.trim(
            func.concat_ws(
                " ",
                User.last_name,
                User.first_name,
                User.patronymic,
            )
        )

        if filters:
            if filters.is_active is not None:
                stmt = stmt.where(User.is_active.is_(filters.is_active))

            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(full_name, filters.full_name),
                    StringTypedSpec(User.email, filters.email),
                    (
                        SearchSpec(
                            filters.search,
                            [
                                User.first_name,
                                User.patronymic,
                                User.last_name,
                                User.email,
                            ],
                        )
                        if filters.search
                        else None
                    ),
                ],
            )

            if filters.role == "admin" or filters.role == "administrator":
                stmt = stmt.where(User.is_admin.is_(True))
            elif filters.role == "operator":
                stmt = stmt.where(User.is_operator.is_(True))

            if filters.sort_by and filters.sort_order:
                role_value = func.case(
                    (User.is_admin.is_(True), 2),
                    (User.is_operator.is_(True), 1),
                    else_=0,
                )
                sort_map = {
                    "full_name": full_name,
                    "role": role_value,
                    "email": User.email,
                    "is_active": User.is_active,
                }
                sort_column = sort_map.get(filters.sort_by)
                if sort_column is not None:
                    stmt = stmt.order_by(
                        sort_column.asc()
                        if filters.sort_order == "ASC"
                        else sort_column.desc()
                    )

        stream = await session.stream_scalars(
            stmt.execution_options(yield_per=chunk_size)
        )
        async for item in stream:
            yield item


user_clients_export_service = UserClientsExportService()
user_admins_operators_export_service = UserAdminsOperatorsExportService()
