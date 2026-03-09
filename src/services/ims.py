import os
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any, AsyncIterator
from uuid import uuid4

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import and_, case, distinct, func, insert, literal, or_, select, union

from src.db.models import IMS, Brand, Company, ImportLogs
from src.mapping.ims import ims_mapping
from src.schemas.ims import (
    IMSCreate,
    IMSRequest,
    IMSTableFilter,
    IMSTopFilter,
    IMSUpdate,
)
from src.services.base import BaseService, ModelType
from src.utils.build_period_values import build_period_values
from src.utils.excel_parser import iter_excel_records
from src.utils.import_result import build_import_result
from src.utils.list_query_helper import (
    InOrNullSpec,
    ListQueryHelper,
    NumberTypedSpec,
    StringTypedSpec,
)
from src.utils.mapping import map_record
from src.utils.validate_required_columns import validate_required_columns

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.base_filter import PaginatedResponse


class IMSMetricsService(BaseService[IMS, IMSCreate, IMSUpdate]):
    async def import_excel(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 2000,
    ):
        from src.db.models.excel_tasks import ExcelTaskType
        from src.tasks.sale_imports import create_excel_task_record, import_sales_task

        upload_dir = Path("temp")
        upload_dir.mkdir(exist_ok=True)
        file_path = upload_dir / f"{uuid4()}_{file.filename}"

        try:
            with open(file_path, "wb") as f:
                content = await file.read()
                f.write(content)

            task = import_sales_task.delay(
                file_path=str(file_path),
                user_id=user_id,
                service_path="src.services.ims.IMSMetricsService",
                model_path="src.db.models.IMS",
                batch_size=batch_size,
            )

            await create_excel_task_record(
                task_id=task.id,
                started_by=user_id,
                file_path="",
                task_type=ExcelTaskType.IMPORT,
            )
            return {"task_id": task.id}
        except Exception:
            if file_path.exists():
                os.remove(file_path)
            raise

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: IMSRequest | None = None,
        load_options: list[Any] | None = None,
    ) -> PaginatedResponse:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        sort_map = {
            "company": self.model.company,
            "brand": self.model.brand,
            "segment": self.model.segment,
            "dosage": self.model.dosage,
            "dosage_form": self.model.dosage_form,
            "molecule": self.model.molecule,
            "period": self.model.period,
            "amount": self.model.amount,
            "packages": self.model.packages,
        }
        stmt = ListQueryHelper.apply_sorting_with_default(
            stmt,
            getattr(filters, "sort_by", None),
            getattr(filters, "sort_order", None),
            sort_map,
            self.model.created_at.desc(),
        )

        if filters:
            stmt = ListQueryHelper.apply_specs(
                stmt,
                [
                    StringTypedSpec(self.model.company, filters.company),
                    StringTypedSpec(self.model.brand, filters.brand),
                    StringTypedSpec(self.model.segment, filters.segment),
                    StringTypedSpec(self.model.molecule, filters.molecule),
                    StringTypedSpec(self.model.dosage, filters.dosage),
                    StringTypedSpec(self.model.dosage_form, filters.dosage_form),
                    StringTypedSpec(self.model.period, filters.period),
                    NumberTypedSpec(self.model.amount, filters.amount),
                    NumberTypedSpec(self.model.packages, filters.packages),
                ],
            )
            # Count before pagination
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_count = await session.scalar(count_stmt)

            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)
        else:
            count_stmt = select(func.count()).select_from(stmt.subquery())
            total_count = await session.scalar(count_stmt)

        result = await session.execute(stmt)

        items = result.unique().scalars().all()

        hasPrev = filters.offset > 0 if filters else False
        hasNext = len(items) == filters.limit if filters and filters.limit else False

        return PaginatedResponse(
            result=items,
            hasPrev=hasPrev,
            hasNext=hasNext,
            count=total_count,
        )

    async def iter_multi(
        self,
        session: "AsyncSession",
        load_options: list[Any] | None = None,
        chunk_size: int = 1000,
    ) -> AsyncIterator[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = ListQueryHelper.apply_sorting_with_created(
            stmt,
            self.model.created_at.desc(),
        )

        stream = await session.stream_scalars(
            stmt.execution_options(yield_per=chunk_size)
        )
        async for item in stream:
            yield item

    async def _import_excel_from_file(
        self,
        session: "AsyncSession",
        file_path: str,
        user_id: int,
        batch_size: int = 2000,
    ):
        try:
            with open(file_path, "rb") as f:
                first_row = next(iter_excel_records(f), None)

            if first_row is None:
                raise HTTPException(status_code=400, detail="Файл пустой")

            _, first_record = first_row
            validate_required_columns(
                [first_record],
                {
                    "компания|company",
                    "бренд|brand",
                    "период|period",
                },
            )

            total_records = 0
            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="IMS",
                records_count=0,
                target_table_name=self.model.__tablename__,
            )
            session.add(import_log)
            await session.flush()

            data_to_insert = []
            imported_count = 0

            with open(file_path, "rb") as f:
                for _, record in iter_excel_records(f):
                    total_records += 1
                    relation_fields = {"import_log_id": import_log.id}
                    data_to_insert.append(
                        map_record(record, ims_mapping, relation_fields)
                    )

                    if len(data_to_insert) >= batch_size:
                        await session.execute(insert(self.model), data_to_insert)
                        imported_count += len(data_to_insert)
                        data_to_insert = []

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported_count += len(data_to_insert)

            import_log.records_count = total_records
            await session.commit()

            return build_import_result(
                total=total_records,
                imported=imported_count,
                skipped_records=[],
                inserted=imported_count,
                deduplicated=0,
            )
        finally:
            pass

    @staticmethod
    def _format_db_period(year: int, month: int) -> str:
        month_names = {
            1: "Январь",
            2: "Февраль",
            3: "Март",
            4: "Апрель",
            5: "Май",
            6: "Июнь",
            7: "Июль",
            8: "Август",
            9: "Сентябрь",
            10: "Октябрь",
            11: "Ноябрь",
            12: "Декабрь",
        }
        return f"{year}/{month:02d} {month_names[month]}"

    @staticmethod
    def _normalize_year(year: int) -> int:
        return 2000 + year if year < 100 else year

    def _parse_month_year(self, period_str: str) -> tuple[int, int]:
        parts = period_str.split("-")
        if len(parts) != 2:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неверный формат: {period_str}. Ожидается 'M-YY'",
            )

        try:
            month = int(parts[0])
            year = int(parts[1])
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неверный формат периода: {period_str}",
            )

        if month < 1 or month > 12:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неверный месяц: {month}",
            )

        return month, self._normalize_year(year)

    def _parse_single_month(self, period: str) -> list[str]:
        month, year = self._parse_month_year(period)
        return [self._format_db_period(year, month)]

    def _parse_quarter(self, period: str) -> list[str]:
        match = re.match(r"^q([1-4])-(\d{2,4})$", period.lower())
        if not match:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неверный формат квартала: {period}. Ожидается 'qN-YY'",
            )

        quarter = int(match.group(1))
        year = self._normalize_year(int(match.group(2)))

        start_month = (quarter - 1) * 3 + 1
        return [self._format_db_period(year, start_month + i) for i in range(3)]

    def _parse_year(self, period: str) -> list[str]:
        if not period.isdigit():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неверный формат года: {period}. Ожидается 'YYYY'",
            )
        year = self._normalize_year(int(period))
        return [self._format_db_period(year, m) for m in range(1, 13)]

    def _parse_mat_simple(self, period: str) -> list[str]:
        end_month, end_year = self._parse_month_year(period)

        result = []
        for i in range(12):
            m = end_month - 11 + i
            y = end_year

            while m <= 0:
                m += 12
                y -= 1
            while m > 12:
                m -= 12
                y += 1

            result.append(self._format_db_period(y, m))

        return result

    def _parse_ytd_simple(self, period: str) -> list[str]:
        end_month, year = self._parse_month_year(period)
        return [self._format_db_period(year, m) for m in range(1, end_month + 1)]

    def parse_period(self, period: str, group_by_period: str) -> list[str]:
        if group_by_period == "month":
            return self._parse_single_month(period)
        elif group_by_period == "quarter":
            return self._parse_quarter(period)
        elif group_by_period == "year":
            return self._parse_year(period)
        elif group_by_period == "mat":
            return self._parse_mat_simple(period)
        elif group_by_period == "ytd":
            return self._parse_ytd_simple(period)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неизвестный тип периода: {group_by_period}",
            )

    def _format_short_period(self, year: int, month: int) -> str:
        return f"{month}-{year % 100:02d}"

    def _expand_mat_period(self, year: int, month: int) -> list[str]:
        return self._parse_mat_simple(self._format_short_period(year, month))

    def _expand_ytd_period(self, year: int, month: int) -> list[str]:
        return self._parse_ytd_simple(self._format_short_period(year, month))

    def _expand_period_values(self, period_values, group_by_period: str) -> list[str]:
        group = (group_by_period or "month").strip().lower()

        if group == "year":
            years = period_values.years or []
            periods: list[str] = []
            for year in years:
                periods.extend(self._parse_year(str(year)))
            return periods

        if group == "quarter":
            periods: list[str] = []
            for year, quarter in period_values.quarters or []:
                periods.extend(self._parse_quarter(f"q{quarter}-{year % 100:02d}"))
            return periods

        if group == "mat":
            periods: list[str] = []
            for year, month in period_values.months or []:
                periods.extend(self._expand_mat_period(year, month))
            return periods

        if group == "ytd":
            periods: list[str] = []
            for year, month in period_values.months or []:
                periods.extend(self._expand_ytd_period(year, month))
            return periods

        periods: list[str] = []
        for year, month in period_values.months or []:
            periods.extend(
                self._parse_single_month(self._format_short_period(year, month))
            )
        return periods

    def _get_previous_periods_from_values(
        self, period_values, group_by_period: str
    ) -> list[str]:
        group = (group_by_period or "month").strip().lower()

        if group == "year":
            prev_values = [year - 1 for year in (period_values.years or [])]
            return self._expand_period_values(
                build_period_values("year", [str(year) for year in prev_values]),
                "year",
            )

        if group == "quarter":
            prev_quarters = []
            for year, quarter in period_values.quarters or []:
                prev_quarter = quarter - 1
                prev_year = year
                if prev_quarter < 1:
                    prev_quarter = 4
                    prev_year -= 1
                prev_quarters.append(f"q-{prev_year}-{prev_quarter}")
            prev_values = build_period_values("quarter", prev_quarters)
            if prev_values is None:
                return []
            return self._expand_period_values(prev_values, "quarter")

        if group == "mat":
            prev_periods = []
            for year, month in period_values.months or []:
                prev_periods.extend(self._expand_mat_period(year - 1, month))
            return prev_periods

        if group == "ytd":
            prev_periods = []
            for year, month in period_values.months or []:
                prev_periods.extend(self._expand_ytd_period(year - 1, month))
            return prev_periods

        prev_periods = []
        for year, month in period_values.months or []:
            prev_month = month - 1
            prev_year = year
            if prev_month < 1:
                prev_month = 12
                prev_year -= 1
            prev_periods.extend(
                self._parse_single_month(
                    self._format_short_period(prev_year, prev_month)
                )
            )
        return prev_periods

    def _format_period_label(self, period_values, group_by_period: str) -> str:
        group = (group_by_period or "month").strip().lower()

        if group == "year" and period_values.years:
            return str(period_values.years[0])

        if group == "quarter" and period_values.quarters:
            year, quarter = period_values.quarters[0]
            return f"{year}-Q{quarter}"

        if period_values.months:
            year, month = period_values.months[0]
            return f"{year}-{month:02d}"

        return period_values.base_periods[0] if period_values.base_periods else ""

    async def get_entities_with_metrics(
        self,
        session: "AsyncSession",
        filters: IMSTopFilter,
        company_id: int | None,
    ):
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )
        if period_values is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="period_values обязательны",
            )

        group_by_period = (filters.group_by_period or "month").strip().lower()
        periods = self._expand_period_values(period_values, group_by_period)

        if filters.group_column == "company":
            group_column = IMS.company
        elif filters.group_column == "brand":
            group_column = IMS.brand
        elif filters.group_column == "segment":
            group_column = IMS.segment
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неизвестный тип сущности: {filters.group_column}",
            )

        ranked_subquery = (
            select(
                group_column.label("entity"),
                func.round(func.sum(IMS.amount)).label("sales"),
                func.row_number()
                .over(order_by=func.sum(IMS.amount).desc())
                .label("rank"),
            )
            .where(IMS.period.in_(periods))
            .group_by(group_column)
        ).subquery()

        if company_id is not None and filters.group_column == "company":
            company_name_stmt = select(
                Company.ims_name.label("ims_name"), Company.name.label("name")
            ).where(Company.id == company_id)
            company_result = await session.execute(company_name_stmt)
            user_company_name = company_result.mappings().all()

            company_ims_name = user_company_name[0]["ims_name"]
            company_name = user_company_name[0]["name"]

            if company_ims_name:
                stmt = select(
                    ranked_subquery.c.rank,
                    ranked_subquery.c.entity,
                    ranked_subquery.c.sales,
                    case(
                        {ranked_subquery.c.entity == company_ims_name: True},
                        else_=False,
                    ).label("is_user_company"),
                ).order_by(ranked_subquery.c.rank)
            else:
                brands_stmt = select(Brand.ims_name).where(
                    Brand.company_id == company_id
                )
                brands_result = await session.execute(brands_stmt)
                company_brands = brands_result.scalars().all()

                if company_brands:
                    user_company_sales_subquery = (
                        select(
                            func.round(func.sum(IMS.amount)).label("total_sales")
                        ).where(
                            and_(IMS.brand.in_(company_brands), IMS.period.in_(periods))
                        )
                    ).scalar_subquery()

                    top_stmt = select(
                        ranked_subquery.c.entity,
                        ranked_subquery.c.sales,
                        literal(False).label("is_user_company"),
                    )

                    user_company_stmt = select(
                        literal(company_name).label("entity"),
                        user_company_sales_subquery.label("sales"),
                        literal(True).label("is_user_company"),
                    ).where(user_company_sales_subquery.is_not(None))

                    combined = union(top_stmt, user_company_stmt).subquery()

                    stmt = (
                        select(
                            func.row_number()
                            .over(order_by=combined.c.sales.desc())
                            .label("rank"),
                            combined.c.entity,
                            combined.c.sales,
                            combined.c.is_user_company,
                        )
                        .select_from(combined)
                        .order_by(combined.c.sales.desc())
                    )
                else:
                    stmt = select(
                        ranked_subquery.c.rank,
                        ranked_subquery.c.entity,
                        ranked_subquery.c.sales,
                        literal(False).label("is_user_company"),
                    ).order_by(ranked_subquery.c.rank)
        elif filters.segment_name and filters.group_column == "segment":
            stmt = select(
                ranked_subquery.c.rank,
                ranked_subquery.c.entity,
                ranked_subquery.c.sales,
                case(
                    {
                        func.upper(ranked_subquery.c.entity)
                        == func.upper(filters.segment_name): True
                    },
                    else_=False,
                ).label("is_user_entity"),
            ).order_by(ranked_subquery.c.rank)
        elif filters.brand_name and filters.group_column == "brand":
            stmt = select(
                ranked_subquery.c.rank,
                ranked_subquery.c.entity,
                ranked_subquery.c.sales,
                case(
                    {
                        func.upper(ranked_subquery.c.entity)
                        == func.upper(filters.brand_name): True
                    },
                    else_=False,
                ).label("is_user_entity"),
            ).order_by(ranked_subquery.c.rank)
        else:
            stmt = select(
                ranked_subquery.c.rank,
                ranked_subquery.c.entity,
                ranked_subquery.c.sales,
            ).order_by(ranked_subquery.c.rank)

            result = await session.execute(stmt)
            entities = result.mappings().all()

            response = {"entities": entities}

            response["metrics"] = {
                "sales": "-",
                "market_sales": "-",
                "market_share": "-",
                "growth_vs_previous": "-",
                "market_growth": "-",
                "growth_vs_market": "-",
            }

            return response

        result = await session.execute(stmt)
        entities = result.mappings().all()

        response = {"entities": entities}

        previous_periods = self._get_previous_periods_from_values(
            period_values, group_by_period
        )

        if filters.segment_name and filters.group_column == "segment":
            entity_filter = func.upper(IMS.segment) == func.upper(filters.segment_name)
        elif filters.brand_name and filters.group_column == "brand":
            entity_filter = func.upper(IMS.brand) == func.upper(filters.brand_name)
        elif company_id is not None:
            company_stmt = select(Company.ims_name).where(Company.id == company_id)
            company_result = await session.execute(company_stmt)
            company_ims_name = company_result.scalar_one_or_none()

            if company_ims_name:
                entity_filter = IMS.company == company_ims_name
            else:
                brands_stmt = select(Brand.ims_name).where(
                    Brand.company_id == company_id
                )
                brands_result = await session.execute(brands_stmt)
                company_brands = brands_result.scalars().all()

                if not company_brands:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="У компании нет IMS названия и нет брендов",
                    )

                entity_filter = IMS.brand.in_(company_brands)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Необходимо указать company_id, segment_name или brand_name",
            )

        entity_sales_stmt = select(func.sum(IMS.amount)).where(
            and_(entity_filter, IMS.period.in_(periods))
        )
        result = await session.execute(entity_sales_stmt)
        entity_sales = result.scalar() or 0.0

        market_sales_stmt = select(func.sum(IMS.amount)).where(IMS.period.in_(periods))
        result = await session.execute(market_sales_stmt)
        market_sales = result.scalar() or 0.0

        market_share = (entity_sales / market_sales * 100) if market_sales > 0 else 0.0

        prev_entity_sales_stmt = select(func.sum(IMS.amount)).where(
            and_(entity_filter, IMS.period.in_(previous_periods))
        )
        result = await session.execute(prev_entity_sales_stmt)
        prev_entity_sales = result.scalar() or 0.0

        prev_market_sales_stmt = select(func.sum(IMS.amount)).where(
            IMS.period.in_(previous_periods)
        )
        result = await session.execute(prev_market_sales_stmt)
        prev_market_sales = result.scalar() or 0.0

        growth_vs_previous = (
            ((entity_sales - prev_entity_sales) / prev_entity_sales * 100)
            if prev_entity_sales > 0
            else 0.0
        )

        market_growth = (
            ((market_sales - prev_market_sales) / prev_market_sales * 100)
            if prev_market_sales > 0
            else 0.0
        )

        growth_vs_market = growth_vs_previous - market_growth

        response["metrics"] = {
            "sales": round(entity_sales),
            "market_sales": round(market_sales),
            "market_share": market_share,
            "growth_vs_previous": growth_vs_previous,
            "market_growth": market_growth,
            "growth_vs_market": growth_vs_market,
        }

        return response

    async def get_table_data(
        self,
        session: "AsyncSession",
        filters: IMSTableFilter,
    ):
        dimension_mapping = {
            "company": IMS.company,
            "brand": IMS.brand,
            "segment": IMS.segment,
            "dosage_form": IMS.dosage_form,
            "dosage": IMS.dosage,
            "molecule": IMS.molecule,
        }

        select_fields = []
        group_by_fields = []

        for dim in filters.group_by_dimensions:
            column = dimension_mapping[dim]
            select_fields.append(column)
            group_by_fields.append(column)

        period_columns = []
        group_by_period = (filters.group_by_period or "month").strip().lower()
        period_values = build_period_values(
            filters.group_by_period, filters.period_values
        )

        if period_values and period_values.base_periods:
            for period in period_values.base_periods:
                single_values = build_period_values(group_by_period, [period])
                if single_values is None:
                    continue
                db_periods = self._expand_period_values(single_values, group_by_period)
                period_label = self._format_period_label(single_values, group_by_period)

                period_amount = func.round(
                    func.sum(case((IMS.period.in_(db_periods), IMS.amount), else_=0))
                )

                period_packages = func.round(
                    func.sum(case((IMS.period.in_(db_periods), IMS.packages), else_=0))
                )

                period_json = func.json_build_object(
                    "amount", period_amount, "packages", period_packages
                ).label(period_label)

                period_columns.append(period_json)

        stmt = select(*select_fields, *period_columns).group_by(*group_by_fields)

        if filters.search:
            search_term = f"%{filters.search}%"
            search_conditions = []

            if "company" in filters.group_by_dimensions:
                search_conditions.append(IMS.company.ilike(search_term))
            if "brand" in filters.group_by_dimensions:
                search_conditions.append(IMS.brand.ilike(search_term))
            if "segment" in filters.group_by_dimensions:
                search_conditions.append(IMS.segment.ilike(search_term))
            if "dosage_form" in filters.group_by_dimensions:
                search_conditions.append(IMS.dosage_form.ilike(search_term))
            if "dosage" in filters.group_by_dimensions:
                search_conditions.append(IMS.dosage.ilike(search_term))
            if "molecule" in filters.group_by_dimensions:
                search_conditions.append(IMS.molecule.ilike(search_term))

            if search_conditions:
                stmt = stmt.where(or_(*search_conditions))

        stmt = ListQueryHelper.apply_specs(
            stmt,
            [
                StringTypedSpec(IMS.company, filters.company),
                StringTypedSpec(IMS.brand, filters.brand),
                StringTypedSpec(IMS.segment, filters.segment),
                StringTypedSpec(IMS.dosage, filters.dosage),
                StringTypedSpec(IMS.dosage_form, filters.dosage_form),
                StringTypedSpec(IMS.molecule, filters.molecule),
                InOrNullSpec(IMS.company, filters.company_names),
                InOrNullSpec(IMS.brand, filters.brand_names),
                InOrNullSpec(IMS.segment, filters.segment_names),
                InOrNullSpec(IMS.dosage_form, filters.dosage_form_names),
                InOrNullSpec(IMS.dosage, getattr(filters, "dosage_names", None)),
                InOrNullSpec(IMS.molecule, getattr(filters, "molecule_names", None)),
            ],
        )

        sort_map = {
            "company": IMS.company,
            "brand": IMS.brand,
            "segment": IMS.segment,
            "dosage_form": IMS.dosage_form,
            "dosage": IMS.dosage,
            "molecule": IMS.molecule,
        }

        stmt = ListQueryHelper.apply_sorting(
            stmt, sort_map.get(filters.sort_by), filters.sort_order
        )
        stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.mappings().all()

    async def get_field(self, session: "AsyncSession", field: str):
        stmt = select(distinct(getattr(self.model, field)))

        result = await session.execute(stmt)

        return result.scalars().all()


ims_service = IMSMetricsService(IMS)
