import os
import re
from uuid import uuid4
from pathlib import Path
from typing import TYPE_CHECKING

from fastapi import HTTPException, UploadFile, status
from sqlalchemy import (
    and_,
    asc,
    case,
    desc,
    distinct,
    func,
    insert,
    literal,
    or_,
    select,
    union,
)

from src.db.models import IMS, Brand, Company, ImportLogs
from src.mapping.ims import ims_mapping
from src.schemas.ims import (
    IMSCreate,
    IMSTableFilter,
    IMSTopFilter,
    IMSUpdate,
)
from src.services.base import BaseService
from src.utils.excel_parser import iter_excel_records
from src.utils.mapping import map_record

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


class IMSMetricsService(BaseService[IMS, IMSCreate, IMSUpdate]):
    async def import_excel(
        self,
        session: "AsyncSession",
        file: "UploadFile",
        user_id: int,
        batch_size: int = 2000,
    ):
        from src.tasks.sale_imports import import_sales_task

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
            return {"task_id": task.id}
        except Exception:
            if file_path.exists():
                os.remove(file_path)
            raise

    async def _import_excel_from_file(
        self,
        session: "AsyncSession",
        file_path: str,
        user_id: int,
        batch_size: int = 2000,
    ):
        with open(file_path, "rb") as f:
            from tempfile import SpooledTemporaryFile

            temp = SpooledTemporaryFile(max_size=50 * 1024 * 1024)
            temp.write(f.read())
            temp.seek(0)

        try:
            total_records = 0
            for _ in iter_excel_records(temp):
                total_records += 1

            temp.seek(0)

            import_log = ImportLogs(
                uploaded_by=user_id,
                target_table="IMS",
                records_count=total_records,
            )
            session.add(import_log)
            await session.flush()

            data_to_insert = []
            imported_count = 0

            for _, record in iter_excel_records(temp):
                relation_fields = {"import_log_id": import_log.id}
                data_to_insert.append(map_record(record, ims_mapping, relation_fields))

                if len(data_to_insert) >= batch_size:
                    await session.execute(insert(self.model), data_to_insert)
                    imported_count += len(data_to_insert)
                    data_to_insert = []

            if data_to_insert:
                await session.execute(insert(self.model), data_to_insert)
                imported_count += len(data_to_insert)

            await session.commit()
            return {"imported": imported_count, "total": total_records}
        finally:
            temp.close()

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

    def parse_period(self, period: str, type_period: str) -> list[str]:
        if type_period == "Month":
            return self._parse_single_month(period)
        elif type_period == "Quarter":
            return self._parse_quarter(period)
        elif type_period == "Year":
            return self._parse_year(period)
        elif type_period == "MAT":
            return self._parse_mat_simple(period)
        elif type_period == "YTD":
            return self._parse_ytd_simple(period)
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Неизвестный тип периода: {type_period}",
            )

    async def get_entities_with_metrics(
        self,
        session: "AsyncSession",
        filters: IMSTopFilter,
        company_id: int | None,
    ):
        periods = []
        for period in filters.periods:
            periods.extend(self.parse_period(period, filters.type_period))

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

        previous_periods = self._get_previous_periods(
            filters.periods, filters.type_period
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

    def _get_previous_periods(self, periods: list[str], type_period: str) -> list[str]:
        previous_periods = []

        for period in periods:
            if type_period == "Month":
                month, year = self._parse_month_year(period)
                prev_month = month - 1
                prev_year = year

                if prev_month < 1:
                    prev_month = 12
                    prev_year -= 1

                prev_period_str = f"{prev_month}-{prev_year % 100:02d}"
                prev_periods = self.parse_period(prev_period_str, type_period)
                previous_periods.extend(prev_periods)

            elif type_period == "Quarter":
                match = re.match(r"^q([1-4])-(\d{2,4})$", period.lower())
                if not match:
                    continue

                quarter = int(match.group(1))
                year = self._normalize_year(int(match.group(2)))

                prev_quarter = quarter - 1
                prev_year = year

                if prev_quarter < 1:
                    prev_quarter = 4
                    prev_year -= 1

                prev_period_str = f"q{prev_quarter}-{prev_year % 100:02d}"
                prev_periods = self.parse_period(prev_period_str, type_period)
                previous_periods.extend(prev_periods)

            elif type_period == "Year":
                year = self._normalize_year(int(period))
                prev_year = year - 1

                prev_periods = self.parse_period(str(prev_year), type_period)
                previous_periods.extend(prev_periods)

            elif type_period == "MAT":
                month, year = self._parse_month_year(period)
                prev_year = year - 1

                prev_period_str = f"{month}-{prev_year % 100:02d}"
                prev_periods = self.parse_period(prev_period_str, "MAT")
                previous_periods.extend(prev_periods)

            elif type_period == "YTD":
                month, year = self._parse_month_year(period)
                prev_year = year - 1

                prev_period_str = f"{month}-{prev_year % 100:02d}"
                prev_periods = self.parse_period(prev_period_str, "YTD")
                previous_periods.extend(prev_periods)

        return previous_periods

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

        if "periods" in filters.model_fields_set:
            for period in filters.periods:
                db_periods = self.parse_period(period, filters.type_period)

                period_amount = func.round(
                    func.sum(case((IMS.period.in_(db_periods), IMS.amount), else_=0))
                )

                period_packages = func.round(
                    func.sum(case((IMS.period.in_(db_periods), IMS.packages), else_=0))
                )

                period_json = func.json_build_object(
                    "amount", period_amount, "packages", period_packages
                ).label(period)

                period_columns.append(period_json)

        stmt = select(*select_fields, *period_columns).group_by(*group_by_fields)

        if filters.company_names:
            stmt = stmt.where(IMS.company.in_(filters.company_names))
        if filters.brand_names:
            stmt = stmt.where(IMS.brand.in_(filters.brand_names))
        if filters.segment_names:
            stmt = stmt.where(IMS.segment.in_(filters.segment_names))
        if filters.dosage_form_names:
            stmt = stmt.where(IMS.dosage_form.in_(filters.dosage_form_names))

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

        if filters.sort_by and filters.sort_order:
            dimension_mapping = {
                "company": IMS.company,
                "brand": IMS.brand,
                "segment": IMS.segment,
                "dosage_form": IMS.dosage_form,
                "dosage": IMS.dosage,
                "molecule": IMS.molecule,
            }
            sort_column = dimension_mapping.get(filters.sort_by)
            if sort_column is not None:
                stmt = stmt.order_by(
                    asc(sort_column)
                    if filters.sort_order == "ASC"
                    else desc(sort_column)
                )

        if filters.limit:
            stmt = stmt.limit(filters.limit)
        if filters.offset:
            stmt = stmt.offset(filters.offset)

        result = await session.execute(stmt)
        return result.mappings().all()

    async def get_field(self, session: "AsyncSession", field: str):
        stmt = select(distinct(getattr(self.model, field)))

        result = await session.execute(stmt)

        return result.scalars().all()


ims_service = IMSMetricsService(IMS)
