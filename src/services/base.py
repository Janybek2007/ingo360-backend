from typing import TYPE_CHECKING, Any, Generic, Sequence, Type, TypeVar

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from src.utils.case_insensitive_dict import CaseInsensitiveDict
from src.utils.list_query_helper import ListQueryHelper

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


ModelType = TypeVar("ModelType")
CreateSchemaType = TypeVar("CreateSchemaType")
UpdateSchemaType = TypeVar("UpdateSchemaType")
FilterSchemaType = TypeVar("FilterSchemaType")


class BaseService(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        self.model = model

    async def create(
        self,
        session: "AsyncSession",
        obj_in: CreateSchemaType,
        load_options: list[Any] | None = None,
    ) -> ModelType:
        obj_data = obj_in.model_dump()
        db_obj = self.model(**obj_data)
        session.add(db_obj)

        try:
            await session.commit()
            await session.refresh(db_obj)

            if load_options:
                stmt = select(self.model).options(*load_options)
                stmt = stmt.where(self.model.id == int(db_obj.id))
                result = await session.execute(stmt)
                db_obj = result.unique().scalar_one()

            return db_obj
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

    async def get(
        self,
        session: "AsyncSession",
        item_id: int,
        load_options: list[Any] | None = None,
    ) -> ModelType | None:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = stmt.where(self.model.id == item_id)

        result = await session.execute(stmt)
        return result.unique().scalar_one_or_none()

    async def get_or_404(
        self,
        session: "AsyncSession",
        item_id: int,
        load_options: list[Any] | None = None,
    ) -> ModelType:
        db_obj = await self.get(session, item_id, load_options)

        if db_obj is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"{self.model.__name__} с id {item_id} не найден",
            )

        return db_obj

    async def update(
        self,
        session: "AsyncSession",
        item_id: int,
        obj_in: UpdateSchemaType,
        load_options: list[Any] | None = None,
    ) -> ModelType:
        db_obj = await self.get_or_404(session, item_id)
        update_data = obj_in.model_dump(exclude_unset=True)

        for field, value in update_data.items():
            setattr(db_obj, field, value)

        try:
            await session.commit()
            await session.refresh(db_obj)

            if load_options:
                db_obj = await self.get(session, item_id, load_options)

            return db_obj
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

    async def delete(self, session: "AsyncSession", item_id: int) -> None:
        try:
            db_obj = await self.get_or_404(session, item_id)

            await session.delete(db_obj)
            await session.commit()

        except IntegrityError as e:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Невозможно удалить запись: существуют связанные данные",
            ) from e

        except SQLAlchemyError as e:
            await session.rollback()
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ошибка при удалении записи из базы данных",
            ) from e

    async def get_multi(
        self,
        session: "AsyncSession",
        filters: FilterSchemaType | None = None,
        load_options: list[Any] | None = None,
    ) -> Sequence[ModelType]:
        stmt = select(self.model)

        if load_options:
            stmt = stmt.options(*load_options)

        stmt = stmt.order_by(self.model.created_at.desc())

        if filters:
            stmt = ListQueryHelper.apply_pagination(stmt, filters.limit, filters.offset)

        result = await session.execute(stmt)
        return result.unique().scalars().all()

    @staticmethod
    async def get_id_map(
        session: "AsyncSession",
        model: Type[ModelType],
        field: str,
        values: set[str] | set[tuple[str, int]],
        filter_field: str | None = None,
        filter_values: set[int] | None = None,
    ):
        if filter_field and filter_values:
            names_only = {v[0] for v in values}
            stmt = select(model).where(
                getattr(model, field).in_(names_only),
                getattr(model, filter_field).in_(filter_values),
            )
        else:
            stmt = select(model).where(getattr(model, field).in_(values))

        result = await session.execute(stmt)
        objs = result.scalars().all()

        if filter_field and filter_values:
            obj_map = CaseInsensitiveDict(
                {
                    (getattr(obj, field), getattr(obj, filter_field)): obj.id
                    for obj in objs
                }
            )
            missing = {v for v in values if (v[0], v[1]) not in obj_map}
        else:
            obj_map = CaseInsensitiveDict({getattr(obj, field): obj.id for obj in objs})
            missing = {v for v in values if v not in obj_map}

        return obj_map, missing

    async def get_field_id_pairs(
        self, session: "AsyncSession", field: str, company_id: int | None = None
    ):
        stmt = select(getattr(self.model, field), self.model.id)

        if company_id:
            stmt = stmt.where(self.model.company_id == company_id)

        result = await session.execute(stmt)
        return result.mappings().all()
