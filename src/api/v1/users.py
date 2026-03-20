from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy import select
from sqlalchemy.orm import joinedload

from src.api.dependencies.current_user import (
    current_active_user,
    current_admin_or_operator_user,
    current_admin_user,
)
from src.api.dependencies.user_manager import get_user_manager
from src.core.auth.user_manager import UserManager
from src.db.models import Company
from src.db.models.users import User
from src.db.session import db_session
from src.schemas.export import ExportExcelRequest
from src.schemas.user import (
    PasswordChange,
    PasswordSetup,
    UserAdminUpdate,
    UserCreateWithoutPassword,
    UserFilter,
    UserRead,
    UserUpdate,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession


router = APIRouter()


@router.post("", response_model=UserRead)
async def create_user(
    user_create: UserCreateWithoutPassword,
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
    current_user: Annotated["User", Depends(current_admin_user)],
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    company_id = user_create.company_id

    if company_id is not None and (
        not user_create.is_admin or not user_create.is_operator
    ):
        try:
            await user_manager.check_company_limit(session, company_id)
        except Exception:
            raise

    created_user = await user_manager.create_with_permission(
        user_create=user_create, creator=current_user, session=session
    )
    await session.refresh(created_user, ["company"])
    return created_user


@router.post("/set-password")
async def set_password(
    password_data: PasswordSetup,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
):
    user = await user_manager.set_password_by_token(
        token=password_data.token, new_password=password_data.password, session=session
    )

    return {"message": "Пароль успешно установлен", "user_id": user.id}


@router.post("/resend-password-setup")
async def resend_password_setup(
    email: str,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
):
    await user_manager.resend_password_setup(email, session)

    return {"message": "Если email существует, письмо будет отправлено"}


@router.get(
    "", response_model=list[UserRead], dependencies=[Depends(current_admin_user)]
)
async def get_users(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
    filters: Annotated[UserFilter, Query()],
):
    load_options = [joinedload(User.company)]
    return await user_manager.get_all(
        session, load_options=load_options, filters=filters
    )


@router.get(
    "/clients",
    response_model=list[UserRead],
    dependencies=[Depends(current_admin_user)],
)
async def get_client(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
    filters: Annotated[UserFilter, Query()],
):
    load_options = [joinedload(User.company)]
    return await user_manager.get_clients(
        session, load_options=load_options, filters=filters
    )


@router.post(
    "/clients/export-excel", dependencies=[Depends(current_admin_or_operator_user)]
)
async def export_clients_excel(
    payload: ExportExcelRequest,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    current_user: Annotated["User", Depends(current_admin_or_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.user_exports.UserClientsExportService",
        model_path="src.db.models.User",
        serializer_path="src.schemas.user.UserRead",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.get(
    "/admins-operators",
    response_model=list[UserRead],
    dependencies=[Depends(current_admin_or_operator_user)],
)
async def get_admins_operators(
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
    filters: Annotated[UserFilter, Query()],
):
    load_options = [joinedload(User.company)]
    return await user_manager.get_admins_and_operators(
        session, filters=filters, load_options=load_options
    )


@router.post(
    "/admins-operators/export-excel",
    dependencies=[Depends(current_admin_or_operator_user)],
)
async def export_admins_operators_excel(
    payload: ExportExcelRequest,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    current_user: Annotated["User", Depends(current_admin_or_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.user_exports.UserAdminsOperatorsExportService",
        model_path="src.db.models.User",
        serializer_path="src.schemas.user.UserRead",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.get("/me", response_model=UserRead)
async def get_me(
    current_user: Annotated["User", Depends(current_active_user)],
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    await session.refresh(current_user, ["company"])

    return current_user


@router.patch("/me", response_model=UserRead)
async def update_me(
    user_update: UserUpdate,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    current_user: Annotated["User", Depends(current_active_user)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
    request: Request,
):
    try:
        updated_user = await user_manager.update(
            user_update, current_user, request=request
        )
        await session.refresh(updated_user, ["company"])

        return updated_user

    except Exception:
        raise


@router.get(
    "/{user_id}", response_model=UserRead, dependencies=[Depends(current_admin_user)]
)
async def get_user(
    user_id: int,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
):
    load_options = [joinedload(User.company)]
    return await user_manager.get_user_by_id(
        session, user_id, load_options=load_options
    )


@router.patch(
    "/{user_id}", response_model=UserRead, dependencies=[Depends(current_admin_user)]
)
async def update_user(
    user_id: int,
    user_update: UserAdminUpdate,
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
    request: Request,
):
    try:
        user = await user_manager.get(user_id)

        update_data = user_update.model_dump(exclude_unset=True)

        final_company_id: int | None = update_data.get("company_id", user.company_id)

        is_being_activated = not user.is_active and update_data.get("is_active", False)
        is_changing_company = (
            "company_id" in update_data and update_data["company_id"] != user.company_id
        )

        if (is_being_activated or is_changing_company) and final_company_id is not None:
            await user_manager.check_company_limit(session, final_company_id)

        if is_being_activated and final_company_id is not None:
            stmt = select(Company.is_active).where(Company.id == final_company_id)
            result = await session.execute(stmt)
            company_is_active = result.scalar_one_or_none()

            if company_is_active is None:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail="Компания с указанным ID не найдена",
                )

            if not company_is_active:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Невозможно активировать пользователя неактивной компании",
                )

        updated_user = await user_manager.update(
            user_update, user, request=request, safe=False
        )
        await session.refresh(updated_user, ["company"])

        return updated_user

    except Exception:
        raise


@router.delete("/{user_id}", dependencies=[Depends(current_admin_user)])
async def delete_user(
    user_id: int,
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
):
    user = await user_manager.get(user_id)
    await user_manager.delete(user)


@router.post("/me/change-password")
async def change_password(
    password_data: PasswordChange,
    user: Annotated[User, Depends(current_active_user)],
    user_manager: Annotated[UserManager, Depends(get_user_manager)],
):
    is_valid = user_manager.password_helper.verify_and_update(
        password_data.old_password, user.hashed_password
    )
    if not is_valid[0]:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Неверный старый пароль"
        )

    hashed_password = user_manager.password_helper.hash(password_data.new_password)
    update_dict = {"hashed_password": hashed_password}

    await user_manager.user_db.update(user, update_dict)

    return {"message": "Пароль успешно изменен"}
