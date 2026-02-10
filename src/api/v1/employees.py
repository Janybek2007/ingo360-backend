from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.api.utils.export_excel import export_excel_response
from src.db.models import Employee, User
from src.db.session import db_session
from src.schemas import employee as employee_schema
from src.schemas.export import ExportExcelRequest
from src.services import employee as employee_service

router = APIRouter(dependencies=[Depends(current_operator_user)])


@router.post("/employees/create", response_model=employee_schema.EmployeeResponse)
async def create_employee(
    employee: employee_schema.EmployeeCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Employee.position),
        joinedload(Employee.product_group),
        joinedload(Employee.region),
        joinedload(Employee.district),
        joinedload(Employee.company),
    ]
    return await employee_service.employee_service.create(
        session, employee, load_options=load_options
    )


@router.post("/employees", response_model=list[employee_schema.EmployeeResponse])
async def get_employees(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: employee_schema.EmployeeListRequest,
):
    load_options = [
        joinedload(Employee.position),
        joinedload(Employee.product_group),
        joinedload(Employee.region),
        joinedload(Employee.district),
        joinedload(Employee.company),
    ]
    return await employee_service.employee_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/employees/export-excel")
async def export_employees_excel(
    payload: ExportExcelRequest,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Employee.position),
        joinedload(Employee.product_group),
        joinedload(Employee.region),
        joinedload(Employee.district),
        joinedload(Employee.company),
    ]
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: employee_service.employee_service.get_multi(
            session, load_options=load_options
        ),
        serialize=lambda e: employee_schema.EmployeeResponse.model_validate(
            e
        ).model_dump(),
    )


@router.get("/employees/{employee_id}", response_model=employee_schema.EmployeeResponse)
async def get_employee(
    employee_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(Employee.position),
        joinedload(Employee.product_group),
        joinedload(Employee.region),
        joinedload(Employee.district),
        joinedload(Employee.company),
    ]
    return await employee_service.employee_service.get_or_404(
        session, employee_id, load_options=load_options
    )


@router.post("/employees/import-excel")
async def bulk_insert_employees(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await employee_service.employee_service.import_excel(
        session, file, user_id=current_user.id
    )


@router.patch(
    "/employees/{employee_id}", response_model=employee_schema.EmployeeResponse
)
async def update_employee(
    employee_id: int,
    employee: employee_schema.EmployeeUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Employee.position),
        joinedload(Employee.product_group),
        joinedload(Employee.region),
        joinedload(Employee.district),
        joinedload(Employee.company),
    ]
    return await employee_service.employee_service.update(
        session, employee_id, employee, load_options=load_options
    )


@router.delete("/employees/{employee_id}")
async def delete_employee(
    employee_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await employee_service.employee_service.delete(session, employee_id)


@router.post("/positions/create", response_model=employee_schema.PositionResponse)
async def create_position(
    position: employee_schema.PositionCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await employee_service.position_service.create(session, position)


@router.post("/positions", response_model=list[employee_schema.PositionResponse])
async def get_positions(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: employee_schema.PositionListRequest,
):
    return await employee_service.position_service.get_multi(session, filters=filters)


@router.post("/positions/export-excel")
async def export_positions_excel(
    payload: ExportExcelRequest,
    session: Annotated["AsyncSession", Depends(db_session.get_session)],
):
    return await export_excel_response(
        payload=payload,
        get_rows=lambda: employee_service.position_service.get_multi(session),
        serialize=lambda p: employee_schema.PositionResponse.model_validate(
            p
        ).model_dump(),
    )


@router.get("/positions/{position_id}", response_model=employee_schema.PositionResponse)
async def get_position(
    position_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await employee_service.position_service.get_or_404(session, position_id)


@router.patch(
    "/positions/{position_id}", response_model=employee_schema.PositionResponse
)
async def update_position(
    position_id: int,
    position: employee_schema.PositionUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await employee_service.position_service.update(
        session, position_id, position
    )


@router.delete("/positions/{position_id}")
async def delete_position(
    position_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await employee_service.position_service.delete(session, position_id)


@router.post("/positions/import-excel")
async def bulk_insert_positions(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    await employee_service.position_service.import_excel(
        session, file, user_id=current_user.id
    )
