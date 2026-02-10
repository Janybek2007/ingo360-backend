from typing import Literal

from fastapi_users import schemas
from pydantic import BaseModel, EmailStr

from .base_filter import SortDirection
from .company import CompanySimpleResponse


class UserRead(schemas.BaseUser[int]):
    first_name: str | None
    last_name: str | None
    patronymic: str | None
    phone_number: str | None
    is_operator: bool
    is_admin: bool
    company: CompanySimpleResponse | None
    position: str | None


class UserCreate(schemas.BaseUserCreate):
    first_name: str
    last_name: str
    patronymic: str | None = None
    company_id: int | None = None
    is_operator: bool = False
    is_admin: bool = False
    position: str | None = None


class UserCreateWithoutPassword(BaseModel):
    first_name: str
    last_name: str
    patronymic: str | None = None
    email: EmailStr
    company_id: int | None = None
    is_operator: bool | None = False
    is_admin: bool | None = False
    position: str | None = None


class UserUpdate(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    patronymic: str | None = None
    phone_number: str | None = None
    email: EmailStr | None = None


class UserAdminUpdate(schemas.BaseUserUpdate, UserUpdate):
    position: str | None = None
    company_id: int | None = None
    is_operator: bool | None = False
    is_admin: bool | None = False


class PasswordSetup(BaseModel):
    token: str
    password: str


class PasswordChange(BaseModel):
    old_password: str
    new_password: str


class UserFilter(BaseModel):
    is_active: bool | None = None
    search: str | None = None
    full_name: str | None = None
    position: str | None = None
    email: str | None = None
    company_ids: list[int] | None = None
    role: Literal["admin", "operator"] | None = None
    sort_by: (
        Literal[
            "full_name",
            "position",
            "company",
            "email",
            "is_active",
            "role",
        ]
        | None
    ) = None
    sort_order: SortDirection | None = None
