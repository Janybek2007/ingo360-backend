from fastapi_users import schemas
from pydantic import BaseModel, EmailStr

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


