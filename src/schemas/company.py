from datetime import date

from pydantic import BaseModel, ConfigDict, EmailStr

from src.schemas.base_filter import BaseFilter


class CompanyCreate(BaseModel):
    name: str
    ims_name: str | None = None
    active_users_limit: int
    can_primary_sales: bool = True
    can_secondary_sales: bool = True
    can_tertiary_sales: bool = True
    can_visits: bool = True
    can_market_analysis: bool = True
    contract_number: str
    contract_end_date: date
    address: str | None = None


class CompanyUpdate(BaseModel):
    name: str | None = None
    ims_name: str | None = None
    active_users_limit: int | None = None
    can_primary_sales: bool | None = None
    can_secondary_sales: bool | None = None
    can_tertiary_sales: bool | None = None
    can_visits: bool | None = None
    can_market_analysis: bool | None = None
    contract_number: str | None = None
    contract_end_date: date | None = None
    is_active: bool | None = None
    address: str | None = None


class CompanySimpleResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    can_primary_sales: bool
    can_secondary_sales: bool
    can_tertiary_sales: bool
    can_visits: bool
    can_market_analysis: bool


class CompanyResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    name: str
    ims_name: str | None = None
    active_users_limit: int
    active_users: int = 0
    can_primary_sales: bool
    can_secondary_sales: bool
    can_tertiary_sales: bool
    can_visits: bool
    can_market_analysis: bool
    contract_number: str
    contract_end_date: date
    is_active: bool
    address: str | None


class RegistrationApplicationCreate(BaseModel):
    owner_name: str
    company_name: str
    email: EmailStr


class RegistrationApplicationUpdate(BaseModel):
    owner_name: str | None = None
    company_name: str | None = None
    email: EmailStr | None = None


class RegistrationApplicationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: int
    owner_name: str
    company_name: str
    email: EmailStr


class CompanyFilter(BaseFilter):
    search: str | None = None
    is_active: bool | None = None
