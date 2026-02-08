from fastapi import APIRouter

from src.core.auth.fastapi_users import fastapi_users
from src.api.dependencies.backend import authentication_backend

router = APIRouter()

router.include_router(
    fastapi_users.get_auth_router(authentication_backend),
)

router.include_router(
    fastapi_users.get_reset_password_router(),
)
