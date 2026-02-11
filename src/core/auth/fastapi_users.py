from fastapi_users import FastAPIUsers

from src.api.dependencies.backend import authentication_backend
from src.api.dependencies.user_manager import get_user_manager
from src.db.models import User

fastapi_users = FastAPIUsers[User, int](
    get_user_manager,
    [authentication_backend],
)
