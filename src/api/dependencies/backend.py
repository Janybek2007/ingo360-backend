from fastapi_users.authentication import AuthenticationBackend

from src.core.auth.transport import bearer_transport
from src.api.dependencies.strategy import get_database_strategy


authentication_backend = AuthenticationBackend(
    name='access-tokens-db',
    transport=bearer_transport,
    get_strategy=get_database_strategy,
)
