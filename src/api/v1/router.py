from fastapi import APIRouter

from src.api.v1 import (
    auth,
    clients,
    companies,
    employees,
    exports,
    filter_options,
    geography,
    health,
    import_logs,
    ims,
    products,
    sales,
    users,
    visits,
    websocket,
)

api_router = APIRouter()

api_router.include_router(health.router, prefix="/health", tags=["health"])
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(geography.router, prefix="/geography", tags=["geography"])
api_router.include_router(products.router, prefix="/products", tags=["products"])
api_router.include_router(employees.router, prefix="/employees", tags=["employees"])
api_router.include_router(clients.router, prefix="/clients", tags=["clients"])
api_router.include_router(companies.router, prefix="/companies", tags=["companies"])
api_router.include_router(sales.router, prefix="/sales", tags=["sales"])
api_router.include_router(visits.router, prefix="/visits", tags=["visits"])
api_router.include_router(
    import_logs.router, prefix="/import_logs", tags=["import logs"]
)
api_router.include_router(ims.router, prefix="/ims", tags=["ims"])
api_router.include_router(filter_options.router, tags=["filter options"])
api_router.include_router(exports.router, tags=["exports"])
api_router.include_router(websocket.router)
