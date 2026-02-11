from src.db.models.sales import (
    PrimarySalesAndStock,
    SecondarySales,
    TertiarySalesAndStock,
)
from src.services.sale.primary import PrimarySalesAndStockService
from src.services.sale.secondary import SecondarySalesService
from src.services.sale.tertiary import TertiarySalesService

primary_sales_service = PrimarySalesAndStockService(PrimarySalesAndStock)
secondary_sales_service = SecondarySalesService(SecondarySales)
tertiary_sales_service = TertiarySalesService(TertiarySalesAndStock)
