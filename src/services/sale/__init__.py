from src.db.models.sales import (
    PrimarySalesAndStock,
    SecondarySales,
    TertiarySalesAndStock,
)

from .primary import PrimarySalesAndStockService
from .secondary import SecondarySalesService
from .tertiary import TertiarySalesService

primary_sales_service = PrimarySalesAndStockService(PrimarySalesAndStock)
secondary_sales_service = SecondarySalesService(SecondarySales)
tertiary_sales_service = TertiarySalesService(TertiarySalesAndStock)
