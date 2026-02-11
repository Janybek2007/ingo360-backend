from src.db.models import products
from src.services.product.brand import BrandService
from src.services.product.dosage import DosageService
from src.services.product.dosage_form import DosageFormService
from src.services.product.product_group import ProductGroupService
from src.services.product.promotion_type import PromotionTypeService
from src.services.product.segment import SegmentService
from src.services.product.sku import SKUService

brand_service = BrandService(products.Brand)
promotion_type_service = PromotionTypeService(products.PromotionType)
dosage_form_service = DosageFormService(products.DosageForm)
dosage_service = DosageService(products.Dosage)
segment_service = SegmentService(products.Segment)
sku_service = SKUService(products.SKU)
product_group_service = ProductGroupService(products.ProductGroup)
