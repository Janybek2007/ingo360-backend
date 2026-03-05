from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from src.api.dependencies.current_user import current_active_user, current_operator_user
from src.db.models import SKU, Brand, ProductGroup, User
from src.db.session import db_session
from src.schemas import product
from src.schemas.base_filter import PaginatedResponse
from src.schemas.export import ExportExcelRequest
from src.services import product as product_serv

router = APIRouter()


@router.post(
    "/product-groups/create",
    response_model=product.ProductGroupResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_product_group(
    product_group: product.ProductGroupCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [joinedload(ProductGroup.company)]
    return await product_serv.product_group_service.create(
        session, product_group, load_options=load_options
    )


@router.post("/product-groups/import-excel")
async def bulk_insert_product_groups(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.product_group_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.post(
    "/product-groups",
    response_model=PaginatedResponse[product.ProductGroupResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_product_groups(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.ProductGroupListRequest,
):
    load_options = [joinedload(ProductGroup.company)]
    return await product_serv.product_group_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/product-groups/export-excel")
async def export_regions_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.product_group.ProductGroupService",
        model_path="src.db.models.products.ProductGroup",
        serializer_path="src.schemas.product.ProductGroupResponse",
        load_options_paths=["company"],
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.get(
    "/product-groups/{product_group_id}",
    response_model=product.ProductGroupResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_product_group(
    product_group_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [joinedload(ProductGroup.company)]
    return await product_serv.product_group_service.get_or_404(
        session, product_group_id, load_options=load_options
    )


@router.patch(
    "/product-groups/{product_group_id}", response_model=product.ProductGroupResponse
)
async def update_product_group(
    product_group_id: int,
    product_group: product.ProductGroupUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [joinedload(ProductGroup.company)]
    return await product_serv.product_group_service.update(
        session, product_group_id, product_group, load_options=load_options
    )


@router.delete(
    "/product-groups/{product_group_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_product_group(
    product_group_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.product_group_service.delete(session, product_group_id)


@router.post(
    "/brands/create",
    response_model=product.BrandResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_brand(
    brand: product.BrandCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Brand.promotion_type),
        joinedload(Brand.product_group),
        joinedload(Brand.company),
    ]
    return await product_serv.brand_service.create(
        session, brand, load_options=load_options
    )


@router.post(
    "/brands",
    response_model=PaginatedResponse[product.BrandResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_brands(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.BrandListRequest,
):
    load_options = [
        joinedload(Brand.promotion_type),
        joinedload(Brand.product_group),
        joinedload(Brand.company),
    ]
    return await product_serv.brand_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/brands/export-excel")
async def export_brands_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.brand.BrandService",
        model_path="src.db.models.products.Brand",
        serializer_path="src.schemas.product.BrandResponse",
        load_options_paths=["promotion_type", "product_group", "company"],
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.post("/brands/import-excel")
async def bulk_insert_brands(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.brand_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/brands/{brand_id}",
    response_model=product.BrandResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_brand(
    brand_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(Brand.promotion_type),
        joinedload(Brand.product_group),
        joinedload(Brand.company),
    ]
    return await product_serv.brand_service.get_or_404(
        session, brand_id, load_options=load_options
    )


@router.patch(
    "/brands/{brand_id}",
    response_model=product.BrandResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_brand(
    brand_id: int,
    brand: product.BrandUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(Brand.promotion_type),
        joinedload(Brand.product_group),
        joinedload(Brand.company),
    ]
    return await product_serv.brand_service.update(
        session, brand_id, brand, load_options=load_options
    )


@router.delete("/brands/{brand_id}", dependencies=[Depends(current_operator_user)])
async def delete_brand(
    brand_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await product_serv.brand_service.delete(session, brand_id)


@router.post(
    "/promotion-types/create",
    response_model=product.PromotionTypeResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_promotion_type(
    promotion_type: product.PromotionTypeCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.promotion_type_service.create(session, promotion_type)


@router.post("/promotion-types/import-excel")
async def bulk_insert_promotion_types(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.promotion_type_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.post(
    "/promotion-types",
    response_model=PaginatedResponse[product.PromotionTypeResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_promotion_types(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.PromotionTypeListRequest,
):
    return await product_serv.promotion_type_service.get_multi(session, filters=filters)


@router.post("/promotion-types/export-excel")
async def export_promotion_types_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.promotion_type.PromotionTypeService",
        model_path="src.db.models.products.PromotionType",
        serializer_path="src.schemas.product.PromotionTypeResponse",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.get(
    "/promotion-types/{promotion_type_id}",
    response_model=product.PromotionTypeResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_promotion_type(
    promotion_type_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.promotion_type_service.get_or_404(
        session, promotion_type_id
    )


@router.patch(
    "/promotion-types/{promotion_type_id}",
    response_model=product.PromotionTypeResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_promotion_type(
    promotion_type_id: int,
    promotion_type: product.PromotionTypeUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.promotion_type_service.update(
        session, promotion_type_id, promotion_type
    )


@router.delete(
    "/promotion-types/{promotion_type_id}",
    dependencies=[Depends(current_operator_user)],
)
async def delete_promotion_type(
    promotion_type_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.promotion_type_service.delete(session, promotion_type_id)


@router.post(
    "/dosage-forms/create",
    response_model=product.DosageFormResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_dosage_form(
    dosage_form: product.DosageFormCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.dosage_form_service.create(session, dosage_form)


@router.post(
    "/dosage-forms",
    response_model=PaginatedResponse[product.DosageFormResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_dosage_forms(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.DosageFormListRequest,
):
    return await product_serv.dosage_form_service.get_multi(session, filters=filters)


@router.post("/dosage-forms/export-excel")
async def export_dosage_forms_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.dosage_form.DosageFormService",
        model_path="src.db.models.products.DosageForm",
        serializer_path="src.schemas.product.DosageFormResponse",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.get(
    "/dosage-forms/{dosage_form_id}",
    response_model=product.DosageFormResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_dosage_form(
    dosage_form_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.dosage_form_service.get_or_404(session, dosage_form_id)


@router.post("/dosage-forms/import-excel")
async def bulk_insert_dosage_forms(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_active_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.dosage_form_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.patch(
    "/dosage-forms/{dosage_form_id}",
    response_model=product.DosageFormResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_dosage_form(
    dosage_form_id: int,
    dosage_form: product.DosageFormUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.dosage_form_service.update(
        session, dosage_form_id, dosage_form
    )


@router.delete(
    "/dosage-forms/{dosage_form_id}", dependencies=[Depends(current_operator_user)]
)
async def delete_dosage_form(
    dosage_form_id: int,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.dosage_form_service.delete(session, dosage_form_id)


@router.post(
    "/dosages/create",
    response_model=product.DosageResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_dosage(
    dosage: product.DosageCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.dosage_service.create(session, dosage)


@router.post(
    "/dosages",
    response_model=PaginatedResponse[product.DosageResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_dosages(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.DosageListRequest,
):
    return await product_serv.dosage_service.get_multi(session, filters=filters)


@router.post("/dosages/export-excel")
async def export_dosages_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.dosage.DosageService",
        model_path="src.db.models.products.Dosage",
        serializer_path="src.schemas.product.DosageResponse",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.post("/dosages/import-excel")
async def bulk_insert_dosages(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.dosage_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/dosages/{dosage_id}",
    response_model=product.DosageResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_dosage(
    dosage_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await product_serv.dosage_service.get_or_404(session, dosage_id)


@router.patch(
    "/dosages/{dosage_id}",
    response_model=product.DosageResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_dosage(
    dosage_id: int,
    dosage: product.DosageUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.dosage_service.update(session, dosage_id, dosage)


@router.delete("/dosages/{dosage_id}", dependencies=[Depends(current_operator_user)])
async def delete_dosage(
    dosage_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await product_serv.dosage_service.delete(session, dosage_id)


@router.post(
    "/segments/create",
    response_model=product.SegmentResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_segment(
    segment: product.SegmentCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.segment_service.create(session, segment)


@router.post(
    "/segments",
    response_model=PaginatedResponse[product.SegmentResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_segments(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.SegmentListRequest,
):
    return await product_serv.segment_service.get_multi(session, filters=filters)


@router.post("/segments/export-excel")
async def export_segments_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.segment.SegmentService",
        model_path="src.db.models.products.Segment",
        serializer_path="src.schemas.product.SegmentResponse",
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.post("/segments/import-excel")
async def bulk_insert_segments(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.segment_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/segments/{segment_id}",
    response_model=product.SegmentResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_segment(
    segment_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await product_serv.segment_service.get_or_404(session, segment_id)


@router.patch(
    "/segments/{segment_id}",
    response_model=product.SegmentResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_segment(
    segment_id: int,
    segment: product.SegmentUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    return await product_serv.segment_service.update(session, segment_id, segment)


@router.delete("/segments/{segment_id}", dependencies=[Depends(current_operator_user)])
async def delete_segment(
    segment_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await product_serv.segment_service.delete(session, segment_id)


@router.post(
    "/skus/create",
    response_model=product.SKUResponse,
    dependencies=[Depends(current_operator_user)],
)
async def create_sku(
    sku: product.SKUCreate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SKU.brand),
        joinedload(SKU.promotion_type),
        joinedload(SKU.product_group),
        joinedload(SKU.dosage_form),
        joinedload(SKU.dosage),
        joinedload(SKU.segment),
        joinedload(SKU.company),
    ]
    return await product_serv.sku_service.create(
        session, sku, load_options=load_options
    )


@router.post(
    "/skus",
    response_model=PaginatedResponse[product.SKUResponse],
    dependencies=[Depends(current_operator_user)],
)
async def get_skus(
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    filters: product.SKUListRequest,
):
    load_options = [
        joinedload(SKU.brand),
        joinedload(SKU.promotion_type),
        joinedload(SKU.product_group),
        joinedload(SKU.dosage_form),
        joinedload(SKU.dosage),
        joinedload(SKU.segment),
        joinedload(SKU.company),
    ]
    return await product_serv.sku_service.get_multi(
        session, load_options=load_options, filters=filters
    )


@router.post("/skus/export-excel")
async def export_skus_excel(
    payload: ExportExcelRequest,
    current_user: Annotated[User, Depends(current_operator_user)],
):
    from src.tasks.export_excel import create_export_task_record, export_excel_task

    task = export_excel_task.delay(
        user_id=current_user.id,
        file_name=payload.file_name,
        service_path="src.services.product.sku.SKUService",
        model_path="src.db.models.products.SKU",
        serializer_path="src.schemas.product.SKUResponse",
        load_options_paths=[
            "brand",
            "promotion_type",
            "product_group",
            "dosage_form",
            "dosage",
            "segment",
            "company",
        ],
        header_map=payload.header_map,
        fields_map=payload.fields_map,
        boolean_map=payload.boolean_map,
        custom_map=payload.custom_map,
    )

    await create_export_task_record(
        task_id=task.id,
        started_by=current_user.id,
        file_path="",
    )

    return {"task_id": task.id}


@router.post("/skus/import-excel")
async def bulk_insert_skus(
    file: UploadFile,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
    current_user: Annotated[User, Depends(current_operator_user)],
):
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only Excel files are allowed",
        )
    result = await product_serv.sku_service.import_excel(
        session, file, user_id=current_user.id
    )
    return result


@router.get(
    "/skus/{sku_id}",
    response_model=product.SKUResponse,
    dependencies=[Depends(current_operator_user)],
)
async def get_sku(
    sku_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    load_options = [
        joinedload(SKU.brand),
        joinedload(SKU.promotion_type),
        joinedload(SKU.product_group),
        joinedload(SKU.dosage_form),
        joinedload(SKU.dosage),
        joinedload(SKU.segment),
        joinedload(SKU.company),
    ]
    return await product_serv.sku_service.get_or_404(
        session, sku_id, load_options=load_options
    )


@router.patch(
    "/skus/{sku_id}",
    response_model=product.SKUResponse,
    dependencies=[Depends(current_operator_user)],
)
async def update_sku(
    sku_id: int,
    sku: product.SKUUpdate,
    session: Annotated[AsyncSession, Depends(db_session.get_session)],
):
    load_options = [
        joinedload(SKU.brand),
        joinedload(SKU.promotion_type),
        joinedload(SKU.product_group),
        joinedload(SKU.dosage_form),
        joinedload(SKU.dosage),
        joinedload(SKU.segment),
        joinedload(SKU.company),
    ]
    return await product_serv.sku_service.update(
        session, sku_id, sku, load_options=load_options
    )


@router.delete("/skus/{sku_id}", dependencies=[Depends(current_operator_user)])
async def delete_sku(
    sku_id: int, session: Annotated[AsyncSession, Depends(db_session.get_session)]
):
    return await product_serv.sku_service.delete(session, sku_id)
