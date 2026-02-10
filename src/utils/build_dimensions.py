def build_dimensions(mapping: dict, dims: list[str] | None):
    select_fields = []
    group_by_fields = []
    search_columns = []

    if not dims:
        return select_fields, group_by_fields, search_columns

    for dim in dims:
        cfg = mapping.get(dim)
        if not cfg:
            continue

        select_fields.extend([cfg["id"], cfg["name"]])
        group_by_fields.extend(cfg["group_fields"])

        if "search" in cfg:
            search_columns.append(cfg["search"])

    return select_fields, group_by_fields, search_columns
