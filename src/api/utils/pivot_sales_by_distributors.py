def pivot_sales_by_distributors(rows) -> dict:
    distributors: dict[int, str] = {
        row["distributor_id"]: row["distributor_name"] for row in rows
    }

    period_map: dict[str, dict] = {}
    for row in rows:
        dist_name = row["distributor_name"]
        for period, values in row["periods_data"].items():
            if period not in period_map:
                period_map[period] = {}
            period_map[period][dist_name] = {
                "total_amount": float(values["total_amount"] or 0),
                "total_packages": int(values["total_packages"] or 0),
            }

    data = [
        {"period": period, **amounts} for period, amounts in sorted(period_map.items())
    ]

    return {
        "data": data,
        "distributors": {str(dist_id): name for dist_id, name in distributors.items()},
    }
