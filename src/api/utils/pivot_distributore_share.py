def pivot_distributor_share(rows) -> dict:
    # 1. Собираем всех дистрибьюторов
    distributors: dict[int, str] = {
        row["distributor_id"]: row["distributor_name"] for row in rows
    }

    # 2. Собираем все периоды
    periods_set: set[str] = set()
    for row in rows:
        periods_set.update(row["periods_data"].keys())

    # 3. Pivot: period -> {dist_id: {amount, share_percent}}
    period_map: dict[str, dict] = {}

    for row in rows:
        dist_key = f"dist_{row['distributor_id']}"

        for period, values in row["periods_data"].items():
            if period not in period_map:
                period_map[period] = {"total_amount": 0, "distributors": {}}

            period_map[period]["distributors"][dist_key] = values
            period_map[period]["total_amount"] += values["amount"]

    # 4. Формируем финальный список
    result = []

    for period_str in sorted(period_map.keys()):
        dist_data = period_map[period_str]["distributors"]
        total_amount = period_map[period_str]["total_amount"]

        original = {}
        amounts = {}
        top_key = None

        for dist_id in distributors:
            dist_key = f"dist_{dist_id}"
            entry = dist_data.get(dist_key)

            share = float(entry["share_percent"]) if entry else 0.0
            amount = float(entry["amount"]) if entry else 0.0

            original[dist_key] = share
            amounts[dist_key] = round(amount, 2)

            # последний ненулевой дистрибьютор
            if share > 0:
                top_key = dist_key

        result.append(
            {
                "period": period_str,
                "totalAmount": round(total_amount, 2),
                **amounts,
                "_original": original,
                "_topKey": top_key,
            }
        )

    return {
        "data": result,
        "distributors": {
            f"dist_{dist_id}": name for dist_id, name in distributors.items()
        },
    }
