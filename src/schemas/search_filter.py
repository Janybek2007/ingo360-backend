from typing import Literal

from src.schemas.base_filter import BaseFilter


class SearchFilter(BaseFilter):
    search: str | None = None
    sort_by: (
        Literal[
            "active_users_limit",
            "contract_number",
            "ims_name",
            "name",
            "status",
        ]
        | None
    ) = None
