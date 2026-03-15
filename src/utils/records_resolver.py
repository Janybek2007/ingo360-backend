from dataclasses import dataclass
from typing import Callable, Type

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class FieldResolverConfig:
    record_key: str
    model: Type | None = None
    lookup_field: str = ""
    required: bool = False
    db_field: str = ""
    error_label: str = ""

    @property
    def aliases(self) -> list[str]:
        return [k.strip() for k in self.record_key.split("|")]

    @property
    def primary_key(self) -> str:
        return self.aliases[0]

    def as_required(self) -> "FieldResolverConfig":
        from dataclasses import replace

        return replace(self, required=True)


@dataclass
class ResolvedFields:
    maps: dict[str, dict]
    missing: dict[str, set]

    def get_id(self, record_key: str, value) -> int | None:
        if value is None:
            return None
        return self.maps.get(record_key, {}).get(str(value))

    def collect_missing_keys(
        self, record: dict, configs: list[FieldResolverConfig]
    ) -> list[str]:
        missing_keys = []
        for cfg in configs:
            if cfg.model is None:
                continue
            val = record.get(cfg.primary_key)
            if val is not None and str(val) in self.missing.get(cfg.primary_key, set()):
                label = cfg.error_label or cfg.primary_key
                missing_keys.append(f"{label}: {val}")
        return missing_keys

    def resolve_id_fields(
        self, record: dict, configs: list[FieldResolverConfig]
    ) -> tuple[dict[str, int | None], list[str]]:
        ids: dict[str, int | None] = {}
        for cfg in configs:
            if cfg.model is None or not cfg.db_field:
                continue
            ids[cfg.db_field] = self.get_id(
                cfg.primary_key, record.get(cfg.primary_key)
            )
        null_keys = [
            cfg.error_label or cfg.primary_key
            for cfg in configs
            if cfg.required and cfg.db_field and ids.get(cfg.db_field) is None
        ]
        # Проверка required полей без model (name, fio, ims_name и т.д.)
        plain_null_keys = [
            cfg.error_label or cfg.primary_key
            for cfg in configs
            if cfg.required and cfg.model is None and not record.get(cfg.primary_key)
        ]
        null_keys.extend(plain_null_keys)
        return ids, null_keys


def normalize_record(record: dict, configs: list[FieldResolverConfig]) -> None:
    """Нормализация одной записи - используется при батчинге"""
    for cfg in configs:
        for alias in cfg.aliases:
            if alias in record and cfg.primary_key not in record:
                val = record[alias]
                record[cfg.primary_key] = str(val) if val is not None else val
                break


async def build_resolved_fields(
    session: AsyncSession,
    unique_values: dict[str, set],
    configs: list[FieldResolverConfig],
    get_id_map: Callable,
) -> ResolvedFields:
    """Строит ResolvedFields из заранее собранных уникальных значений"""
    maps: dict[str, dict] = {}
    missing: dict[str, set] = {}

    for cfg in configs:
        if cfg.model is None:
            maps[cfg.primary_key] = {}
            missing[cfg.primary_key] = set()
            continue
        values = unique_values.get(cfg.primary_key, set())
        if values:
            id_map, missing_set = await get_id_map(
                session, cfg.model, cfg.lookup_field, values
            )
        else:
            id_map, missing_set = {}, set()
        maps[cfg.primary_key] = id_map
        missing[cfg.primary_key] = missing_set

    return ResolvedFields(maps=maps, missing=missing)


async def resolve_records_fields(
    session: AsyncSession,
    records: list[dict],
    configs: list[FieldResolverConfig],
    get_id_map: Callable,
) -> ResolvedFields:
    """Стандартный режим - весь список сразу"""
    unique_values: dict[str, set] = {}

    for cfg in configs:
        for r in records:
            normalize_record(r, [cfg])
        if cfg.model is None:
            continue
        unique_values[cfg.primary_key] = {
            str(r.get(cfg.primary_key))
            for r in records
            if r.get(cfg.primary_key) is not None
        }

    return await build_resolved_fields(session, unique_values, configs, get_id_map)
