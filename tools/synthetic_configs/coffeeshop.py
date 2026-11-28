from pathlib import Path

from synthetic_configs.base import SyntheticConfig

LOCATIONS = 1000
PRODUCTS = 26
ORDERS_PER_SCALE = 8_500_000
LINES_PER_PERIOD = 156
ORDERS_PER_PERIOD = 100
TIME_EXTRACT = "FROM ol.order_time)"
TIME_EXTRACT_CAST = "FROM CAST(ol.order_time AS TIME))"
ORDER_LINES = [1] * 60 + [2] * 30 + [3] * 5 + [4] * 4 + [5]


def expected_rows(scale_factor: float) -> dict[str, int]:
    orders = max(1, int(round(ORDERS_PER_SCALE * scale_factor)))
    periods, rest = divmod(orders, ORDERS_PER_PERIOD)
    return {
        "dim_locations": LOCATIONS,
        "dim_products": PRODUCTS,
        "order_lines": periods * LINES_PER_PERIOD + sum(ORDER_LINES[:rest]),
    }


def legacy_generate(scale_factor: float, seed: int, out: Path) -> None:
    from benchbox.core.coffeeshop.generator import CoffeeShopDataGenerator

    CoffeeShopDataGenerator(scale_factor=scale_factor, output_dir=out).generate_data()


def queries() -> dict[str, str]:
    from benchbox.core.coffeeshop.queries import CoffeeShopQueryManager

    return {
        query_id: sql.replace(TIME_EXTRACT, TIME_EXTRACT_CAST)
        for query_id, sql in CoffeeShopQueryManager().get_all_queries().items()
    }


def benchbox_schema() -> dict:
    from benchbox.core.coffeeshop.schema import TABLES

    return TABLES


CONFIG = SyntheticConfig(
    name="coffeeshop",
    tables=["dim_locations", "dim_products", "order_lines"],
    primary_keys={
        "dim_locations": ["record_id"],
        "dim_products": ["record_id"],
        "order_lines": ["order_id", "line_number"],
    },
    foreign_keys=[
        ("order_lines", "location_record_id", "dim_locations", "record_id"),
        ("order_lines", "product_record_id", "dim_products", "record_id"),
    ],
    expected_rows=expected_rows,
    queries=queries,
    legacy_generate=legacy_generate,
    benchbox_schema=benchbox_schema,
    legacy_suffix="csv",
    legacy_delimiter=",",
    legacy_header=False,
    verify_scales=(0.01, 0.1, 1.0),
    budgets={
        (1.0, "tbl"): (15.0, 512),
        (1.0, "parquet"): (15.0, 512),
        (10.0, "tbl"): (150.0, 512),
        (10.0, "parquet"): (150.0, 512),
    },
)
