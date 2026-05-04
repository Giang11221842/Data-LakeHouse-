import ast
import configparser
from pathlib import Path
from typing import List, Optional

from etls.bronze_ingestion import ingest_taxi_to_layer

CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "config.conf"


def _load_service_types() -> List[str]:
    config = configparser.ConfigParser()
    config.read(CONFIG_PATH)

    raw_value = config.get("taxi_type", "service_type", fallback="['yellow']")

    try:
        parsed = ast.literal_eval(raw_value)
        if isinstance(parsed, list):
            return [str(item).strip().lower() for item in parsed if str(item).strip()]
    except (SyntaxError, ValueError):
        pass

    return [value.strip().lower() for value in raw_value.split(",") if value.strip()]


def taxi_pipeline(year: int, month: int, layer: str = "bronze", service_type: Optional[str] = None):
    service_types = [service_type.lower()] if service_type else _load_service_types()
    results = []

    for service in service_types:
        results.append(
            ingest_taxi_to_layer(
                service_type=service,
                year=year,
                month=month,
                layer=layer,
                pipeline_name=f"nyc_taxi_{layer}_pipeline",
            )
        )

    return results
