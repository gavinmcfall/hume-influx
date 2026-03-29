"""InfluxDB v2 writer for Hume body composition data."""

import logging
from datetime import datetime, timezone

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

from .config import InfluxConfig

logger = logging.getLogger(__name__)

# Fields to write to InfluxDB — curated from the 97 available
BODY_FIELDS = {
    # Core
    "weight": "weight",
    "bmi": "bmi",
    "fatRate": "body_fat_pct",
    "fatMass": "fat_mass",
    "muscleMass": "muscle_mass",
    "muscleRate": "muscle_pct",
    "boneMass": "bone_mass",
    "moisture": "body_water_pct",
    "basalMetabolicRate": "bmr",
    "physicalAge": "metabolic_age",
    "proteinRate": "protein_pct",
    "proteinMass": "protein_mass",
    "viscelarFat": "visceral_fat",
    "subcutaneousFat": "subcutaneous_fat_pct",
    "bodyFatSubCutKg": "subcutaneous_fat_kg",
    "fatFreeMass": "fat_free_mass",
    "bodySkeletal": "skeletal_muscle_pct",
    "heartRate": "heart_rate",
    "standardWeight": "standard_weight",
    "controlWeight": "control_weight",
    "obesityDegreeIndex": "obesity_degree",
    "agRatio": "ag_ratio",
    # Segmental muscle
    "leftArmMuscleWeightIndex": "muscle_left_arm",
    "rightArmMuscleWeightIndex": "muscle_right_arm",
    "trunkMuscleWeightIndex": "muscle_trunk",
    "leftLegMuscleIndex": "muscle_left_leg",
    "rightLegMuscleIndex": "muscle_right_leg",
    # Segmental fat
    "leftArmFatIndex": "fat_left_arm",
    "rightArmFatIndex": "fat_right_arm",
    "trunkFatIndex": "fat_trunk",
    "leftLegFatIndex": "fat_left_leg",
    "rightLegFatIndex": "fat_right_leg",
    # Water
    "waterECWKg": "water_ecw_kg",
    "waterICWKg": "water_icw_kg",
    "cellMassKg": "cell_mass_kg",
    "mineralKg": "mineral_kg",
}


class InfluxWriter:
    def __init__(self, config: InfluxConfig):
        self._config = config
        self._client = InfluxDBClient(url=config.url, token=config.token, org=config.org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._client.query_api()

    def close(self):
        self._client.close()

    def get_last_timestamp(self, user: str) -> int:
        """Get the most recent body_composition timestamp for a user (epoch ms)."""
        query = f'''
            from(bucket: "{self._config.bucket}")
                |> range(start: -365d)
                |> filter(fn: (r) => r._measurement == "body_composition")
                |> filter(fn: (r) => r.user == "{user}")
                |> filter(fn: (r) => r._field == "weight")
                |> keep(columns: ["_time"])
                |> sort(columns: ["_time"], desc: true)
                |> limit(n: 1)
        '''
        try:
            tables = self._query_api.query(query, org=self._config.org)
            for table in tables:
                for record in table.records:
                    return int(record.get_time().timestamp() * 1000)
        except Exception:
            logger.debug("No existing body composition data for %s", user)
        return 0

    def write_measurements(self, measurements: list[dict], user: str) -> int:
        """Write body composition measurements. Returns count written."""
        points = []

        for m in measurements:
            device_time = m.get("deviceTime", 0)
            if not device_time:
                continue

            ts = datetime.fromtimestamp(device_time / 1000, tz=timezone.utc)

            point = Point("body_composition").tag("user", user)

            field_count = 0
            for src_key, dst_key in BODY_FIELDS.items():
                val = m.get(src_key)
                if val is not None and isinstance(val, (int, float)):
                    point = point.field(dst_key, float(val))
                    field_count += 1

            if field_count > 0:
                point = point.time(ts, WritePrecision.MS)
                points.append(point)

        if points:
            self._write_api.write(bucket=self._config.bucket, record=points)
            logger.info("Wrote %d body composition points for %s", len(points), user)

        return len(points)
