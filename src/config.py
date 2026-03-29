"""TOML config loader with environment variable overrides."""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path

import tomllib

logger = logging.getLogger(__name__)

CONFIG_PATH = os.environ.get("CONFIG_PATH", "/app/config.toml")


@dataclass
class UserConfig:
    name: str = ""
    email: str = ""
    password: str = ""


@dataclass
class InfluxConfig:
    url: str = "http://influxdb.vitals.svc.cluster.local:8086"
    bucket: str = "health"
    org: str = "vitals"
    token: str = ""


@dataclass
class MainConfig:
    log_level: str = "INFO"
    loop_minutes: int = 60


@dataclass
class AppConfig:
    users: list[UserConfig] = field(default_factory=list)
    influx: InfluxConfig = field(default_factory=InfluxConfig)
    main: MainConfig = field(default_factory=MainConfig)


def load_config(path: str | None = None) -> AppConfig:
    config_path = Path(path or CONFIG_PATH)

    if config_path.exists():
        with open(config_path, "rb") as f:
            raw = tomllib.load(f)
        logger.info("Loaded config from %s", config_path)
    else:
        logger.warning("Config file not found at %s, using defaults + env", config_path)
        raw = {}

    influx_raw = raw.get("influx", {})
    main_raw = raw.get("main", {})

    users = []
    for u in raw.get("users", []):
        users.append(UserConfig(
            name=u.get("name", u.get("email", "").split("@")[0]),
            email=u.get("email", ""),
            password=u.get("password", ""),
        ))

    # Backwards compat: single [hume] section
    if not users and "hume" in raw:
        h = raw["hume"]
        users.append(UserConfig(
            name=h.get("name", h.get("email", "").split("@")[0]),
            email=os.environ.get("HUME_EMAIL", h.get("email", "")),
            password=os.environ.get("HUME_PASSWORD", h.get("password", "")),
        ))

    return AppConfig(
        users=users,
        influx=InfluxConfig(
            url=os.environ.get("INFLUX_URL", influx_raw.get("url", "http://influxdb.vitals.svc.cluster.local:8086")),
            bucket=os.environ.get("INFLUX_BUCKET", influx_raw.get("bucket", "health")),
            org=os.environ.get("INFLUX_ORG", influx_raw.get("org", "vitals")),
            token=os.environ.get("INFLUX_TOKEN", influx_raw.get("token", "")),
        ),
        main=MainConfig(
            log_level=os.environ.get("LOG_LEVEL", main_raw.get("log_level", "INFO")),
            loop_minutes=int(os.environ.get("LOOP_MINUTES", main_raw.get("loop_minutes", 60))),
        ),
    )
