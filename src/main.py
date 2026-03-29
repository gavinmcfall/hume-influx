"""Entry point: single run — fetch and write body composition data, then exit."""

import logging
import sys

from .config import UserConfig, load_config
from .hume import HumeClient
from .influx import InfluxWriter

logger = logging.getLogger("hume-influx")


def main():
    config = load_config()

    logging.basicConfig(
        level=getattr(logging, config.main.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("Starting hume-influx (single run)")
    logger.info("Users: %s", ", ".join(u.name for u in config.users))

    writer = InfluxWriter(config.influx)

    try:
        for user_config in config.users:
            _sync_user(user_config, writer)
    finally:
        writer.close()

    logger.info("Done")


def _sync_user(user_config: UserConfig, writer: InfluxWriter):
    user = user_config.name
    logger.info("[%s] Syncing...", user)

    client = HumeClient(user_config.email, user_config.password)
    last_ts = writer.get_last_timestamp(user)

    measurements = client.fetch_measurements()
    if not measurements:
        logger.info("[%s] No measurements found", user)
        return

    new = [m for m in measurements if m.get("deviceTime", 0) > last_ts]
    if not new:
        logger.info("[%s] All %d measurements already recorded", user, len(measurements))
        return

    logger.info("[%s] Found %d new measurements (of %d total)", user, len(new), len(measurements))
    written = writer.write_measurements(new, user)
    logger.info("[%s] Wrote %d points", user, written)


if __name__ == "__main__":
    main()
