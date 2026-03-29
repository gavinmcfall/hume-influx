"""Entry point: config load → poll loop for Hume body composition data."""

import logging
import signal
import time

from .config import AppConfig, UserConfig, load_config
from .hume import HumeClient
from .influx import InfluxWriter

logger = logging.getLogger("hume-influx")

_shutdown = False


def _handle_signal(signum, frame):
    global _shutdown
    logger.info("Received signal %d, shutting down", signum)
    _shutdown = True


def main():
    global _shutdown

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    config = load_config()

    logging.basicConfig(
        level=getattr(logging, config.main.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("Starting hume-influx")
    logger.info("Users: %s", ", ".join(u.name for u in config.users))
    logger.info("Loop interval: %d minutes", config.main.loop_minutes)

    writer = InfluxWriter(config.influx)

    try:
        while not _shutdown:
            for user_config in config.users:
                if _shutdown:
                    break
                try:
                    _poll_user(user_config, writer)
                except Exception:
                    logger.exception("Error polling user %s", user_config.name)

            if _shutdown:
                break

            logger.info("Sleeping %d minutes until next poll", config.main.loop_minutes)
            sleep_until = time.time() + config.main.loop_minutes * 60
            while time.time() < sleep_until and not _shutdown:
                time.sleep(1)
    finally:
        writer.close()
        logger.info("Shutdown complete")


def _poll_user(user_config: UserConfig, writer: InfluxWriter):
    user = user_config.name
    logger.info("[%s] Polling...", user)

    client = HumeClient(user_config.email, user_config.password)
    last_ts = writer.get_last_timestamp(user)

    measurements = client.fetch_measurements()
    if not measurements:
        logger.info("[%s] No measurements found", user)
        return

    # Filter to new measurements only
    new = [m for m in measurements if m.get("deviceTime", 0) > last_ts]
    if not new:
        logger.info("[%s] All %d measurements already recorded", user, len(measurements))
        return

    logger.info("[%s] Found %d new measurements (of %d total)", user, len(new), len(measurements))
    written = writer.write_measurements(new, user)
    logger.info("[%s] Wrote %d points", user, written)


if __name__ == "__main__":
    main()
