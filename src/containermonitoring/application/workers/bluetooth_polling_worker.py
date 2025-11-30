import logging
import threading
import time

from config.bluetooth_config import BluetoothConfig
from src.containermonitoring.application.services import BluetoothPollingService
from src.shared.infrastructure.bluetooth import (
    DeviceConfigLoader
)

logger = logging.getLogger(__name__)


class BluetoothPollingWorker:
    """
    Background worker for polling Bluetooth devices

    Responsibilities:
    - Run every N seconds (configured in .env)
    - Load device config from JSON (hot reload)
    - Poll each device for sensor readings
    - Handle device online/offline status
    - Publish readings and status to MQTT

    Hot Reload:
    - Reloads bluetooth_devices.json every N cycles
    - Detects new devices and starts polling them
    - Removes deleted devices from polling
    """

    def __init__(
            self,
            bluetooth_polling_service: BluetoothPollingService,
            device_config_loader: DeviceConfigLoader
    ):
        """
        Initialize worker

        Args:
            bluetooth_polling_service: Service for polling logic
            device_config_loader: Loader for device configuration
        """
        self.bluetooth_polling_service = bluetooth_polling_service
        self.device_config_loader = device_config_loader

        self.running = False
        self.thread = None
        self.cycle_count = 0

        logger.info(
            f"Bluetooth Polling Worker initialized "
            f"(interval: {BluetoothConfig.POLLING_INTERVAL}s)"
        )

    def start(self):
        """Start the polling worker"""
        if self.running:
            logger.warning("Bluetooth Polling Worker is already running")
            return

        logger.info("Starting Bluetooth Polling Worker...")
        self.running = True

        self.thread = threading.Thread(
            target=self._run_loop,
            daemon=True,
            name="BluetoothPollingWorker"
        )
        self.thread.start()

        logger.info("✅ Bluetooth Polling Worker started")

    def stop(self):
        """Stop the polling worker"""
        if not self.running:
            logger.warning("Bluetooth Polling Worker is not running")
            return

        logger.info("Stopping Bluetooth Polling Worker...")
        self.running = False

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)

        logger.info("✅ Bluetooth Polling Worker stopped")

    def _run_loop(self):
        """Main polling loop"""
        logger.info("Bluetooth Polling Worker loop started")

        # Initial load
        devices = self.device_config_loader.load_devices(force_reload=True)

        while self.running:
            try:
                self.cycle_count += 1

                logger.info(
                    f"=== Bluetooth Polling Cycle #{self.cycle_count} ==="
                )

                # Hot reload config every N cycles
                if self.cycle_count % BluetoothConfig.CONFIG_RELOAD_CYCLES == 0:
                    logger.info("Hot reloading device configuration...")
                    devices = self.device_config_loader.load_devices(
                        force_reload=True
                    )

                # Poll each device
                if not devices:
                    logger.warning("No devices configured for polling")
                else:
                    logger.info(f"Polling {len(devices)} device(s)...")

                    for device in devices:
                        if not self.running:
                            break

                        try:
                            self.bluetooth_polling_service.poll_device(device)
                        except Exception as e:
                            logger.error(
                                f"Error polling device {device.device_identifier}: {e}",
                                exc_info=True
                            )

                logger.info(
                    f"=== Polling Cycle #{self.cycle_count} Complete ===\n"
                )

                # Sleep until next cycle
                self._sleep_until_next_cycle()

            except Exception as e:
                logger.error(
                    f"Error in Bluetooth Polling Worker loop: {e}",
                    exc_info=True
                )
                # Sleep before retry
                time.sleep(5)

        logger.info("Bluetooth Polling Worker loop ended")

    def _sleep_until_next_cycle(self):
        """Sleep until next polling cycle, allowing quick shutdown"""
        elapsed = 0
        interval = BluetoothConfig.POLLING_INTERVAL

        while elapsed < interval and self.running:
            time.sleep(min(1, interval - elapsed))
            elapsed += 1

    def is_running(self) -> bool:
        """Check if worker is running"""
        return self.running