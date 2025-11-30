import logging
from src.shared.infrastructure.workers import BackgroundWorker
from src.containermonitoring.application.services import SensorReadingService
from src.containermonitoring.infrastructure.messaging import MqttPublisher

logger = logging.getLogger(__name__)


class SensorReadingSyncWorker(BackgroundWorker):
    """
    Background worker for syncing sensor readings to Backend

    Responsibilities:
    - Run every N seconds (default: 3600s = 1 hour)
    - Find all readings with synced_to_backend=False
    - Separate alerts from normal readings
    - Publish alerts individually (immediate priority)
    - Publish normal readings as batch
    - Mark successfully published readings as synced
    - Handle publishes failures gracefully

    Business Logic:
    - Alerts are always published immediately when created (via REST API)
    - This worker is a safety net for any missed alerts
    - Normal readings are batched for efficiency
    - If MQTT is down, readings accumulate and sync when the connection returns
    """

    def __init__(
            self,
            sensor_reading_service: SensorReadingService,
            mqtt_publisher: MqttPublisher,
            interval_seconds: int = 3600  # 1-hour default
    ):
        """
        Initialize sync worker

        Args:
            sensor_reading_service: Service for reading operations
            mqtt_publisher: Publisher for MQTT communication
            interval_seconds: Seconds between sync attempts (default: 3600 = 1h)
        """
        super().__init__(
            name="SensorReadingSyncWorker",
            interval_seconds=interval_seconds
        )

        self.sensor_reading_service = sensor_reading_service
        self.mqtt_publisher = mqtt_publisher

    def do_work(self):
        """
        Execute sync work

        Flow:
        1. Check MQTT connection
        2. Get pending readings from a database
        3. Separate alerts from normal readings
        4. Publish alerts individually
        5. Publish normal readings as a batch
        6. Mark successful readings as synced
        """
        logger.info("=== Sensor Reading Sync Worker Started ===")

        # Check MQTT connection
        if not self.mqtt_publisher.is_connected():
            logger.warning(
                "MQTT not connected, skipping sync. "
                "Will retry on next interval."
            )
            return

        # Get pending readings
        logger.info("Fetching pending sync readings...")
        pending_readings = self.sensor_reading_service.get_pending_sync_readings(
            limit=1000  # Process max 1000 per batch
        )

        if not pending_readings:
            logger.info("No pending readings to sync")
            return

        logger.info(f"Found {len(pending_readings)} pending readings")

        # Separate alerts from normal readings
        alerts = [r for r in pending_readings if r.is_alert]
        normal_readings = [r for r in pending_readings if not r.is_alert]

        logger.info(
            f"  - Alerts: {len(alerts)}\n"
            f"  - Normal: {len(normal_readings)}"
        )

        # Track successful syncs
        successfully_synced = []

        # Publish alerts individually
        if alerts:
            logger.info(f"Publishing {len(alerts)} alerts...")
            for alert in alerts:
                try:
                    if self.mqtt_publisher.publish_alert(alert):
                        successfully_synced.append(alert)
                        logger.debug(f"  ✅ Alert {alert.id} published")
                    else:
                        logger.warning(f"  ❌ Alert {alert.id} failed to publish")
                except Exception as e:
                    logger.error(
                        f"  ❌ Error publishing alert {alert.id}: {e}",
                        exc_info=True
                    )

            logger.info(
                f"Alerts published: {len([r for r in successfully_synced if r.is_alert])}/{len(alerts)}"
            )

        # Publish normal readings as a batch
        if normal_readings:
            logger.info(f"Publishing batch of {len(normal_readings)} normal readings...")
            try:
                if self.mqtt_publisher.publish_reading_batch(normal_readings):
                    successfully_synced.extend(normal_readings)
                    logger.info(f"  ✅ Batch published successfully")
                else:
                    logger.warning(f"  ❌ Batch failed to publish")
            except Exception as e:
                logger.error(f"  ❌ Error publishing batch: {e}", exc_info=True)

        # Mark successfully synced readings
        if successfully_synced:
            logger.info(f"Marking {len(successfully_synced)} readings as synced...")
            synced_count = self.sensor_reading_service.mark_readings_as_synced(
                successfully_synced
            )
            logger.info(f"  ✅ Marked {synced_count} readings as synced")

        # Summary
        logger.info(
            f"=== Sync Worker Complete ===\n"
            f"  Total processed: {len(pending_readings)}\n"
            f"  Successfully synced: {len(successfully_synced)}\n"
            f"  Failed: {len(pending_readings) - len(successfully_synced)}"
        )