# python
import logging
import threading
import time
import queue

from config.bluetooth_config import BluetoothConfig
from src.containermonitoring.application.services import BluetoothPollingService
from src.containermonitoring.infrastructure.persistence import DeviceRepository
from src.shared.infrastructure.bluetooth import (
    DeviceConfigLoader
)

logger = logging.getLogger(__name__)


class BluetoothPollingWorker:
    """
    Background worker for polling Bluetooth devices
    """

    def __init__(
            self,
            bluetooth_polling_service: BluetoothPollingService,
            device_config_loader: DeviceConfigLoader,
            device_repository: DeviceRepository
    ):
        """
        Initialize worker

        Args:
            bluetooth_polling_service: Service for polling logic
            device_config_loader: Loader for device configuration
        """
        self.bluetooth_polling_service = bluetooth_polling_service
        self.device_config_loader = device_config_loader
        self.device_repository = device_repository

        self.running = False
        self.thread = None
        self.cycle_count = 0

        # Cola para peticiones de envío de configuración de umbral (thread-safe)
        self.command_queue: "queue.Queue[tuple[str, float]]" = queue.Queue()

        logger.info(
            f"Bluetooth Polling Worker initialized "
            f"(interval: {BluetoothConfig.POLLING_INTERVAL}s)"
        )

    def enqueue_send_threshold_config(self, device_id: str, threshold: float) -> None:
        """
        Encola una petición para enviar el umbral al dispositivo identificado por
        Llamar desde hilos externos (por ejemplo, el post-save handler).
        """
        self.command_queue.put((device_id, float(threshold)))
        logger.info(f"Enqueued threshold config for {device_id}: {threshold}%")

    def start(self):
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

        logger.info("Bluetooth Polling Worker started")

    def stop(self):
        if not self.running:
            logger.warning("Bluetooth Polling Worker is not running")
            return

        logger.info("Stopping Bluetooth Polling Worker...")
        self.running = False

        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=10)

        logger.info("Bluetooth Polling Worker stopped")

    def _run_loop(self):
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

                # Procesar cola de comandos pendientes (antes de los polls)
                self._process_command_queue(devices)

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

    def _process_command_queue(self, devices):
        """
        Drena la cola de comandos y envía cada request usando el servicio.
        Ejecutado en el hilo del worker para usar la misma lógica de acceso al puerto.
        """
        while True:
            try:
                queued_device_id, threshold= self.command_queue.get(timeout=0.1)
            except queue.Empty:
                break

            try:
                logger.info(f"Dequeued threshold config for {queued_device_id} -> {threshold}%")

                bDevice = None

                try:
                    db_device = self.device_repository.find_by_id(queued_device_id)
                except Exception:
                    logger.exception("Error looking up device in repository")
                    db_device = None

                db_identifier = None
                if db_device is not None:
                    if isinstance(db_device, dict):
                        db_identifier = (
                                db_device.get("device_identifier")
                                or db_device.get("deviceIdentifier")
                                or db_device.get("id")
                        )
                    else:
                        db_identifier = (
                                getattr(db_device, "device_identifier", None)
                                or getattr(db_device, "deviceIdentifier", None)
                                or getattr(db_device, "id", None)
                        )

                # Buscar en devices usando preferentemente db_identifier, si no usar queued_device_id
                if devices:
                    for d in devices:
                        if isinstance(d, dict):
                            d_identifier = (
                                    d.get("deviceIdentifier")
                                    or d.get("device_identifier")
                                    or d.get("id")
                            )
                        else:
                            d_identifier = (
                                    getattr(d, "deviceIdentifier", None)
                                    or getattr(d, "device_identifier", None)
                                    or getattr(d, "id", None)
                            )

                        if d_identifier is None:
                            continue

                        # comparar como strings; preferir match con db_identifier si existe
                        if db_identifier is not None:
                            if str(d_identifier) == str(db_identifier) or str(db_identifier) in str(
                                    d_identifier) or str(d_identifier) in str(db_identifier):
                                bDevice = d
                                break
                        else:
                            # fallback: comparar con queued_device_id (igual o parcial)
                            if str(d_identifier) == str(queued_device_id) or str(queued_device_id) in str(
                                    d_identifier) or str(d_identifier) in str(queued_device_id):
                                bDevice = d
                                break

                if not bDevice:
                    id_for_log = db_identifier if db_identifier is not None else queued_device_id
                    logger.warning(f"send_threshold_config: device not found in config: {id_for_log}")
                    continue

                logger.info(f"Processing queued send_threshold_config for {queued_device_id} -> {threshold}%")
                try:
                    success = self.bluetooth_polling_service.send_threshold_config(bDevice, threshold)
                    logger.info(f"send_threshold_config to {queued_device_id} returned: {success}")
                except Exception as e:
                    logger.error(f"Error sending threshold to {queued_device_id}: {e}", exc_info=True)

            finally:
                # marcar la tarea como hecha en la cola si se desea (no obligatorio)
                try:
                    self.command_queue.task_done()
                except Exception:
                    pass

    def _sleep_until_next_cycle(self):
        """Sleep until next polling cycle, allowing quick shutdown"""
        elapsed = 0
        interval = BluetoothConfig.POLLING_INTERVAL

        while elapsed < interval and self.running:
            time.sleep(0.5)
            elapsed += 0.5

    def is_running(self) -> bool:
        return self.running
