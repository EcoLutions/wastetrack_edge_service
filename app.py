import logging
import os
from dataclasses import dataclass
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS

from src.containermonitoring.application.event_handlers import (ContainerConfigHandler, DeviceEventHandler, )
from src.containermonitoring.application.services import (ContainerConfigService, DeviceService, SensorReadingService, )
from src.containermonitoring.infrastructure.messaging import (MqttPublisher, MqttSubscriber, )
from src.containermonitoring.infrastructure.persistence import (ContainerConfigRepository, DeviceRepository,
                                                                SensorReadingRepository, )
from src.containermonitoring.interfaces.rest import SensorReadingController
from src.shared.infrastructure.mqtt import MqttConnectionManager

@dataclass
class AppDependencies:
    app: Flask
    mqtt_manager: Optional[MqttConnectionManager]
    mqtt_subscriber: Optional[MqttSubscriber]
    mqtt_publisher: Optional[MqttPublisher]

def configure_logging() -> None:
    """Configure basic logging for the service."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, log_level, logging.INFO)

    log_format = (
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    log_handlers = [logging.StreamHandler()]

    log_file = os.getenv("LOG_FILE")
    if log_file:
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        log_handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(  level=level,format=log_format,handlers=log_handlers, force=True,)

def build_dependencies(start_mqtt: bool = True) -> AppDependencies:
    """
    Build and wire all application dependencies.

    Returns an AppDependencies instance with the Flask app and MQTT helpers.
    """
    load_dotenv()
    configure_logging()

    # Repositories
    device_repo = DeviceRepository()
    config_repo = ContainerConfigRepository()
    reading_repo = SensorReadingRepository()

    # Services
    device_service = DeviceService(device_repo)
    config_service = ContainerConfigService(config_repo)
    reading_service = SensorReadingService(
        reading_repo,
        device_service,
        config_service,
    )

    # REST Controller
    controller = SensorReadingController(reading_service)

    # Flask app
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(controller.get_blueprint())

    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"}), 200

    mqtt_manager: Optional[MqttConnectionManager] = None
    mqtt_subscriber: Optional[MqttSubscriber] = None
    mqtt_publisher: Optional[MqttPublisher] = None

    if start_mqtt:
        mqtt_manager = MqttConnectionManager()
        mqtt_publisher = MqttPublisher(mqtt_manager)
        mqtt_subscriber = MqttSubscriber(
            mqtt_manager,
            DeviceEventHandler(device_repo),
            ContainerConfigHandler(config_repo),
        )

        mqtt_manager.connect()
        mqtt_subscriber.subscribe_to_backend_events()

    return AppDependencies(
        app=app,
        mqtt_manager=mqtt_manager,
        mqtt_subscriber=mqtt_subscriber,
        mqtt_publisher=mqtt_publisher,
    )

def create_app(start_mqtt: bool = True) -> Flask:
    """
    Flask application factory used by gunicorn/Flask CLI.

    start_mqtt can be disabled in tests to avoid broker dependency.
    """
    return build_dependencies(start_mqtt=start_mqtt).app

if __name__ == "__main__":
    enable_mqtt = os.getenv("ENABLE_MQTT", "true").lower() == "true"
    deps = build_dependencies(start_mqtt=enable_mqtt)

    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", 5000))
    debug = os.getenv("FLASK_DEBUG", "False").lower() == "true"

    logging.getLogger(__name__).info("Starting Flask app on %s:%s (debug=%s), MQTT %s", host,port,debug,"enabled" if enable_mqtt else "disabled",)

    deps.app.run(host=host, port=port, debug=debug)
