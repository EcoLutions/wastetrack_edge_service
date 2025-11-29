import paho.mqtt.client as mqtt
import logging
import json
import time
import threading
from typing import Callable, Dict, Optional
from config.mqtt_config import MqttConfig

logger = logging.getLogger(__name__)

class MqttConnectionManager:
    """
    Manage MQTT connection between Edge and Backend

    Responsibilities:
        - Connect/disconnect from the MQTT broker
        - Subscribe to topics (Backend → Edge)
        - Publish messages (Edge → Backend)
        - Automatic reconnection if connection is lost
        - Routing messages to specific handlers
        - Support for MQTT wildcards (+ and #)
    """

    def __init__(self):
        # Create MQTT client instance with persistent session
        self.client = mqtt.Client(
            client_id=MqttConfig.CLIENT_ID,
            clean_session=False
        )

        # Configur actual callbacks
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        # Config authentication if provided
        if MqttConfig.has_authentication():
            self.client.username_pw_set(
                MqttConfig.USERNAME,
                MqttConfig.PASSWORD
            )

        # Handlers for subscribed topics
        self.message_handlers: Dict[str, Callable] = {}

        # Connection state
        self.connected = False
        self.reconnecting = False

        logger.info("MQTT Connection Manager initialized")

    def connect(self):
        """
        Connection to the MQTT broker
        Note: This method starts the network loop in a background thread.
        """
        try:
            logger.info(
                f"Connecting to MQTT broker: "
                f"{MqttConfig.BROKER_HOST}:{MqttConfig.BROKER_PORT}"
            )

            self.client.connect(
                MqttConfig.BROKER_HOST,
                MqttConfig.BROKER_PORT,
                MqttConfig.KEEP_ALIVE
            )

            # Start network loop in background thread
            self.client.loop_start()

        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            self._schedule_reconnect()

    def disconnect(self):
        """Disconnect from the MQTT broker"""
        logger.info("Disconnecting from MQTT broker...")
        self.client.loop_stop()
        self.client.disconnect()
        self.connected = False
        logger.info("Disconnected from MQTT broker")

    def subscribe(self, topic: str, handler: Callable[[str, dict], None]):
        """
        Subscribe to an MQTT topic and register its handler

        Args:
            topic: MQTT topic (may include wildcards + or #)
                   Example: "cm/devices/events/created"
                   Example: "cm/containers/events/+"
            handler: Function to handle incoming messages for this topic
                     Signature: handler(topic: str, payload: dict)

        Wildcards MQTT:
            + : Single level wildcard (ej: cm/devices/+)
            # : Multi level wildcard (ej: cm/#)
        """
        # Subscribe to topic
        result = self.client.subscribe(topic, qos=MqttConfig.QOS_SUBSCRIBE)

        if result[0] == mqtt.MQTT_ERR_SUCCESS:
            # Registrar handler
            self.message_handlers[topic] = handler
            logger.info(f"✅ Subscribed to topic: {topic}")
        else:
            logger.error(f"❌ Failed to subscribe to topic: {topic}")

    def publish(self, topic: str, payload: dict, retain: bool = False) -> bool:
        """
        Publish a message to an MQTT topic

        Args:
            topic: Destination MQTT topic
            payload: Message payload as a dictionary
            retain: If True, the broker will save the last message.
                    (useful for configurations)

        Returns:
            True if successfully published, False otherwise
        """
        if not self.connected:
            logger.warning(f"Cannot publish to {topic}: not connected to broker")
            return False

        try:
            # Serialize payload to JSON
            payload_str = json.dumps(payload, default=str)

            # Publish message
            result = self.client.publish(
                topic,
                payload_str,
                qos=MqttConfig.QOS_PUBLISH,
                retain=retain
            )

            # Check result
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                logger.info(f"✅ Published to {topic}")
                logger.debug(f"   Payload: {payload_str[:200]}...")
                return True
            else:
                logger.error(f"❌ Failed to publish to {topic}: rc={result.rc}")
                return False

        except Exception as e:
            logger.error(f"Error publishing to {topic}: {e}", exc_info=True)
            return False

    def _on_connect(self, client, userdata, flags, rc):
        """
        Callback when connecting to the broker

        rc codes:
            0: Connection successful
            1: Connection refused - incorrect protocol version
            2: Connection refused - invalid client identifier
            3: Connection refused - server unavailable
            4: Connection refused - bad username or password
            5: Connection refused - not authorized
        """
        if rc == 0:
            self.connected = True
            self.reconnecting = False
            logger.info("✅ MQTT connection successful")

            # Re-subscribe to all registered topics
            if self.message_handlers:
                logger.info("Re-subscribing to registered topics...")
                for topic in self.message_handlers.keys():
                    self.client.subscribe(topic, qos=MqttConfig.QOS_SUBSCRIBE)
                    logger.info(f"   Re-subscribed: {topic}")
        else:
            self.connected = False
            error_messages = {
                1: "Incorrect protocol version",
                2: "Invalid client identifier",
                3: "Server unavailable",
                4: "Bad username or password",
                5: "Not authorized"
            }
            error_msg = error_messages.get(rc, f"Unknown error code: {rc}")
            logger.error(f"❌ MQTT connection failed: {error_msg}")
            self._schedule_reconnect()

    def _on_disconnect(self, client, userdata, rc):
        """
        Callback when disconnecting from the broker

        rc = 0: Normal disconnection (call to disconnect())
        rc != 0: Unexpected disconnection
        """
        self.connected = False

        if rc != 0:
            logger.warning(f"⚠️  Unexpected MQTT disconnect (rc={rc})")
            self._schedule_reconnect()
        else:
            logger.info("MQTT disconnected normally")

    def _on_message(self, client, userdata, msg):
        """
        Callback when an MQTT message arrives

        Args:
            msg: Object MQTTMessage with attributes:
                 - topic: str
                 - payload: bytes
                 - qos: int
                 - retain: bool
        """
        topic = msg.topic

        try:
            # Decode payload
            payload_str = msg.payload.decode('utf-8')

            # Parse JSON
            try:
                payload_dict = json.loads(payload_str)
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON payload on {topic}: {e}")
                logger.error(f"Raw payload: {payload_str[:500]}")
                return

            logger.info(f"📨 Received message on: {topic}")
            logger.debug(f"   Payload: {payload_str[:200]}...")

            # Search for handler matching topic
            handler = self._find_handler(topic)

            if handler:
                try:
                    # Call the handler
                    handler(topic, payload_dict)
                except Exception as e:
                    logger.error(
                        f"Error in message handler for {topic}: {e}",
                        exc_info=True
                    )
            else:
                logger.warning(f"No handler found for topic: {topic}")

        except Exception as e:
            logger.error(
                f"Error processing message on {topic}: {e}",
                exc_info=True
            )

    def _find_handler(self, topic: str) -> Optional[Callable]:
        """
        Find the handler for a topic, supporting wildcards

        Args:
            topic: Topic of the received message

        Returns:
            Handler function o None si no hay match

        Examples:
            topic="cm/devices/events/created"
            pattern="cm/devices/events/created" → Match
            pattern="cm/devices/events/+" → Match
            pattern="cm/devices/#" → Match
            pattern="cm/containers/+" → No Match
        """
        # First search for exact match
        if topic in self.message_handlers:
            return self.message_handlers[topic]

        # Then search for wildcard matches
        for pattern, handler in self.message_handlers.items():
            if self._topic_matches(pattern, topic):
                return handler

        return None

    def _topic_matches(self, pattern: str, topic: str) -> bool:
        """
        Check if a topic matches a pattern (MQTT wildcards)

        Wildcards:
            + : single level wildcard
                Example: "cm/devices/+" matches "cm/devices/events"
            # : multi level wildcard (debe ser el último)
                Example: "cm/#" matches "cm/devices/events/created"

        Args:
            pattern: Pattern con wildcards
            topic: Received topic

        Returns:
            True if there is a match, False otherwise
        """
        pattern_parts = pattern.split('/')
        topic_parts = topic.split('/')

        # Special case: # (multi-level wildcard)
        if '#' in pattern_parts:
            # # should be the last element
            if pattern_parts[-1] == '#':
                # Compare up to the penultimate element
                pattern_parts = pattern_parts[:-1]
                return all(
                    p == '+' or p == t
                    for p, t in zip(pattern_parts, topic_parts[:len(pattern_parts)])
                )

        # If there is no #, they must have the same number of levels.
        if len(pattern_parts) != len(topic_parts):
            return False

        # Compare level by level
        return all(
            p == '+' or p == t
            for p, t in zip(pattern_parts, topic_parts)
        )

    def _schedule_reconnect(self):
        """
        Schedule a reconnection attempt after RECONNECT_DELAY

        Use a separate thread so as not to block the main thread.
        """
        if self.reconnecting:
            return  # An attempt at reconnection is already underway.

        self.reconnecting = True
        logger.info(f"Scheduling reconnect in {MqttConfig.RECONNECT_DELAY}s...")

        def reconnect():
            time.sleep(MqttConfig.RECONNECT_DELAY)
            if not self.connected:
                logger.info("Attempting to reconnect to MQTT broker...")
                self.connect()

        thread = threading.Thread(target=reconnect, daemon=True)
        thread.start()

    def is_connected(self) -> bool:
        """Returns True if connected to the broker"""
        return self.connected