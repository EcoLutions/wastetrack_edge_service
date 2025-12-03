import threading
import logging
import time
from typing import Optional, Dict, ContextManager
from contextlib import contextmanager

import serial
from serial.tools import list_ports

logger = logging.getLogger(__name__)


class SerialClientAdapter:
    """
    Adaptador mínimo que expone la interfaz que espera el resto del código.
    Usa un `serial.Serial` ya abierto internamente.
    """

    def __init__(self, raw_serial_client):
        self._client = raw_serial_client

    def connect(self) -> bool:
        # Ya está abierto por el manager
        return True

    def disconnect(self) -> None:
        # No cerramos aquí: el manager controla el ciclo de vida
        return None

    def write(self, data: bytes) -> int:
        return self._client.write(data)

    def read(self, size: int = 1) -> bytes:
        return self._client.read(size)

    def readline(self) -> bytes:
        return self._client.readline()

    def flush(self) -> None:
        try:
            self._client.flush()
        except Exception:
            logger.debug("Flush failed on adapted serial", exc_info=True)


    def send_message(self, message: Dict) -> bool:
        try:
            # Suponiendo que el cliente real tiene un metodo para enviar bytes/strings
            self._send_raw(message)
            return True
        except Exception as e:
            logger.error("Error sending message via serial client: %s", e, exc_info=True)
            return False

    def send_and_wait_response(self, message: Dict, expected_topic: str, timeout: float) -> Optional[Dict]:
        if not self.send_message(message):
            return None

        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                raw = self._read_raw_message(timeout=0.1)
                if not raw:
                    continue
                # parsear raw a dict/message según protocolo (ajustar según tu implementación)
                parsed = self._parse_raw_message(raw)
                if parsed.get("topic") == expected_topic:
                    return parsed
            except Exception:
                continue
        return None

    # --- Métodos auxiliares que debes adaptar al cliente real ---
    def _send_raw(self, message: Dict) -> None:
        """
        Serializa y envía el mensaje usando el cliente serie real.
        Reemplazar por: self._client.write(...), self._client.send(...), etc.
        """
        payload = self._serialize_message(message)
        if hasattr(self._client, "write"):
            self._client.write(payload.encode("utf-8"))
        elif hasattr(self._client, "send_message"):
            self._client.send_message(payload)
        else:
            raise RuntimeError("Raw serial client no tiene método de envío conocido")

    def _read_raw_message(self, timeout: float) -> Optional[str]:
        """
        Lee datos desde el cliente serie; ajustar según la API (read, readline, pop, etc.).
        """
        if hasattr(self._client, "readline"):
            # readline blocking/with timeout según implementación
            return self._client.readline().decode("utf-8")
        if hasattr(self._client, "read_message"):
            return self._client.read_message(timeout=timeout)
        return None

    def _serialize_message(self, message: Dict) -> str:
        import json
        return json.dumps(message)

    def _parse_raw_message(self, raw: str) -> Dict:
        import json
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return json.loads(raw)
        except Exception:
            return {}

# python
class SerialPortManager:
    """
    Manager que reusa conexiones `serial.Serial` por puerto y proporciona
    un contexto \`with\` que bloquea el puerto mientras se usa.
    """

    _instance = None
    _global_lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._connections = {}  # type: Dict[str, Dict]
        return cls._instance

    def _port_exists(self, port_name: str) -> bool:
        return any(p.device == port_name for p in list_ports.comports())

    def _open_serial(self, port: str, baudrate: int, timeout: float) -> Optional[serial.Serial]:
        try:
            ser = serial.Serial(port=port, baudrate=baudrate, timeout=timeout)
            logger.info(f"Opened serial port {port}")
            return ser
        except Exception:
            logger.exception(f"Failed to open serial port {port}")
            return None

    @contextmanager
    def get_connection(self, port: str, baudrate: int = 115200, timeout: float = 5.0) -> ContextManager[Optional[SerialClientAdapter]]:
        """
        Context manager: usage:

        with SerialPortManager().get_connection('COM4') as adapter:
            if not adapter:
                return False
            command_sender = BluetoothCommandSender(adapter)
            command_sender.set_threshold(...)

        Devuelve \`SerialClientAdapter\` con el puerto bloqueado durante el contexto.
        """
        with SerialPortManager._global_lock:
            entry = self._connections.get(port)
            if not entry:
                if not self._port_exists(port):
                    logger.error(f"Port {port} not present")
                    yield None
                    return
                ser = self._open_serial(port, baudrate, timeout)
                if not ser:
                    yield None
                    return
                entry = {
                    "serial": ser,
                    "lock": threading.Lock(),
                    "refcount": 0
                }
                self._connections[port] = entry

            # increase refcount for diagnostic (not strictly needed)
            entry["refcount"] += 1
            lock = entry["lock"]
            ser = entry["serial"]

        # Acquire per-port usage lock while yielding adapter
        lock.acquire()
        try:
            adapter = SerialClientAdapter(ser)
            yield adapter
        finally:
            lock.release()
            with SerialPortManager._global_lock:
                entry = self._connections.get(port)
                if entry:
                    entry["refcount"] -= 1
                    # Optionally close when refcount == 0 and you want to free resource
                    # keep-open is usually better for performance; uncomment to close:
                    # if entry["refcount"] <= 0:
                    #     try:
                    #         entry["serial"].close()
                    #     except Exception:
                    #         logger.exception("Error closing serial")
                    #     self._connections.pop(port, None)

    def close_connection(self, port: str) -> None:
        """
        Forzar cierre del puerto (useful for cleanup).
        """
        with SerialPortManager._global_lock:
            entry = self._connections.pop(port, None)
            if entry:
                try:
                    entry["serial"].close()
                    logger.info(f"Closed serial port {port}")
                except Exception:
                    logger.exception(f"Error closing serial port {port}")
