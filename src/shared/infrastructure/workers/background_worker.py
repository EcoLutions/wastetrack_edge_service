import logging
import threading
import time
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class BackgroundWorker(ABC):
    """
    Base class for background workers

    Provides infrastructure for periodic task execution in a separate thread.
    Subclasses implement the do_work() method with their specific logic.

    Features:
    - Runs in daemon thread (won't prevent app shutdown)
    - Configurable interval
    - Graceful start/stop
    - Exception handling
    """

    def __init__(self, name: str, interval_seconds: int):
        """
        Initialize background worker

        Args:
            name: Worker name (for logging)
            interval_seconds: Seconds between work executions
        """
        self.name = name
        self.interval_seconds = interval_seconds
        self.running = False
        self.thread: threading.Thread = None

        logger.info(
            f"Background worker '{name}' initialized "
            f"(interval: {interval_seconds}s)"
        )

    @abstractmethod
    def do_work(self):
        """
        Implement this method with the worker's logic

        This method will be called periodically at the configured interval.
        Should handle its own exceptions.
        """
        pass

    def start(self):
        """Start the background worker"""
        if self.running:
            logger.warning(f"Worker '{self.name}' is already running")
            return

        logger.info(f"Starting background worker: {self.name}")
        self.running = True

        self.thread = threading.Thread(
            target=self._run_loop,
            daemon=True,  # Daemon thread won't prevent app shutdown
            name=f"Worker-{self.name}"
        )
        self.thread.start()

        logger.info(f"✅ Worker '{self.name}' started")

    def stop(self):
        """Stop the background worker"""
        if not self.running:
            logger.warning(f"Worker '{self.name}' is not running")
            return

        logger.info(f"Stopping background worker: {self.name}")
        self.running = False

        # Wait for the thread to finish (with timeout)
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=5)

        logger.info(f"✅ Worker '{self.name}' stopped")

    def _run_loop(self):
        """Internal loop that executes work periodically"""
        logger.info(f"Worker '{self.name}' loop started")

        while self.running:
            try:
                # Execute work
                self.do_work()
            except Exception as e:
                logger.error(
                    f"Error in worker '{self.name}': {e}",
                    exc_info=True
                )

            # Sleep until next execution
            # Use multiple small sleeps to allow quick shutdown
            elapsed = 0
            while elapsed < self.interval_seconds and self.running:
                time.sleep(min(1, self.interval_seconds - elapsed))
                elapsed += 1

        logger.info(f"Worker '{self.name}' loop ended")

    def is_running(self) -> bool:
        """Check if a worker is running"""
        return self.running