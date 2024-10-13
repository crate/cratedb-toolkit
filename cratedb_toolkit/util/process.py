import logging
import time
import typing as t

logger = logging.getLogger(__name__)


class FixedBackoff:
    def __init__(self, sequence: t.List[int]):
        self.sequence = sequence
        self.attempt = 0

    def next(self) -> None:
        self.attempt += 1
        if self.attempt < len(self.sequence):
            delay = self.sequence[self.attempt]
        else:
            delay = self.sequence[-1]
        logger.info(f"Retry attempt #{self.attempt} in {delay} seconds")
        time.sleep(delay)

    def reset(self) -> None:
        self.attempt = 0
