from enum import Enum, auto


class RunState(Enum):
    starting = auto()
    started = auto()
    stopping = auto()
    stopped = auto()


class JobOutcome(Enum):
    success = auto()
    failure = auto()
    missed_start_deadline = auto()
    cancelled = auto()
    expired = auto()
