from enum import Enum, auto


class RunState(Enum):
    starting = auto()
    started = auto()
    stopping = auto()
    stopped = auto()
