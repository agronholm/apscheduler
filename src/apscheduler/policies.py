from enum import Enum, auto


class ConflictPolicy(Enum):
    #: replace the existing schedule with a new one
    replace = auto()
    #: keep the existing schedule as-is and drop the new schedule
    do_nothing = auto()
    #: raise an exception if a conflict is detected
    exception = auto()


class CoalescePolicy(Enum):
    #: run once, with the earliest fire time
    earliest = auto()
    #: run once, with the latest fire time
    latest = auto()
    #: submit one job for every accumulated fire time
    all = auto()
