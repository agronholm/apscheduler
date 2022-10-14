from __future__ import annotations


class CronBaseRule:

    PICKLE_VERSION = 1

    REGEX = ""
    EXCEPTION = 'Invalid {position} value "{value}"'

    def __init__(self, first=None, last=None, step=None, min_val=0, max_val=9999):

        self.first = first
        self.last = last

        self.first_is_numeric = True
        self.last_is_numeric = True

        self.last_from_first = False

        self.step = step

        self.min_val = min_val
        self.max_val = max_val

    def _raise(self, **kwargs):

        if isinstance(self.EXCEPTION, str):
            msg = self.EXCEPTION

        elif isinstance(self.EXCEPTION, dict):
            msg = self.EXCEPTION[kwargs.get("option")]

        else:
            msg = ", ".join(f"{key} {val}" for key, val in kwargs.items())

        raise ValueError(msg.format(**kwargs))

    def parse(self, value, value_type):

        try:
            return int(value), True

        except TypeError:
            return None, False

    def prettify(self, value, is_numeric=None):

        return value

    def _validate_first(self):

        if self.first < self.min_val:

            raise ValueError(
                f"the first value ({self.prettify(self.first, self.first_is_numeric)}) "
                f"is lower than the minimum value ({self.min_val})"
            )

    def _validate_last(self):

        if self.last is not None:

            if self.last > self.max_val:

                raise ValueError(
                    f"the last value {self.prettify(self.last, self.last_is_numeric)} "
                    f"is higher than the maximum value ({self.max_val})"
                )

            if self.first > self.last:

                raise ValueError(
                    f"The first value {self.prettify(self.first, self.first_is_numeric)} "
                    "is higher than "
                    f"the last value {self.prettify(self.last, self.last_is_numeric)}"
                )

    def _validate_step(self):

        value_range = (self.last or self.max_val) - self.first

        if self.step and self.step > value_range:
            raise ValueError(
                f"the step value ({self.step}) is higher "
                f"than the total range of the expression ({value_range})"
            )

    def validate(self):

        for meth in (self._validate_first, self._validate_last, self._validate_step):
            meth()

    def next(self, date, field):

        min_val = max(field.min_val(date), self.first)

        max_val = field.max_val(date)
        if self.last is not None:
            max_val = min(field.max_val(date), self.last)

        next_val = max(min_val, field.cur_val(date))

        if self.step:
            distance_to_next = (self.step - (next_val - min_val)) % self.step
            next_val += distance_to_next

        return next_val if next_val <= max_val else None

    def __eq__(self, other):

        return (
            isinstance(other, self.__class__)
            and self.first == other.first
            and self.last == other.last
            and self.step == other.step
        )

    def __str__(self):

        string = self.prettify(self.first, self.first_is_numeric)

        if self.last is not None and not self.last_from_first:
            string += f"-{self.prettify(self.last, self.last_is_numeric)}"

        if self.step:
            string += f"/{self.step}"

        return string

    def __repr__(self):

        return (
            f"{self.__class__.__name__}("
            f"first='{self.prettify(self.first, self.first_is_numeric)}'"
            f"last='{self.prettify(self.last, self.last_is_numeric) if not self.last_from_first else None}'"
            f"step='{self.step}')"
        )

    def __getstate__(self):

        return {
            "version": self.PICKLE_VERSION,
            "first": self.first,
            "last": self.last,
            "first_is_numeric": self.first_is_numeric,
            "last_is_numeric": self.last_is_numeric,
            "last_from_first": self.last_from_first,
            "step": self.step,
            "min_val": self.min_val,
            "max_val": self.max_val,
        }

    def __setstate__(self, state):

        version = state.pop("version", 1)

        if version > self.PICKLE_VERSION:
            raise ValueError(
                f"Got serialized data for version {version} of {self.__class__.__name__}, "
                f"but only versions up to {self.PICKLE_VERSION} can be handled"
            )

        for key, val in state.items():
            setattr(self, key, val)
