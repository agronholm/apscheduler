from abc import ABCMeta, abstractmethod

import six


class BaseTrigger(six.with_metaclass(ABCMeta)):
    """Abstract base class that defines the interface that every trigger must implement."""

    @abstractmethod
    def get_next_fire_time(self, previous_fire_time, now):
        """
        Returns the next datetime to fire on, If no such datetime can be calculated, returns ``None``.

        :param datetime.datetime previous_fire_time: the previous time the trigger was fired
        :param datetime.datetime now: current datetime
        """
