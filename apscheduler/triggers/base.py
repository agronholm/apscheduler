from abc import ABCMeta, abstractmethod

import six


class BaseTrigger(six.with_metaclass(ABCMeta)):
    @abstractmethod
    def get_next_fire_time(self, start_date):
        """
        Returns the next datetime, equal to or greater than ``start_date``, when this trigger will fire.
        If no such datetime can be calculated, returns ``None``.

        :type start_date: datetime.datetime
        """
