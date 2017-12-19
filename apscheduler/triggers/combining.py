from apscheduler.triggers.base import BaseTrigger
from apscheduler.util import obj_to_ref, ref_to_obj


class BaseMultiTrigger(BaseTrigger):
    __slots__ = 'triggers'

    def __init__(self, triggers):
        self.triggers = triggers

    def __getstate__(self):
        return {
            'version': 1,
            'triggers': [(obj_to_ref(trigger.__class__), trigger.__getstate__())
                         for trigger in self.triggers]
        }

    def __setstate__(self, state):
        if state.get('version', 1) > 1:
            raise ValueError(
                'Got serialized data for version %s of %s, but only versions up to 1 can be '
                'handled' % (state['version'], self.__class__.__name__))

        self.triggers = []
        for clsref, state in state['triggers']:
            cls = ref_to_obj(clsref)
            trigger = cls.__new__(cls)
            trigger.__setstate__(state)
            self.triggers.append(trigger)

    def __repr__(self):
        return '<{}({})>'.format(self.__class__.__name__, self.triggers)


class AndTrigger(BaseMultiTrigger):
    """
    Always returns the earliest next fire time that all the given triggers can agree on.
    The trigger is considered to be finished when any of the given triggers has finished its
    schedule.

    Trigger alias: ``and``

    :param list triggers: triggers to combine
    """

    __slots__ = ()

    def get_next_fire_time(self, previous_fire_time, now):
        while True:
            fire_times = [trigger.get_next_fire_time(previous_fire_time, now)
                          for trigger in self.triggers]
            if None in fire_times:
                return None
            elif min(fire_times) == max(fire_times):
                return fire_times[0]
            else:
                now = max(fire_times)

    def __str__(self):
        return 'and[{}]'.format(', '.join(str(trigger) for trigger in self.triggers))


class OrTrigger(BaseMultiTrigger):
    """
    Always returns the earliest next fire time produced by any of the given triggers.
    The trigger is considered finished when all the given triggers have finished their schedules.

    Trigger alias: ``or``

    :param list triggers: triggers to combine

    .. note:: Triggers that depends on the previous fire time, such as the interval trigger, may
        seem to behave strangely since they are always passed the previous fire time produced by
        any of the given triggers.
    """

    __slots__ = ()

    def get_next_fire_time(self, previous_fire_time, now):
        fire_times = [trigger.get_next_fire_time(previous_fire_time, now)
                      for trigger in self.triggers]
        fire_times = [fire_time for fire_time in fire_times if fire_time is not None]
        return min(fire_times) if fire_times else None

    def __str__(self):
        return 'or[{}]'.format(', '.join(str(trigger) for trigger in self.triggers))
