from .all import CronDayOfWeekAllRule
from .position import CronDayOfWeekPositionRule
from .question_mark import CronDayOfWeekQuestionMarkRule
from .range import CronDayOfWeekRangeRule


DAY_OF_WEEK_RULES = (
    CronDayOfWeekQuestionMarkRule,
    CronDayOfWeekAllRule,
    CronDayOfWeekRangeRule,
    CronDayOfWeekPositionRule
)
