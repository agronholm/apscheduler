from __future__ import annotations

from ...rule.base.question_mark import CronQuestionMarkRule
from .base import CronBaseField


class CronDayBaseField(CronBaseField):
    def parse(self, expr):

        super().parse(expr)

        if len(self.rules) > 1:

            rules_to_pop = []

            for i, rule in enumerate(self.rules):
                if isinstance(rule, CronQuestionMarkRule):
                    rules_to_pop.append(i)

            for i in rules_to_pop:
                self.rules.pop(i)
