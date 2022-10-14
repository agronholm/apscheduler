from __future__ import annotations

INDEX_TRANSLATIONS = {1: "1st", 2: "2nd", 3: "3rd"}


class CronParserError(ValueError):
    def __init__(self, sequence, field_name, index, expression, reasons):

        self.sequence = sequence
        self.field_name = field_name
        self.field_index = index
        self.expression = expression
        self.reasons = reasons

        msg = ""
        if sequence and field_name and index:
            msg = (
                f'Unrecognized pattern "{sequence}" in the {field_name.replace("_", " ").title()} field '
                f'({INDEX_TRANSLATIONS.get(index, f"{index}th")} field of crontab "{self.expression}")'
            )

        if reasons:
            if msg:
                msg += ":\n" + "\n".join(reasons)
            else:
                msg = reasons

        super().__init__(msg)
