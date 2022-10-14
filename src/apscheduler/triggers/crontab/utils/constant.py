from __future__ import annotations

from datetime import timedelta as datetime_timedelta

MONTHS = (
    "jan",
    "feb",
    "mar",
    "apr",
    "may",
    "jun",
    "jul",
    "aug",
    "sep",
    "oct",
    "nov",
    "dec",
)
MONTHS_STR_RE = "|".join(MONTHS)
MONTHS_INT_RE = "[1-9]|1[0-2]"
MONTHS_RE = f"{MONTHS_STR_RE}|{MONTHS_INT_RE}"

WEEKDAYS_SUNDAY = ("sun", "mon", "tue", "wed", "thu", "fri", "sat")
WEEKDAYS_MONDAY = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")

WEEKDAYS_STR_RE = "|".join(WEEKDAYS_SUNDAY)
WEEKDAYS_INT_RE = "[1-7]"
WEEKDAYS_RE = f"{WEEKDAYS_STR_RE}|{WEEKDAYS_INT_RE}"

WEEKDAY_OPTIONS_SHORT = ["#1", "#2", "#3", "#4", "#5", "l"]
WEEKDAY_OPTIONS_SHORT_RE = "|".join(WEEKDAY_OPTIONS_SHORT)
WEEKDAY_OPTIONS_LONG = ["1st", "2nd", "3rd", "4th", "5th", "last"]
WEEKDAY_OPTIONS_LONG_RE = "|".join(WEEKDAY_OPTIONS_LONG)

MONTH_RANGE_RE = "3[01]|[12][0-9]|[1-9]"

BUSINESS_DAYS = ["bus"] * 5 + ["sat", "sun"]

MICROSECOND_TIMEDELTA = datetime_timedelta(microseconds=1)
