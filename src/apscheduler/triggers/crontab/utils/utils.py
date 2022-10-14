from __future__ import annotations

from datetime import datetime as datetime_datetime
from datetime import timedelta as datetime_timedelta


def str_lower_strip(string, replace_char=" "):

    if not isinstance(string, str):
        string = str(string)

    return replace_char.join(string.split()).lower()


def ceil_date(date: datetime_datetime) -> datetime_datetime:

    """
    Rounds the given datetime object upwards.

    :type date: datetime

    """

    if date.microsecond > 0:
        return date + datetime_timedelta(seconds=1, microseconds=-date.microsecond)
    return date


def fix_daylight_saving_time_shift(
    first_date: datetime_datetime, second_date: datetime_datetime
) -> datetime_datetime:

    first_dst = first_date.dst()
    second_dst = second_date.dst()

    if first_dst and second_dst and first_dst != second_dst:

        dst_diff = first_dst - second_dst
        if dst_diff:
            second_date += dst_diff

    return second_date
