"""
This module contains just the code for the scheduled task.
It should not be run directly.
"""

from __future__ import annotations

from datetime import datetime


def tick():
    print("Hello, the time is", datetime.now())
