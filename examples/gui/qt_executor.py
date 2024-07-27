from __future__ import annotations

import sys
from datetime import datetime

from apscheduler import Scheduler
from apscheduler.executors.qt import QtJobExecutor
from apscheduler.triggers.interval import IntervalTrigger

try:
    from PySide6.QtWidgets import QApplication, QLabel, QMainWindow
except ImportError:
    try:
        from PyQt6.QtWidgets import QApplication, QLabel, QMainWindow
    except ImportError:
        raise ImportError("Either PySide6 or PyQt6 is needed to run this") from None


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()

        self.setWindowTitle("APScheduler demo")

        self.clock = QLabel()
        font = self.clock.font()
        font.setPointSize(30)
        self.clock.setFont(font)

        self.update_time()
        self.setCentralWidget(self.clock)

    def update_time(self) -> None:
        now = datetime.now()
        self.clock.setText(f"The time is now {now:%H:%M:%S}")


app = QApplication(sys.argv)
window = MainWindow()
window.show()
with Scheduler() as scheduler:
    scheduler.job_executors["qt"] = QtJobExecutor()
    scheduler.add_schedule(
        window.update_time, IntervalTrigger(seconds=1), job_executor="qt"
    )
    scheduler.start_in_background()
    app.exec()
