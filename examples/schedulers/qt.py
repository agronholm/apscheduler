"""
Demonstrates how to use the Qt compatible scheduler to schedule a job that executes on 3 second
intervals.
"""

from datetime import datetime
import signal
import sys
from importlib import import_module
from itertools import product

from apscheduler.schedulers.qt import QtScheduler

for version, pkgname in product(range(6, 1, -1), ("PySide", "PyQt")):
    try:
        qtwidgets = import_module(pkgname + str(version) + ".QtWidgets")
    except ImportError:
        pass
    else:
        QApplication = qtwidgets.QApplication
        QLabel = qtwidgets.QLabel
        break
else:
    raise ImportError(
        "Could not import the QtWidgets module from either PySide or PyQt"
    )


def tick():
    label.setText('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    app = QApplication(sys.argv)

    # This enables processing of Ctrl+C keypresses
    signal.signal(signal.SIGINT, lambda *args: QApplication.quit())

    label = QLabel('The timer text will appear here in a moment!')
    label.setWindowTitle('QtScheduler example')
    label.setFixedSize(280, 50)
    label.show()

    scheduler = QtScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()

    # Execution will block here until the user closes the windows or Ctrl+C is pressed.
    app.exec()
