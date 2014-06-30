"""
Demonstrates how to use the Qt compatible scheduler to schedule a job that executes on 3 second intervals.
"""

from datetime import datetime
import signal
import sys

from apscheduler.schedulers.qt import QtScheduler

try:
    from PyQt5.QtWidgets import QApplication, QLabel
except ImportError:
    try:
        from PyQt4.QtGui import QApplication, QLabel
    except ImportError:
        from PySide.QtGui import QApplication, QLabel


def tick():
    label.setText('Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    app = QApplication(sys.argv)
    signal.signal(signal.SIGINT, lambda *args: QApplication.quit())  # This enables processing of Ctrl+C keypresses
    label = QLabel('The timer text will appear here in a moment!')
    label.setWindowTitle('QtScheduler example')
    label.setFixedSize(280, 50)
    label.show()

    scheduler = QtScheduler()
    scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.start()

    # Execution will block here until the user closes the windows or Ctrl+C is pressed.
    app.exec_()
