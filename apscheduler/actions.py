"""Constants for misfire actions."""

ACTION_NONE = 0        # No action on misfire
ACTION_RUN_ONCE = 1    # Execute the job once if it was missed one or more time
ACTION_RUN_ALL = 2     # Retroactively execute all missed runs
