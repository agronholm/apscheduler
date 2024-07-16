from __future__ import annotations

from logging import Logger

import attrs
from attr.validators import instance_of
from tenacity import (
    AsyncRetrying,
    RetryCallState,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)
from tenacity.stop import stop_base
from tenacity.wait import wait_base


@attrs.define(kw_only=True, frozen=True)
class RetrySettings:
    """
    Settings for retrying an operation with Tenacity.

    :param stop: defines when to stop trying
    :param wait: defines how long to wait between attempts
    """

    stop: stop_base = attrs.field(
        validator=instance_of(stop_base),
        default=stop_after_delay(60),
    )
    wait: wait_base = attrs.field(
        validator=instance_of(wait_base),
        default=wait_exponential(min=0.5, max=20),
    )


@attrs.define(kw_only=True, slots=False)
class RetryMixin:
    """
    Mixin that provides support for retrying operations.

    :param retry_settings: Tenacity settings for retrying operations in case of a
        database connecitivty problem
    """

    retry_settings: RetrySettings = attrs.field(default=RetrySettings())
    _logger: Logger = attrs.field(init=False)

    @property
    def _temporary_failure_exceptions(self) -> tuple[type[Exception], ...]:
        """
        Tuple of exception classes which indicate that the operation should be retried.

        """
        return ()

    def _retry(self) -> AsyncRetrying:
        def after_attempt(retry_state: RetryCallState) -> None:
            self._logger.warning(
                "Temporary data store error (attempt %d): %s",
                retry_state.attempt_number,
                retry_state.outcome.exception(),
            )

        return AsyncRetrying(
            stop=self.retry_settings.stop,
            wait=self.retry_settings.wait,
            retry=retry_if_exception_type(self._temporary_failure_exceptions),
            after=after_attempt,
            reraise=True,
        )
