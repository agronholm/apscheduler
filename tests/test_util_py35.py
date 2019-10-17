from functools import partial

from apscheduler.util import iscoroutinefunction_partial


class TestIsCoroutineFunctionPartial:
    @staticmethod
    def not_a_coro(x):
        pass

    @staticmethod
    async def a_coro(x):
        pass

    def test_non_coro(self):
        assert not iscoroutinefunction_partial(self.not_a_coro)

    def test_coro(self):
        assert iscoroutinefunction_partial(self.a_coro)

    def test_coro_partial(self):
        assert iscoroutinefunction_partial(partial(self.a_coro, 1))
