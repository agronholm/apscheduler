import sys

import pytest


def minpython(*version):
    version_str = '.'.join([str(num) for num in version])

    def outer(func):
        dec = pytest.mark.skipif(sys.version_info < version,
                                 reason='This test requires at least Python %s' % version_str)
        return dec(func)
    return outer
