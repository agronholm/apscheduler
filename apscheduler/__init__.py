import sys


fallback_release = '3.5.0'
if sys.version_info >= (3, 8):
    from importlib.metadata import version, PackageNotFoundError

    try:
        release = version('APScheduler').split('-')[0]
    except PackageNotFoundError:
        release = fallback_release

    del version, PackageNotFoundError
else:
    from pkg_resources import get_distribution, DistributionNotFound

    try:
        release = get_distribution('APScheduler').version.split('-')[0]
    except DistributionNotFound:
        release = fallback_release

    del get_distribution, DistributionNotFound

version_info = tuple(int(x) if x.isdigit() else x for x in release.split('.'))
version = __version__ = '.'.join(str(x) for x in version_info[:3])
del sys, fallback_release
