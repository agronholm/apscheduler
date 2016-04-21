# These will be removed in APScheduler 4.0.
release = __import__('pkg_resources').get_distribution('APScheduler').version.split('-')[0]
version_info = tuple(int(x) if x.isdigit() else x for x in release.split('.'))
version = __version__ = '.'.join(str(x) for x in version_info[:3])
