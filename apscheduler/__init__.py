# These will be removed in APScheduler 4.0.
parsed_version = __import__('pkg_resources').get_distribution('APScheduler').parsed_version
version_info = tuple(int(x) if x.isdigit() else x for x in parsed_version.public.split('.'))
version = parsed_version.base_version
release = __version__ = parsed_version.public
del parsed_version
