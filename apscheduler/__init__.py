version_info = (2, 0, 3, 'post1')
version = release = '.'.join(str(n) for n in version_info[:3])
if len(version_info) > 3:
    release += '.' + version_info[3]
