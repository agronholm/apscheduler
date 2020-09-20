"""This module contains several handy functions primarily meant for internal use."""

import re
from datetime import datetime, timedelta
from functools import partial
from inspect import isclass, ismethod, signature
from typing import Any, Tuple


class _Undefined:
    def __bool__(self):
        return False

    def __repr__(self):
        return '<undefined>'


undefined = _Undefined()  #: a unique object that only signifies that no value is defined


_DATE_REGEX = re.compile(
    r'(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})'
    r'(?:[ T](?P<hour>\d{1,2}):(?P<minute>\d{1,2}):(?P<second>\d{1,2})'
    r'(?:\.(?P<microsecond>\d{1,6}))?'
    r'(?P<timezone>Z|[+-]\d\d:\d\d)?)?$')


def datetime_ceil(dateval: datetime):
    """Round the given datetime object upwards."""
    if dateval.microsecond > 0:
        return dateval + timedelta(seconds=1, microseconds=-dateval.microsecond)
    return dateval


def get_callable_name(func):
    """
    Returns the best available display name for the given function/callable.

    :rtype: str

    """
    # the easy case (on Python 3.3+)
    if hasattr(func, '__qualname__'):
        return func.__qualname__

    # class methods, bound and unbound methods
    f_self = getattr(func, '__self__', None) or getattr(func, 'im_self', None)
    if f_self and hasattr(func, '__name__'):
        f_class = f_self if isclass(f_self) else f_self.__class__
    else:
        f_class = getattr(func, 'im_class', None)

    if f_class and hasattr(func, '__name__'):
        return '%s.%s' % (f_class.__name__, func.__name__)

    # class or class instance
    if hasattr(func, '__call__'):
        # class
        if hasattr(func, '__name__'):
            return func.__name__

        # instance of a class with a __call__ method
        return func.__class__.__name__

    raise TypeError('Unable to determine a name for %r -- maybe it is not a callable?' % func)


def obj_to_ref(obj):
    """
    Returns the path to the given callable.

    :rtype: str
    :raises TypeError: if the given object is not callable
    :raises ValueError: if the given object is a :class:`~functools.partial`, lambda or a nested
        function

    """
    if isinstance(obj, partial):
        raise ValueError('Cannot create a reference to a partial()')

    name = get_callable_name(obj)
    if '<lambda>' in name:
        raise ValueError('Cannot create a reference to a lambda')
    if '<locals>' in name:
        raise ValueError('Cannot create a reference to a nested function')

    if ismethod(obj):
        if hasattr(obj, 'im_self') and obj.im_self:
            # bound method
            module = obj.im_self.__module__
        elif hasattr(obj, 'im_class') and obj.im_class:
            # unbound method
            module = obj.im_class.__module__
        else:
            module = obj.__module__
    else:
        module = obj.__module__
    return '%s:%s' % (module, name)


def ref_to_obj(ref):
    """
    Returns the object pointed to by ``ref``.

    :type ref: str

    """
    if not isinstance(ref, str):
        raise TypeError('References must be strings')
    if ':' not in ref:
        raise ValueError('Invalid reference')

    modulename, rest = ref.split(':', 1)
    try:
        obj = __import__(modulename, fromlist=[rest])
    except ImportError:
        raise LookupError('Error resolving reference %s: could not import module' % ref)

    try:
        for name in rest.split('.'):
            obj = getattr(obj, name)
        return obj
    except Exception:
        raise LookupError('Error resolving reference %s: error looking up object' % ref)


def maybe_ref(ref):
    """
    Returns the object that the given reference points to, if it is indeed a reference.
    If it is not a reference, the object is returned as-is.

    """
    if not isinstance(ref, str):
        return ref
    return ref_to_obj(ref)


def check_callable_args(func, args, kwargs):
    """
    Ensures that the given callable can be called with the given arguments.

    :type args: tuple
    :type kwargs: dict

    """
    pos_kwargs_conflicts = []  # parameters that have a match in both args and kwargs
    positional_only_kwargs = []  # positional-only parameters that have a match in kwargs
    unsatisfied_args = []  # parameters in signature that don't have a match in args or kwargs
    unsatisfied_kwargs = []  # keyword-only arguments that don't have a match in kwargs
    unmatched_args = list(args)  # args that didn't match any of the parameters in the signature
    # kwargs that didn't match any of the parameters in the signature
    unmatched_kwargs = list(kwargs)
    # indicates if the signature defines *args and **kwargs respectively
    has_varargs = has_var_kwargs = False

    try:
        sig = signature(func)
    except ValueError:
        # signature() doesn't work against every kind of callable
        return

    for param in sig.parameters.values():
        if param.kind == param.POSITIONAL_OR_KEYWORD:
            if param.name in unmatched_kwargs and unmatched_args:
                pos_kwargs_conflicts.append(param.name)
            elif unmatched_args:
                del unmatched_args[0]
            elif param.name in unmatched_kwargs:
                unmatched_kwargs.remove(param.name)
            elif param.default is param.empty:
                unsatisfied_args.append(param.name)
        elif param.kind == param.POSITIONAL_ONLY:
            if unmatched_args:
                del unmatched_args[0]
            elif param.name in unmatched_kwargs:
                unmatched_kwargs.remove(param.name)
                positional_only_kwargs.append(param.name)
            elif param.default is param.empty:
                unsatisfied_args.append(param.name)
        elif param.kind == param.KEYWORD_ONLY:
            if param.name in unmatched_kwargs:
                unmatched_kwargs.remove(param.name)
            elif param.default is param.empty:
                unsatisfied_kwargs.append(param.name)
        elif param.kind == param.VAR_POSITIONAL:
            has_varargs = True
        elif param.kind == param.VAR_KEYWORD:
            has_var_kwargs = True

    # Make sure there are no conflicts between args and kwargs
    if pos_kwargs_conflicts:
        raise ValueError('The following arguments are supplied in both args and kwargs: %s' %
                         ', '.join(pos_kwargs_conflicts))

    # Check if keyword arguments are being fed to positional-only parameters
    if positional_only_kwargs:
        raise ValueError('The following arguments cannot be given as keyword arguments: %s' %
                         ', '.join(positional_only_kwargs))

    # Check that the number of positional arguments minus the number of matched kwargs matches the
    # argspec
    if unsatisfied_args:
        raise ValueError('The following arguments have not been supplied: %s' %
                         ', '.join(unsatisfied_args))

    # Check that all keyword-only arguments have been supplied
    if unsatisfied_kwargs:
        raise ValueError(
            'The following keyword-only arguments have not been supplied in kwargs: %s' %
            ', '.join(unsatisfied_kwargs))

    # Check that the callable can accept the given number of positional arguments
    if not has_varargs and unmatched_args:
        raise ValueError(
            'The list of positional arguments is longer than the target callable can handle '
            '(allowed: %d, given in args: %d)' % (len(args) - len(unmatched_args), len(args)))

    # Check that the callable can accept the given keyword arguments
    if not has_var_kwargs and unmatched_kwargs:
        raise ValueError(
            'The target callable does not accept the following keyword arguments: %s' %
            ', '.join(unmatched_kwargs))


def marshal_object(obj) -> Tuple[str, Any]:
    return f'{obj.__class__.__module__}:{obj.__class__.__qualname__}', obj.__getstate__()


def unmarshal_object(ref: str, state):
    cls = ref_to_obj(ref)
    instance = cls.__new__(cls)
    instance.__setstate__(state)
    return instance
