from typing import Any, Callable

from decorator import decorate


def get_mode(*args, **kw) -> str:
    return kw.get("configuration", args[0])["mode"]


def generic(get_key: Callable) -> Callable:

    registry = {}

    def register(key: Any) -> Callable:  # noqa: WPS430
        """generic.register(key) -> decorator(func)"""

        def decorator(func: Callable) -> Callable:
            registry.update({key: func})
            return func

        return decorator

    def _generic(func, *args, **kw):  # noqa: WPS430
        """Wraps the default generic."""
        method = registry.get(get_key(*args, **kw), func)
        return method(*args, **kw)

    def decorator(default) -> Callable:
        default.register = register
        return decorate(default, _generic)

    return decorator
