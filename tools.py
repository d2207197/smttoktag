#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import singledispatch, update_wrapper, wraps


def methdispatch(func):
    dispatcher = singledispatch(func)

    @wraps(func)
    def wrapper(*args, **kw):
        return dispatcher.dispatch(args[1].__class__)(*args, **kw)
    wrapper.register = dispatcher.register
    return wrapper


def listify(func):
    "Decorator to convert generator output to a list"
    @wraps(func)
    def listify(*args, **kw):
        return list(func(*args, **kw))
    return listify


def trace(func):
    "Decorator for logging input and output of function"
    @wraps(func)
    def logify(*args, **kw):
        print(args, kw, end=' ->>>\t', file=sys.stderr)
        output = func(*args, **kw)
        print(output, file=sys.stderr)
        return output
    return logify
