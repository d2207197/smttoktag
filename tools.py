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


def group_n_lines(iterable, *, n):
    "s -> (s0,s1), (s2,s3), (s4, s5), ..."
    a = iter(iterable)
    return zip(*(a for _ in range(n)))


def line_stripper(lines):
    return map(str.strip, lines)


def blank_line_splitter(lines):
    buffer = []
    for line in lines:
        if line == '':
            yield buffer
            buffer = []
        else:
            buffer.append(line)
    if buffer:
        yield buffer
