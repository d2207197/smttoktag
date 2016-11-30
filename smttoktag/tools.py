#!/usr/bin/env python
# -*- coding: utf-8 -*-

from functools import singledispatch, update_wrapper, wraps

# def ngrams(words, l):
#     for ngram in zip(*(words[i:] for i in range(l))):
#         yield ' '.join(ngram)


def ngrams(words, length):
    return zip(*[words[i:] for i in range(0, length)])


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


import re
CHNUM = '〇一二兩三四五六七八九十百千萬億兆'
NUM = '0123456789'
NUMBERS_RE = re.compile(r'[{}]+|[{}]+'.format(CHNUM, NUM))

import string
LATIN_LETTERS_RE = re.compile(
    r'\{\{[^ ]+?\}\}|' +
    r'[-/.﹒~—─=*&_’]*(?:[{letters}]+[-/.﹒~—─=*&_’]*)+'.format(
        letters=string.ascii_letters))

def strQ2B(ustring):
    "全形拉丁字母、數字、符號轉半形"
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)

        if inside_code == 0x3000:
            inside_code = 0x0020
        # elif 0xff01 <= inside_code <= 0xff0f:
        # pass
        else:
            inside_code -= 0xfee0
        if inside_code < 0x0020 or inside_code > 0x7e:
            rstring += uchar
        else:
            rstring += chr(inside_code)
    return rstring

def is_number(x):
    try:
        float(x)
    except ValueError:
        return False
    else:
        return True



def restore_tag(tagged_s, tag, orig_strs):
    def iter_orig_strs(m):
        iter_orig_strs.i += 1
        return orig_strs[iter_orig_strs.i]
    iter_orig_strs.i = -1
    return re.sub(tag, iter_orig_strs, tagged_s)
    

def number2tag(s):
    PLACE_HOLDER = '{{CD}}'
    PLACE_HOLDER_LEN = len(PLACE_HOLDER)
    if is_number(s):
        return '{{CD}}'
    matches = list(NUMBERS_RE.finditer(s))
    matched_number_strs = [m.group() for m in matches]
    matched_spans = [m.span() for m in matches]
    
    unmatched_substrs = []
    last_end = 0
    for start, end in matched_spans:
        unmatched_substrs.append(s[last_end:start])
        last_end = end
    unmatched_substrs.append(s[last_end:])
    
    return '{{CD}}'.join(unmatched_substrs), matched_number_strs


def fw2tag(s):
    s = re.sub(LATIN_LETTERS_RE, '{{FW}}', s)
    return s

def zhsent_preprocess(s):

    s = strQ2B(s)
    # zh_chars = ' '.join(nltk.word_tokenize(zh_chars))
    s = number2tag(s)
    s = '{{CD}}'.join(fw2tag(subs) for subs in s.split('{{CD}}'))
    return s


ZHTOKEN_WITH_SPECIAL = re.compile(r'\{\{[^ ]+?\}\}|[^\s]', flags=re.UNICODE)


def zh_and_special_tokenize(s):
    return tuple(re.findall(ZHTOKEN_WITH_SPECIAL, s))


def zhsent_tokenize(words):
    sent_end_word = frozenset(';!?:。,')
    sent = []
    for word in words:
        sent.append(word)
        if word in sent_end_word:
            yield tuple(sent)
            sent = []
    if sent:
        yield tuple(sent)
