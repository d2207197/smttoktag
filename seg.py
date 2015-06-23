#!/usr/bin/env python
# -*- coding: utf-8 -*-


import fileinput


from collections import namedtuple

from functools import total_ordering


@total_ordering
class SegInfo(namedtuple('SegInfo', ['zh', 'zh_seg', 'tag', 'pr'])):

    def __add__(self, other):
        return SegInfo(self.zh + other.zh,
                       self.zh_seg + '+' + other.zh_seg,
                       self.tag + '+' + other.tag,
                       self.pr + other.pr)

    def __eq(self, other):
        return

    def __lt__(self, other):
        return self.pr < other.pr

from functools import lru_cache

from math import log


@lru_cache()
def queryzh(zh):
    # print(zh)
    out = [
        SegInfo(x['zh'].decode('utf8'),
                x['zh_seg'].decode('utf8'),
                x['tag'].decode('utf8'),
                log(x['pr']))
        for x in queryzh.t.where('zh == {}'.format(repr(zh.encode('utf8'))))]
    # print(out)
    if not out:
        print('!!!! no:', zh)
    return out


# def allpartition(seq):
#     for l in range(1, len(seq) + 1):
#         yield seq[0:l], seq[l:]


@lru_cache()
def allpartition(seq):
    out = [(seq[0:1], seq[1:])]
    if len(seq) > 1:
        for part1, part2 in allpartition(seq[1:]):
            out.append((seq[0:1] + part1, part2))
    return out


# @lru_cache()
# def allpartition(seq):
#     yield seq[0:1], seq[1:]
#     if len(seq) > 1:
#         for part1, part2 in allpartition(seq[1:]):
#             yield seq[0:1] + part1, part2


@lru_cache()
def segprob(zh_chars, lvl=1):
    print('lvl:', lvl, 'zh_chars:', zh_chars)
    out = []
    for part1, part2 in allpartition(zh_chars):
        print('lvl:', lvl, 'part1:', part1, 'part2:', part2)
        seginfos1 = queryzh(part1)
        for seginfo1 in seginfos1:
            if part2:
                out.extend(seginfo1 + seginfo2 for seginfo2 in segprob(part2, lvl=lvl + 1))
            else:
                out.append(seginfo1)
    return sorted(out)[-5:]


import tables as tb
if __name__ == '__main__':
    with tb.open_file('data/zh.pos.tag.ptable.h5') as h5:
        queryzh.t = h5.root.ptable.ptable
        for line in fileinput.input():
            zh_chars = line.strip()
            print('outout', *sorted(segprob(zh_chars)), sep='\n')
