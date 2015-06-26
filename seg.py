#!/usr/bin/env python
# -*- coding: utf-8 -*-


from collections import namedtuple
from functools import total_ordering


@total_ordering
class SegInfo(namedtuple('SegInfo', ['zh', 'zh_seg', 'tag', 'tag_cat',  'pr'])):

    def __add__(self, other):
        return SegInfo(self.zh + other.zh,
                       self.zh_seg + ' ' + other.zh_seg,
                       self.tag + ' ' + other.tag,
                       self.tag_cat + ' + ' + other.tag_cat,
                       self.pr + other.pr)

    def __eq(self, other):
        return

    def __lt__(self, other):
        return self.pr < other.pr

    def __and__(self, lm_pr):
        return SegInfo(self.zh,
                       self.zh_seg,
                       self.tag,
                       self.tag_cat,
                       self.pr,)


from functools import lru_cache
from math import log
import sys
import tools


class PyTablesTM:

    "Translation Model from PyTables"

    def __init__(self, h5_file_path, h5_path):
        import tables as tb
        h5 = tb.open_file(h5_file_path)
        self.pytables = h5.get_node(h5_path)

    @lru_cache()
    def __getitem__(self, zh):
        return [
            SegInfo(x['zh'].decode('utf8'),
                    x['zh_seg'].decode('utf8'),
                    x['tag'].decode('utf8'),
                    x['tag'].decode('utf8'),
                    log(x['pr']),)

            for x in self.pytables.where('zh == {}'.format(repr(zh.encode('utf8'))))]


class KenLM:

    'KenLM Language Model'

    def __init__(self, blm_path):
        import kenlm
        self.lm = kenlm.LanguageModel(blm_path)

    @tools.methdispatch
    def __getitem__(self, tags):
        return self.lm.score(tags)

    @__getitem__.register(tuple)
    def _(self, tags):
        return self.lm.score(' '.join(tags))


@lru_cache()
@tools.listify
def allpartition(seq):
    yield seq[0:1], seq[1:]
    if len(seq) > 1:
        for part1, part2 in allpartition(seq[1:]):
            yield seq[0:1] + part1, part2


# @lru_cache()
# def allpartition(seq):
#     yield seq[0:1], seq[1:]
#     if len(seq) > 1:
#         for part1, part2 in allpartition(seq[1:]):
#             yield seq[0:1] + part1, part2
from operator import itemgetter


class ZhTokTagger:

    def __init__(self, tm, lm):
        'Chinese sentence tokenizer and Part-Of-Speech tagger'
        self.tm = tm
        self.lm = lm

    @lru_cache()
    def __call__(self, zh_chars):
        tm_out = []
        for part1, part2 in allpartition(zh_chars):
            seginfos1 = self.tm[part1]
            for seginfo1 in seginfos1:
                if part2:
                    tm_out.extend(
                        seginfo1 + seginfo2 for score, lm_pr, seginfo2 in self.__call__(part2))
                else:
                    tm_out.append(seginfo1)

        tm_out = sorted(((self.lm[seginfo.tag] + seginfo.pr, self.lm[seginfo.tag], seginfo)
                         for seginfo in tm_out), key=itemgetter(0))[-5:]
        return tm_out


if __name__ == '__main__':
    import fileinput
    toktagger = ZhTokTagger(
        PyTablesTM('data/zh2toktag.ptable.h5', '/phrasetable'), KenLM('data/zh.pos.tag.blm'))

    for line in fileinput.input():
        zh_chars = line.strip()
        print(*toktagger(zh_chars), sep='\n', end='\n\n')
