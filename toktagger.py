#!/usr/bin/env python
# -*- coding: utf-8 -*-


from collections import namedtuple
from functools import total_ordering, reduce


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
from functools import update_wrapper


class PyTablesTM:

    "Translation Model from PyTables"

    def __init__(self, h5_file_path, h5_path='/phrasetable'):
        '''create an object for querying PyTables translation model'''
        import tables as tb
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            self.h5 = tb.open_file(h5_file_path)
            self.pytables = self.h5.get_node(h5_path)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.h5.close()

    @lru_cache()
    def __getitem__(self, zh):
        return [
            SegInfo(x['zh'].decode('utf8'),
                    x['zh_seg'].decode('utf8'),
                    x['tag'].decode('utf8'),
                    x['tag'].decode('utf8'),
                    x['pr'],)

            for x in self.pytables.where('zh == {}'.format(repr(zh.encode('utf8'))))]

    __call__ = __getitem__
    update_wrapper(__call__, __getitem__)


class KenLM:

    'KenLM Language Model'

    def __init__(self, blm_path):
        import kenlm
        self.lm = kenlm.LanguageModel(blm_path)

    @tools.methdispatch
    @lru_cache()
    def __getitem__(self, tags):
        return self.lm.score(tags, bos=False, eos=False)

    @__getitem__.register(tuple)
    def _(self, tags):
        return self.lm.score(' '.join(tags), bos=False, eos=False)

    __call__ = __getitem__
    update_wrapper(__call__, __getitem__)


@lru_cache()
@tools.listify
def allpartition(seq, *, max_length=3 * 5):
    max_length = min(len(seq), max_length)
    for length in range(max_length, 1 - 1, -1):
        yield seq[0:length], seq[length:]

from operator import itemgetter, attrgetter

from itertools import groupby


class ZhTokTagger:

    '''Chinese sentence tokenizer and Part-Of-Speech tagger

>>> zhttagger = ZhTokTagger( tm = PyTablesTM('path/to/h5file', '/pytable/path'), lm = KenLM('path/to/blm'))
>>> zhtagger('今天出去玩')
("今天出去玩", "今天 出去 玩", "Nd VA VC", "Nd + VA VC", - \
 22.90191810211992, -8.768468856811523, -31.670386958931445)
'''

    def __init__(self, tm, lm):
        self.tm = tm
        self.lm = lm

    def _topN_seginfos(self, seginfos, n):

        def groupby_tag(seginfos):
            tag_getter = attrgetter('tag')
            return (subiter for key, subiter in groupby(sorted(seginfos, key=tag_getter), key=tag_getter))

        def append_lmpr_tmlmpr(seginfo):
            lm_pr = self.lm[seginfo.tag]
            return seginfo, lm_pr, lm_pr + seginfo.pr

        def top1_pr(seginfos):
            return sorted(seginfos)[-1]

        top1_pr_seginfo_of_each_tag = (top1_pr(seginfos_of_same_tag)
                                       for seginfos_of_same_tag in groupby_tag(seginfos))

        topn_with_lmpr_tmlmpr = sorted(
            (append_lmpr_tmlmpr(seginfo) for seginfo in top1_pr_seginfo_of_each_tag), key=itemgetter(-1))[-n:]
        return tuple([seginfo for seginfo, lmpr, tmlmpr in topn_with_lmpr_tmlmpr])

    @lru_cache()
    def _tok_tag(self, zh_chars):
        tm_out = []
        for part1, part2 in allpartition(zh_chars):
            part1_query = ''.join(part1)

            seginfos1 = self.tm[part1_query]
            # print(part1_query, '->', seginfos1)
            if not seginfos1:
                seginfos1 = [SegInfo(part1_query, part1_query, 'Nb', 'Nb', -17 * len(part1))]

            if not part2:
                tm_out.extend(seginfos1)
            else:
                for seginfo1 in seginfos1:
                    tm_out.extend(
                        seginfo1 + seginfo2 for seginfo2 in self._tok_tag(part2))

        return self._topN_seginfos(tm_out, 5)

    def __call__(self, zh_chars):
        '''>>> zhtagger('今天出去玩')
("今天出去玩", "今天 出去 玩", "Nd VA VC", "Nd + VA VC", -22.90191810211992, -8.768468856811523, -31.670386958931445)'''
        zh_chars = zh_chars.strip()
        zh_chars = tools.zhsent_preprocess(zh_chars)
        zh_chars = tools.zh_and_special_tokenize(zh_chars)
        sents = tools.zhsent_tokenize(zh_chars)
        sents_seginfos = [self._tok_tag(sent)[-1] for sent in sents]
        return reduce(lambda a, b: a + b, sents_seginfos)


import argparse


def argparser(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Chinese tokenzier and Part-Of-Speech tagger.')
    parser.add_argument(
        '--translation-model', '-t', metavar='H5_FILE_PATH', help='Pytables Phrase Table', required=True)
    parser.add_argument(
        '--language-model', '-l', metavar='KENLM_BLM_PATH', help='KenLM BLM', required=True)

    parser.add_argument(
        '--format', '-f',  default='/',  help='output format', choices=['verbose', 'tab', '/'])
    parser.add_argument('FILE', nargs='*', help='input file(text in chinese)')

    return parser.parse_args(args)

if __name__ == '__main__':
    import fileinput
    cmd_options = argparser()
    with PyTablesTM(cmd_options.translation_model) as pytables_tm:

        toktagger = ZhTokTagger(
            tm=pytables_tm,
            lm=KenLM(cmd_options.language_model))

        for line in fileinput.input(cmd_options.FILE):
            zh_chars = line.strip()
            tagger_out = toktagger(zh_chars)
            # print(tagger_out)

            if cmd_options.format == 'verbose':
                print(*tagger_out, sep='\t')
            elif cmd_options.format == 'tab':
                print(tagger_out[1], tagger_out[2], sep='\t')
            elif cmd_options.format == '/':
                print(*('{}/{}'.format(zh, tag)
                        for zh, tag in zip(tagger_out[1].split(), tagger_out[2].split())))
