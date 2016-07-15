#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import Counter, defaultdict, namedtuple
from itertools import groupby
from operator import attrgetter, itemgetter
import sys
import fileinput
import json
from itertools import islice
import operator
from math import sqrt
from collections import OrderedDict
from heapq import nlargest
""" Linux cmd:
pv 100000citeseerx.sents.tagged.h | lmr 5m 4 'python mapper.py' 'python reducer.py' output
pv citeseerx.sents.tagged.h | lmr 100m 40 'python mapper.py' 'python reducer.py' output
"""


class OrderedDefaultDict(OrderedDict):
    def __init__(self, default_factory=None, *args, **kwargs):
        super(OrderedDefaultDict, self).__init__(*args, **kwargs)
        self.default_factory = default_factory

    def __missing__(self, key):
        if self.default_factory is None:
            raise KeyError(key)
        val = self[key] = self.default_factory()
        return val


class WordPat(Counter):
    def __init__(self, dev_thresh, minCount):
        self.default_factory = Counter  # Collocates
        self.dev_thresh = dev_thresh
        self.minCount = minCount

    def __repr__(self):
        #tail = ', ...' if len(self) > 3 else ''
        items = ', \n    '.join(map(str, iter(self.items())))
        return '{}(freq = {}, avg_freq = {}, dev = {}, \n \n)'.format(
            self.__class__.__name__, self.freq, self.avg_freq, self.dev, items)

    def calc_metrics(self):
        self.freq = sum(self.values())
        len_self = len(self)
        self.avg_freq = self.freq / len_self if len_self else 0.0
        self.dev = sqrt(sum(
            (xfreq - self.avg_freq) ** 2
            for xfreq in list(self.values())) / len_self) if len_self else 0.0

    def gen_goodpat(self):
        if self.freq == 0 or self.dev == 0:
            return
        good_pat = []
        for pat in self:
            if (self[pat] - self.avg_freq) / self.dev > self.dev_thresh:
                good_pat.append((pat, self[pat]))
        for pat, count in sorted(good_pat, key=lambda x: x[1], reverse=True):
            if count > self.minCount:
                yield pat, count

    def most_common(self, n=None):
        if n is None:
            return sorted(self.gen_goodpat(), key=itemgetter(1), reverse=True)
        return nlargest(n, self.gen_goodpat(), key=itemgetter(1))


def line2structed(line):
    wordpos, pat, iocs, example, instance_sym, cnt = line.rstrip().split('\t',
                                                                         5)
    return PatData(wordpos, pat, iocs, example, eval(instance_sym), int(cnt))


PatData = namedtuple('PatData', ['wordpos', 'pat', 'iocs', 'example',
                                 'instance_sym', 'cnt'])

# import pdb


def reducer():
    table = {}
    lines = fileinput.input()
    structed_lines = map(line2structed, lines)
    for wordpos, wordposdatas in groupby(structed_lines,
                                         key=attrgetter('wordpos')):
        patterns = WordPat(1, 5)
        # wordpos_cnt, patterns = 0, WordPat(0, 0)
        pat_dict = defaultdict(list)
        for iocs, grouped_wordposdatas in groupby(
            sorted(wordposdatas,
                   key=lambda x: (x.pat, x.example)),
            key=lambda x: (x.pat, x.example)):
            grouped_wordposdatas = list(grouped_wordposdatas)
            # if len(grouped_wordposdatas) > 2:
            # print('group start')
            wordposdata = grouped_wordposdatas[0]._replace(
                cnt=sum(wpd.cnt for wpd in grouped_wordposdatas))

            # for wordposdata in grouped_wordposdatas:
            patterns[wordposdata.pat] += wordposdata.cnt
            # if len(grouped_wordposdatas) > 2:
            # print(wordposdata)
            pat_dict[wordposdata.pat].append(wordposdata)
        patterns.calc_metrics()
        # if wordpos == 'important:ADJ':
        # pdb.set_trace()
        isgoodpat = False
        for pat, pat_cnt in patterns.most_common():
            if not isgoodpat:
                isgoodpat = True
                # print wordpos, wordpos_cnt
                table[wordpos] = [patterns.freq, patterns.avg_freq,
                                  patterns.dev]
            iocss = Counter()
            # iocss = WordPat(1, 10)
            # iocss = WordPat(0, 0)
            iocs_dict = defaultdict(list)
            for patdata in pat_dict[pat]:
                iocss[patdata.iocs] += patdata.cnt
                if patdata.cnt > 3:  ## 篩選 instance 的次數
                    iocs_dict[patdata.iocs].append(patdata)
            # iocss.calc_metrics()
            example_list = []
            # print '\t', pat, pat_cnt
            # iocss.calc_metrics()
            # print('<IOCSS>', iocss)

            # for iocs, iocs_cnt in iocss.most_common():
            #     best_example_cnter = Counter()
            #     for iocsdata in iocs_dict[iocs]:
            #         best_example_cnter[iocsdata.example] += iocsdata.cnt
            #     best_example, example_cnt = best_example_cnter.most_common(1)[0
            #                                                                   ]
            #     example_list.append([best_example, iocs_cnt, example_cnt])

            for iocs, patdatas in iocs_dict.items():
                for patdata in patdatas:

                    example_list.append([patdata.example, patdata.cnt,
                                         patdata.instance_sym])

            # print '\t\t', best_example, iocs_cnt, example_cnt
            table[wordpos].append([pat, pat_cnt, example_list])
        """
        patInstances = [(pat, list(instances)) for pat, instances in groupby(wordposdatas, key=attrgetter('pat'))]
        patCounts = dict([(pat, len(instances)) for pat, instances in patInstances])
        patInstances = dict(patInstances)
        patterns = WordPat(0, 10)
        # patterns = WordPat(1, 10)
        patterns.update(patCounts)
        patterns.calc_metrics()
        goodPats = sorted([ (x, y) for x, y in patterns.gen_goodpat() ], key=lambda x:x[1], reverse=True)    
        if goodPats:
            table[wordpos] = [ sum([ count for _, count in goodPats]) ]
        else:
            continue        
        for pat, count in goodPats:
            colInstances = [ (col, list(instances)) for col, instances in groupby(patInstances[pat], key=attrgetter('iocs')) ]
            colCounts = dict([ (col, len(instances)) for col, instances in colInstances ])
            colInstances = dict(colInstances)
            patterns = WordPat(0, 10)
            # patterns = WordPat(1, 10)
            patterns.update( colCounts )
            patterns.calc_metrics()
            goodCols = sorted([ (col, count) for col, count in patterns.gen_goodpat() ], key=lambda x:x[1], reverse=True)
            if not goodCols:
                bestngram = max([ (len(list(instances)), ngram) for ngram, instances in groupby(patInstances[pat],
                                                                                                key=attrgetter('ngram')) ])
                table[wordpos] += [ [pat, patCounts[pat], [(bestngram[1], colCounts[col], bestngram[0])]] ]
                continue
            res = []    
            for col, count in goodCols[:5]:
                bestngram = max([ (len(list(instances)), ngram) for ngram, instances in groupby(colInstances[col], key=attrgetter('ngram')) ])
                res += [(bestngram[1], colCounts[col], bestngram[0])]
            table[wordpos] += [ [pat, patCounts[pat], res] ]
        """
    import json
    print(json.dumps(table))
    fileinput.close()


if __name__ == '__main__':
    reducer()
