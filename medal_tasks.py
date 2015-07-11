#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi

import gentask
import gentask_giza
from pathlib import Path
import sys
from sbc4_tm_lm_tasks import sbc4_tok_tag_tm, sbc4_tag_lm
from collections import Counter, defaultdict
from operator import itemgetter
from functools import reduce
orig_ench = gentask.localtarget_task('src_data/medal.ench.txt')

target_dir = Path('tgt_data/medal')
ench = gentask.transformat_tab2lines('line_sep_ench', orig_ench(),
                                     target_dir / 'ench.txt')
en = gentask.slice_lines_grouped_by_n('en', ench(), target_dir / 'en.txt',
                                      n=3,
                                      s=0)
en_unidecode = gentask.unidecode('en_unidecode', en(),
                                 target_dir / 'en.unidecode.txt')
en_retok = gentask.word_tokenize('en_retok', en_unidecode(),
                                 target_dir / 'en.retok.txt')
en_truecase = gentask.truecase('medal_en_truecase', en_retok(), en_retok(),
                               target_dir / 'en.truecase.txt')
en_genia = gentask.geniatagger('medal_en_genia', en_truecase(),
                               target_dir / 'en.genia.txt')

en_genia_line_iih = gentask.genia_line_IIH(
    'en_genia_line_iih', en_genia(), target_dir / 'en.genia.hiih.txt'
)  # horizontal and IIH

en_patterns = gentask.patterns('en_patterns', en_genia_line_iih(),
                               target_dir / 'en.patterns.json.d')

en_patterns_pretty = gentask.patterns_pretty(
    'en_patterns_pretty', en_patterns(), target_dir / 'en.patterns.json')

ch = gentask.slice_lines_grouped_by_n('ch', ench(), target_dir / 'ch.txt',
                                      n=3,
                                      s=1)
ch_toktag = gentask.zhtoktag('ch_toktag', ch(), target_dir / 'ch.toktag.txt',
                             tm=sbc4_tok_tag_tm(),
                             lm=sbc4_tag_lm())

ch_tok = gentask.remove_slashtag('ch_tok', ch_toktag(),
                                 target_dir / 'ch.tok.txt')

en_chtok = gentask.parallel_lines_merge('en_chtok', en_truecase(), ch_tok(),
                                        target_dir / 'en_chtok.txt')

giza_task = gentask_giza.giza(inputf=str(target_dir / 'en_chtok.txt'),
                              outputd=str(target_dir / 'giza/'))


class phrasetable(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(str(target_dir / 'moses-train' / 'model' /
                                     'phrase-table.gz'))


import time

from collections import namedtuple

PhraseInfo = namedtuple('PhraseInfo', ['ch', 'aligns', 'scores'])


class MosesPhraseTable(dict):
    def __init__(self, phrasetable_file):
        for line in phrasetable_file:
            en, ch, scores, aligns, cnt = line.strip().split(' ||| ')
            # inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w
            inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w = map(
                float, scores.strip().split())
            aligns = (map(int, align.split('-'))
                      for align in aligns.strip().split())
            aligns_dict = defaultdict(list)
            for en_pos, ch_pos in aligns:
                aligns_dict[en_pos].append(ch_pos)

            self[en.strip()] = PhraseInfo(
                ch=ch.strip().split(),
                aligns=aligns_dict,
                scores={
                    'inv phrase prob': inv_phrase_prob,
                    'inv lex weight': inv_lex_w,
                    'dir phrase prob': dir_phrase_prob,
                    'dir lex weight': dir_lex_w
                })
            # print(self[en.strip()])
            # time.sleep(0.5)

            # def __getitem__(self, en):
            #     return self.ptable[en]

            # def __call___(self, en):
            #     return self.ptable[en]

            # def __contains__(self, key):
            #     return key in self.ptable


import warnings


class spg(luigi.Task):
    def requires(self):
        return {
            'en patterns': en_patterns_pretty(),
            'phrase table': phrasetable()
        }

    def output(self):
        return luigi.LocalTarget(str(target_dir / 'spg.json'))

    def run(self):
        import gzip
        with gzip.open(self.input()['phrase table'].fn,
                       mode='rt',
                       encoding='utf8') as ptablef:
            phrasetable = MosesPhraseTable(ptablef)
        import json
        with self.input()['en patterns'].open('r') as patternsf:
            patterns = json.load(patternsf)

        for pattern in patterns:
            ch_patterns = []
            for instance_info in pattern['instances']:

                # instance_info = {
                #    "instance": "writing an apology",
                #    "tag": {
                #        "0": "V",
                #        "2": "sth"
                #    }
                # }

                instance = instance_info['instance']
                en_tags = {int(k): v for k, v in instance_info['tag'].items()}

                if instance not in phrasetable:
                    warnings.warn(
                        'instance not in phrase-table: {}'.format(instance))
                    continue

                phrase_info = phrasetable[instance]

                if 0 not in phrase_info.aligns:
                    warnings.warn(
                        'no anchor verb: {}'.format(phrasetable[instance]))
                    continue

                ch_tags = [
                    (phrase_info.aligns[en_pos], tag)
                    for en_pos, tag in en_tags.items()
                    if en_pos in phrase_info.aligns
                ]

                ch_pattern = []
                important_tags = frozenset(['V', 'sth', 'adjp', 'advp', 'inf',
                                            'wh', 'doing'])
                important_pos = set()
                for ch_poss, tag in sorted(ch_tags, key=itemgetter(0)):
                    if any(ch_pos in important_pos for ch_pos in ch_poss):
                        break

                    if tag in important_tags:
                        important_pos.update(ch_poss)

                    if tag == 'V':
                        for ch_pos in ch_poss:
                            ch_pattern.append(
                                (ch_pos,
                                 '{}:{}'.format(phrase_info.ch[ch_pos], tag)))
                    elif tag in important_tags:
                        for ch_pos in ch_poss:
                            ch_pattern.append((ch_pos, tag))

                    else:
                        for ch_pos in ch_poss:
                            ch_pattern.append(
                                (ch_pos,
                                 '{}:{}'.format(phrase_info.ch[ch_pos], tag)))

                else:
                    ch_pattern = list(map(itemgetter(1),
                                          sorted(ch_pattern,
                                                 key=itemgetter(0))))

                    #  [ 'X', 'X', 'A', 'B', 'B', 'B', 'X' ] ->  ['X', 'A', 'B', 'X']  # merge continuous tag
                    ch_pattern = reduce(lambda x, z: x + [z] if x[-1] != z else
                                        x, ch_pattern[1:], ch_pattern[:1])
                    ch_patterns.append(
                        ' '.join(ch_pattern)
                    )  #+ '\n' + repr(instance) + '\n' +
                    # repr(phrase_info) + '\n' + repr(en_tags))

            ch_patterns_cnter = Counter(ch_patterns)
            pattern[
                'ch_patterns'
            ] = [{"pattern": ch_pattern,
                  "cnt": cnt}
                 for ch_pattern, cnt in ch_patterns_cnter.most_common()]

        with self.output().open('w') as outf:
            json.dump(patterns, outf)


if __name__ == '__main__':
    luigi.interface.setup_interface_logging()
    print(luigi.configuration.get_config())
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch)
    # w.add(en_patterns_pretty())
    w.add(spg())

    w.run()

    # w.add(giza_task)
    # w.run()
