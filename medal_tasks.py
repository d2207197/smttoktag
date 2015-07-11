#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi

import gentask
import gentask_giza
from pathlib import Path
from sbc4_tm_lm_tasks import sbc4_tok_tag_tm, sbc4_tag_lm

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


class MosesPhraseTable(dict):
    def __init__(self, phrasetable_file):
        for line in phrasetable_file:
            en, ch, scores, aligns, cnt = line.strip().split(' ||| ')
            # inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w
            inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w = map(
                float, scores.strip().split())
            aligns = (align.split('-') for align in aligns.strip().split())
            aligns = {int(en_pos): int(ch_pos) for en_pos, ch_pos in aligns}
            self[en.strip()] = {
                'ch': ch.strip().split(),
                'aligns': aligns,
                'scrose': {
                    'inv phrase prob': inv_phrase_prob, 'inv lex weight':
                    inv_lex_w, 'dir phrase prob': dir_phrase_prob,
                    'dir lex weight': dir_lex_w
                }
            }
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
            pt = MosesPhraseTable(ptablef)
        import json
        with self.input()['en patterns'].open('r') as patternsf:
            patterns = json.load(patternsf)

        for pattern in patterns:
            ch_patterns = []
            for instance in pattern['instances']:
                if instance in pt:
                    ch_pattern = {}

                    instance_word_pos = {
                        word: pos
                        for pos, word in enumerate(instance.split())
                    }
                    if 0 not in pt[instance]['aligns']:
                        warnings.warn(
                            'no anchor verb: {}'.format(pt[instance]))
                        continue
                    ch_pos = pt[instance]['aligns'][0]
                    ch_verb = pt[instance]['ch'][ch_pos]
                    ch_pattern[ch_pos] = ch_verb
                    pattern_syms = pattern['pattern'].split()
                    for pattern_sym in pattern_syms[1:]:
                        if pattern_sym == 'sth':
                            pass
                        elif pattern_sym == 'adjp':
                            pass
                        elif pattern_sym == 'advp':
                            pass
                        elif pattern_sym == 'inf':
                            pass
                        elif pattern_sym == 'wh':
                            pass
                        elif pattern_sym == 'doing':
                            pass
                        else:
                            en_pos = instance_word_pos[pattern_sym]

                            if en_pos not in pt[instance]['aligns']:
                                warnings.warn('{} not aligned: {}'.format(
                                    pattern_sym, pt[instance]))
                                break

                            ch_pos = pt[instance]['aligns'][en_pos]

                            ch_word = pt[instance]['ch'][ch_pos]
                            ch_pattern[ch_pos] = ch_word

                    else:
                        ch_patterns.append(
                            [ch_word
                             for pos, ch_word in sorted(ch_pattern.items())])

            pattern['ch_patterns'] = ch_patterns
        with self.output().open('w') as outf:
            json.dump(patterns, outf)


if __name__ == '__main__':
    luigi.interface.setup_interface_logging()
    print(luigi.configuration.get_config())
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch)
    w.add(spg())

    w.run()

    # w.add(giza_task)
    # w.run()
