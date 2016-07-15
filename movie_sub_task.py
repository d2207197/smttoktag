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
from itertools import chain
from functools import reduce
import gentask_pattern
import gentask_spg

ch = gentask.localtarget_task('tgt_data/moviesub/ch.txt')
en = gentask.localtarget_task('tgt_data/moviesub/en.txt')

target_dir = Path('tgt_data/moviesub')

# ench = gentask.transformat_tab2lines('line_sep_ench', orig_ench(),
# target_dir / 'ench.txt')
# en = gentask.slice_lines_grouped_by_n('en', ench(), target_dir / 'en.txt',
#                                       n=3,
#                                       s=0)
# en_unidecode = gentask.unidecode('en_unidecode', en(),
#                                  target_dir / 'en.unidecode.txt')
# en_retok = gentask.word_tokenize('en_retok', en_unidecode(),
#                                  target_dir / 'en.retok.txt')
# en_truecase = gentask.truecase('moviesub_en_truecase', en_retok(), en_retok(),
#                                target_dir / 'en.truecase.txt')

# en_genia = gentask.geniatagger('moviesub_en_genia', en_truecase(),
#                                target_dir / 'en.genia.txt')

# en_genia_line_iih = gentask.genia_line_IIH(
#     'en_genia_line_iih', en_genia(), target_dir / 'en.genia.hiih.txt'
# )  # horizontal and IIH

# en_patterns = gentask.patterns('en_patterns', en_genia_line_iih(),
#                                target_dir / 'en.patterns.json.d')

# en_patterns_allline = gentask.pattern_allline(
#     'en_patterns_allline', en_genia_line_iih(), target_dir / 'en.patterns.d')

# en_patterns_pretty = gentask.patterns_pretty(
# 'en_patterns_pretty', en_patterns(), target_dir / 'en.patterns.json')

# patterns_allline_task = gentask_pattern.pipeline_allline_task(
# 'moviesub_en_patterns_allline', en_truecase())

filtered_patterns = gentask_pattern.filtered_patterns_from_sentences(
    'moviesub_en_filtered_patterns', en())

# ch = gentask.slice_lines_grouped_by_n('ch', ench(), target_dir / 'ch.txt',
# n=3,
# s=1)
ch_untok = gentask.untok('ch_untok', ch(), target_dir / 'ch.untok.txt')
ch_toktag = gentask.zhtoktag('ch_toktag', ch_untok(),
                             target_dir / 'ch.toktag.txt',
                             tm=sbc4_tok_tag_tm(),
                             lm=sbc4_tag_lm())

ch_tok = gentask.remove_slashtag('ch_tok', ch_toktag(),
                                 target_dir / 'ch.tok.txt')

en_chtok = gentask.parallel_lines_merge('en_chtok', en(), ch_tok(),
                                        target_dir / 'en_chtok.txt')

# giza_task = gentask_giza.giza(inputf=str(target_dir / 'en_chtok.txt'),
#                               outputd=str(target_dir / 'giza/'))

unpack_singleline_patterns = gentask.localtarget_task(
    target_dir / 'en.gt.hiih.patterns.pretty.unpack_singleline.json')

phrasetable = gentask.localtarget_task(target_dir / 'phrase-table.10000.gz')

h5_phrasetable = gentask_spg.h5_phrasetable('h5_phrasetable', phrasetable,
                                            target_dir / 'phrase-table.h5',
                                            reverse=True)

print(target_dir)
spg = gentask_spg.spg('spg', unpack_singleline_patterns, h5_phrasetable,
                      target_dir / 'spg.json')
filtered_spg = gentask_spg.filter_spg('filter_spg', spg(),
                                      target_dir / 'spg.filtered.json')
spg_with_sentence = gentask_spg.spg_find_sentence(
    'spg_find_sentence', filtered_spg(), en(), ch(),
    target_dir / 'spg.filtered.with_sentences.json')

spg_sampled = gentask.shuf('spg_shuf', spg_with_sentence(),
                           target_dir / 'spg.filtered.with_sentences.100.json',
                           n=100)

spg_flat = gentask_spg.spg_flatten('spg_flatten', spg_sampled(), target_dir /
                                   'spg.filtered.with_sentences.100.flat.json')

spg_txt = gentask_spg.spg_txt('spg_txt', spg_flat(), target_dir /
                              'spg.filtered.with_sentences.100.flat.txt')

if __name__ == '__main__':
    luigi.interface.setup_interface_logging()
    print(luigi.configuration.get_config())
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch)
    # w.add(en_patterns_pretty())
    # w.add(patterns_allline_task)
    # w.add(filtered_patterns())
    # w.add(ch_toktag(parallel_params='--slf .'))
    # w.add(ch_tok())
    # w.add(spg())
    # w.add(filtered_spg())
    # w.add(spg_with_sentence())
    # w.add(spg_sampled())
    # w.add(spg_flat())
    w.add(spg_txt())
    w.run()

    # w.add(giza_task)
    # w.run()
