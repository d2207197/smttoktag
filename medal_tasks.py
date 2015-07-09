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
if __name__ == '__main__':
    luigi.interface.setup_interface_logging()
    print(luigi.configuration.get_config())
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch)
    w.add(en_chtok())
    w.run()

    w.add(giza_task)
    w.run()
