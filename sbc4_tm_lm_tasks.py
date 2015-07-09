#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import tools
import gentask
from pathlib import Path

target_dir = Path('tgt_data/sbc4')


class sbc4_orig(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget('src_data/SBC4.orig.txt')


class sbc4(luigi.Task):
    def requires(self):
        return sbc4_orig()

    def output(self):
        return luigi.LocalTarget(str(target_dir / 'chtag.txt'))

    def run(self):
        with self.input().open('r') as inf, self.output().open('w') as outf:
            for line in inf:
                zh, tag = line.strip().rsplit('|||', 1)
                zh, tag = zh.replace('\t', ' '), tag.replace('\t', ' ')
                print(zh, file=outf)
                print(tag, file=outf)
                print(file=outf)


import contextlib


class sbc4_zhpreprocess(luigi.Task):
    def requires(self):
        return sbc4()

    def output(self):
        return luigi.LocalTarget(str(target_dir / 'chtag.chpreprcs.txt'))

    def run(self):
        with self.input().open('r') as inf, self.output().open('w') as outf:
            for zh, en, _ in tools.group_n_lines(inf, n=3):
                zh, en = zh.strip(), en.strip()
                zh = tools.zhsent_preprocess(zh)
                with contextlib.redirect_stdout(outf):
                    print(zh)
                    print(en)
                    print()


sbc4_zh = gentask.slice_lines_grouped_by_n('sbc4_zh', sbc4(),
                                           target_dir / 'ch.txt',
                                           n=3,
                                           s=0)
sbc4_tag = gentask.slice_lines_grouped_by_n('sbc4_tag', sbc4(),
                                            target_dir / 'tag.txt',
                                            n=3,
                                            s=1)

sbc4_tag_lm = gentask.lm(
    'sbc4_tag_lm', sbc4_tag, target_dir / 'tag.lm', target_dir / 'tag.blm')

sbc4_zhpreprocess_slash = gentask.transformat_line2slash(
    'sbc4_zhpreprocess_slash', sbc4_zhpreprocess(),
    target_dir / 'zhpreprcs.slash.txt')

sbc4_tok_tag_tm = gentask.phrasetable(
    'sbc4_tok_tag_phrasetable', sbc4_zhpreprocess(),
    target_dir / 'toktag.phrasetable.h5')

if __name__ == "__main__":
    luigi.run(local_scheduler=True)
