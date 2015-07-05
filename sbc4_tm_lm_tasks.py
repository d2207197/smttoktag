#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import tools
import gentask


class sbc4_orig(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/sbc4/SBC4.orig.txt')


class sbc4(luigi.Task):

    def requires(self):
        return sbc4_orig()

    def output(self):
        return luigi.LocalTarget('data/sbc4/sbc4.txt')

    def run(self):
        with self.input().open('r') as inf, self.output().open('w') as outf:
            for line in inf:
                zh, tag = line.strip().rsplit('|||', 1)
                zh, tag = zh.replace('\t', ' '), tag.replace('\t', ' ')
                print(zh, file=outf)
                print(tag, file=outf)
                print(file=outf)


sbc4_zh = gentask.slice_lines_grouped_by_n('sbc4_zh', sbc4(), 'data/sbc4/sbc4.zh.txt', n=3, s=0)
sbc4_tag = gentask.slice_lines_grouped_by_n('sbc4_tag', sbc4(), 'data/sbc4/sbc4.tag.txt', n=3, s=1)

sbc4_tag_lm = gentask.lm(
    'sbc4_tag_lm', sbc4_tag(), 'data/sbc4/sbc4.tag.lm', 'data/sbc4/sbc4.tag.blm')

sbc4_zh_to_tok_tag_phrasetable = gentask.phrasetable(
    'sbc4_zh_to_tok_tag_phrasetable', sbc4(), 'data/sbc4/sbc4.zh2toktag.phrasetable.h5')


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
