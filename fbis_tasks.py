#!/usr/bin/env python
# -*- coding: utf-8 -*-


import gentask
import luigi
import tools
from subprocess import call
import shlex
import os
from sbc4_tm_lm_tasks import sbc4_zh_to_tok_tag_phrasetable, sbc4_tag_lm


class fbis_ch(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/fbis/FBIS.ch')


class fbis_en(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/fbis/FBIS.en')


class fbis_en_genia(luigi.Task):
    parallel_params = luigi.Parameter(default='')
    parallel_blocksize = luigi.Parameter(default='2k')

    def requires(self):
        return fbis_en()

    def output(self):
        return luigi.LocalTarget('data/fbis/fbis.en.genia')

    def run(self):
        import shlex
        from subprocess import call
        tagger_cmd = 'geniatagger'

        parallel_cmd = 'parallel {params} -k --block-size {blocksize} --pipe {cmd}'.format(
            params=self.parallel_params,
            blocksize=self.parallel_blocksize,
            cmd=shlex.quote(tagger_cmd))

        cmd = shlex.split(parallel_cmd)
        print('running... ', parallel_cmd)

        with self.input().open('r') as in_file, self.output().open('w') as out_file:
            retcode = call(cmd, stdin=in_file, stdout=out_file)
            assert retcode == 0


def chunk_BII2IIH(chunk_tags):
    # print(tagged_sent)
    # print(list(zip(*tagged_sent)))
    chunk = []
    new_chunk_tags = []
    for chunk_tag in chunk_tags:
        if not chunk_tag.startswith('I-') and chunk:
            chunk[0] = chunk[0].replace('B-', 'I-')
            chunk[-1] = chunk[-1].replace('I-', 'H-')
            new_chunk_tags.extend(chunk)
            chunk = []
        if chunk_tag == 'O':
            new_chunk_tags.append(chunk_tag)
        else:
            chunk.append(chunk_tag)
    return new_chunk_tags


class fbis_en_genia_line_IIH(luigi.Task):

    def requires(self):
        return fbis_en_genia()

    def output(self):
        return luigi.LocalTarget('data/fbis/fbis.en.genia.h')

    def run(self):
        with self.input().open('r') as in_file, self.output().open('w') as out_file:
            lines = tools.line_stripper(in_file)
            for sublines in tools.blank_line_splitter(lines):
                sublines = (line.split('\t') for line in sublines)
                word, lemma, tag, chunk, _ = zip(*sublines)
                chunk = chunk_BII2IIH(chunk)
                print(*word, end='\t', file=out_file)
                print(*lemma, end='\t', file=out_file)
                print(*tag, end='\t', file=out_file)
                print(*chunk, file=out_file)



fbis_ch_untok = gentask.untok('fbis_ch_untok', fbis_ch(), 'data/fbis/fbis.ch.untok')

fbis_ch_untok_toktag = gentask.zhtoktag(
    'fbis_ch_untok_toktag', fbis_ch_untok(), 'data/fbis/fbis.ch.untok.tok.txt', tm=sbc4_zh_to_tok_tag_phrasetable(), lm=sbc4_tag_lm())


if __name__ == '__main__':
    luigi.run(local_scheduler=True)
