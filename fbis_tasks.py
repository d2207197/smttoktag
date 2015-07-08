#!/usr/bin/env python
# -*- coding: utf-8 -*-


import gentask
import luigi
import tools
from subprocess import call
import shlex
import os
from pathlib import Path
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
    if chunk:
        new_chunk_tags.extend(chunk)
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
                sublines = [line.split('\t') for line in sublines]
                word, lemma, tag, chunk, _ = zip(*sublines)
                chunk = chunk_BII2IIH(chunk)
                print(*word, end='\t', file=out_file)
                print(*lemma, end='\t', file=out_file)
                print(*tag, end='\t', file=out_file)
                print(*chunk, file=out_file)


class pattern_json_reformat_jqscript(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('pattern.reformat.jq')


class fbis_en_patterns(luigi.Task):

    def requires(self):
        return fbis_en_genia_line_IIH()

    def output(self):
        return luigi.LocalTarget('data/fbis/fbis.patterns.json.d')

    def run(self):
        working_directory = Path('Collocation_syntax/')
        output_folder = Path(self.output().fn).absolute()

        mapper = './mapper.py'
        reducer = './reducer.py'

        lmr_cmd = ['lmr', '3m', '32', mapper, reducer,  output_folder.as_posix()]
        with self.input().open('r') as input_data:
            cwd = Path.cwd()
            os.chdir(working_directory.as_posix())
            call(lmr_cmd, stdin=input_data)
            os.chdir(cwd.as_posix())


class fbis_en_patterns_reformat(luigi.Task):

    def requires(self):
        return {
            'patterns_json': fbis_en_patterns(),
            'jq script': pattern_json_reformat_jqscript()
        }

    def output(self):
        return luigi.LocalTarget('data/fbis/fbis.patterns.json')

    def run(self):
        jq_input_files = [fpath.as_posix()
                          for fpath in Path(self.input()['patterns_json'].fn).glob('reducer-*')]
        jq_cmd = ['jq', '-s', '-f', self.input()['jq script'].fn] + list(jq_input_files)
        with self.output().open('w') as outf:
            call(jq_cmd, stdout=outf)


class fbis_en_ch_prune_long(luigi.Task):

    def requires(self):
        return {'en': fbis_en(),
                'ch': fbis_ch()}

    def output(self):
        return {'en': luigi.LocalTarget('data/fbis/fbis.en.pruned'),
                'ch': luigi.LocalTarget('data/fbis/fbis.ch.pruned')}

    def run(self):
        with self.input()['en'].open('r') as en_infile, self.input()['ch'].open('r') as ch_infile:
            with self.output()['en'].open('w') as en_outfile, self.output()['ch'].open('w') as ch_outfile:
                for enline, chline in zip(en_infile, ch_infile):
                    if len(chline) > 120:
                        continue
                    en_outfile.write(enline)
                    ch_outfile.write(chline)

fbis_ch_untok = gentask.untok(
    'fbis_ch_untok', fbis_en_ch_prune_long(), 'data/fbis/fbis.ch.untok', input_target_key='ch')

fbis_ch_untok_toktag = gentask.zhtoktag(
    'fbis_ch_untok_toktag', fbis_ch_untok(), 'data/fbis/fbis.ch.untok.tok.txt', tm=sbc4_zh_to_tok_tag_phrasetable(), lm=sbc4_tag_lm())


if __name__ == '__main__':
    luigi.run(local_scheduler=True)
