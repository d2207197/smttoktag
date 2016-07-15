#!/usr/bin/env python
# -*- coding: utf-8 -*-
import luigi
from subprocess import call, Popen, PIPE
from gentask import simpletask
import contextlib
from pathlib import Path
import tools

import time

from collections import namedtuple

PhraseInfo = namedtuple('PhraseInfo', ['ch', 'aligns', 'scores'])

import difflib

import copy

import warnings


@simpletask
def geniatagger(inf, outf):
    geniatagger_cmd = ['parallel', '--pipe', '-k', '--block-size', '2m',
                       'geniatagger', '-nt']
    call(geniatagger_cmd, stdin=inf, stdout=outf)


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
        chunk[0] = chunk[0].replace('B-', 'I-')
        chunk[-1] = chunk[-1].replace('I-', 'H-')
        new_chunk_tags.extend(chunk)
    return new_chunk_tags


@simpletask
def genia_line_IIH(inf, outf):
    lines = tools.line_stripper(inf)
    for sublines in tools.blank_line_splitter(lines):
        sublines = [line.split('\t') for line in sublines]
        word, lemma, tag, chunk, _ = zip(*sublines)
        chunk = chunk_BII2IIH(chunk)
        with contextlib.redirect_stdout(outf):
            print(*word, end='\t')
            print(*lemma, end='\t')
            print(*tag, end='\t')
            print(*chunk)


@simpletask
def all_patterns(inf, outf):
    import os
    working_directory = Path('Collocation_syntax/')
    mapper = './mapper.py'
    parallel_cmd = ['parallel', '--pipe', mapper, ]
    cwd = Path.cwd()
    os.chdir(working_directory.as_posix())
    call(parallel_cmd, stdin=inf, stdout=outf)
    os.chdir(cwd.as_posix())


def filtered_patterns(task_name, input_task, output_file):
    class task(luigi.Task):
        def requires(self):
            return all_patterns(task_name + '_patterns', input_task,
                                output_file.with_suffix('.txt'))()

        def output(self):
            return luigi.LocalTarget(str(output_file))

        def run(self):
            import os

            output_folder = Path(self.output().fn).absolute()

            reducer = 'Collocation_syntax/reducer.py'

            # lmr_cmd = ['lmr', '1m', '32', mapper, reducer,

            # output_folder.as_posix()]
            with self.output().open('w') as outf:
                sort_cmd = ['sort', '-k1,1', '-t\t', '-s', self.input().fn]
                sort_popen = Popen(sort_cmd, stdout=PIPE)
                reducer_cmd = [reducer]
                reducer_popen = Popen(reducer_cmd,
                                      stdin=sort_popen.stdout,
                                      stdout=outf)

    task.__name__ = task_name
    return task


class pattern_json_pretty_jqscript(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget('pattern.pretty.jq')


def patterns_pretty(task_name, input_task, output_file):
    class task(luigi.Task):
        def requires(self):
            return {
                'patterns_json': input_task,
                'jq script': pattern_json_pretty_jqscript()
            }

        def output(self):
            return luigi.LocalTarget(str(output_file))

        def run(self):
            # jq_input_files = [fpath.as_posix() for fpath in Path(
            # self.input()['patterns_json'].fn).glob('reducer-*')]
            jq_cmd = ['jq', '-s', '-f', self.input()['jq script'].fn]
            print(jq_cmd)
            with self.input()['patterns_json'].open(
                'r') as inf, self.output().open(
                    'w') as outf:
                call(jq_cmd, stdin=inf, stdout=outf)

    task.__name__ = task_name
    return task


def filtered_patterns_from_sentences(task_prefix, input_task):
    gt_task = geniatagger(task_prefix + '_gt', input_task, Path(
        input_task.output().fn).with_suffix('.gt.txt'))()
    hiih_task = genia_line_IIH(task_prefix + '_iih', gt_task, Path(
        gt_task.output().fn).with_suffix('.hiih.txt'))()
    filtered_patterns_task = filtered_patterns(
        task_prefix + '_pattern_allline', hiih_task,
        Path(hiih_task.output().fn).with_suffix('.patterns.json'))()

    pretty_patterns = patterns_pretty(
        task_prefix + '_pretty', filtered_patterns_task, Path(
            filtered_patterns_task.output().fn).with_suffix('.pretty.json'))

    # patterns_allline_task = patterns_allline(
    # task_name + '_pattern_allline', hiih_task,
    # Path(hiih_task.output().fn).with_suffix('.patterns.txt'))

    return pretty_patterns
