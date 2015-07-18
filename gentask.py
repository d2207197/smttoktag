#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tools
import luigi
import tables as tb
from functools import wraps
import shlex
from pathlib import Path
from subprocess import call


def simpletask(handler):
    @wraps(handler)
    def gentask(task_name, input_task, output_file, *args,
                input_target_key=None, **kwargs):
        output_file = str(output_file)

        class task(luigi.Task):
            def requires(self):
                return input_task

            def output(self):
                return luigi.LocalTarget(output_file)

            def run(self):
                input_file_target = (
                    self.input()[input_target_key]
                    if input_target_key else self.input()
                )

                with input_file_target.open('r') as inf, self.output().open(
                    'w') as outf:
                    handler(inf, outf, *args, **kwargs)

        task.__name__ = task_name
        return task

    return gentask


import contextlib


@simpletask
def transformat_tab2lines(inf, outf):
    last_len_cols = None
    for line in inf:
        cols = line.strip().split('\t')
        if last_len_cols:
            assert len(cols) == last_len_cols

        last_len_cols = len(cols)
        with contextlib.redirect_stdout(outf):
            print(*cols, sep='\n', end='\n\n')


@simpletask
def transformat_line2slash(inf, outf):
    for zh, tag, _ in tools.group_n_lines(inf, n=3):
        zh, tag = zh.strip().split(), tag.strip().split()
        print(*('{}/{}'.format(z, t) for z, t in zip(zh, tag)), file=outf)


@simpletask
def slice_lines_grouped_by_n(inf, outf, *, n, s):
    for lines in tools.group_n_lines(inf, n=n):
        if type(s) == slice:
            outf.write(''.join(lines[s]))
        elif type(s) == int:
            outf.write(lines[s])
        else:
            raise AssertionError


@simpletask
def remove_slashtag(inf, outf):
    for line in inf:
        words = line.strip().split()
        words = (word.rsplit('/')[0] for word in words)
        print(*words, file=outf)


def parallel_lines_merge(task_name, input_task1, input_task2, output_path):
    class task(luigi.Task):
        def requires(self):
            return {'in1': input_task1, 'in2': input_task2}

        def output(self):
            return luigi.LocalTarget(str(output_path))

        def run(self):
            with self.input()['in1'].open(
                'r') as in1f, self.input()['in2'].open('r') as in2f:
                with self.output().open('w') as outf:
                    for line1, line2 in zip(in1f, in2f):
                        outf.write(line1)
                        outf.write(line2)
                        outf.write('\n')

    task.__name__ = task_name
    return task


def truecase(task_name, input_task, train_task, output_path):
    class train(luigi.Task):
        def requires(self):
            return train_task

        def output(self):
            return luigi.LocalTarget(self.input().fn + '.truecase_model')

        def run(self):
            call(['train-truecaser.perl', '--model', self.output().fn,
                  '--corpus', self.input().fn])

    class truecase(luigi.Task):
        def requires(self):
            return {'corpus': input_task, 'model': train()}

        def output(self):
            return luigi.LocalTarget(str(output_path))

        def run(self):
            with self.input()['corpus'].open('r') as inf, self.output().open(
                'w') as outf:
                call(['truecase.perl', '--model', self.input()['model'].fn],
                     stdin=inf,
                     stdout=outf)

    truecase.__name__ = task_name
    return truecase


@simpletask
def untok(inf, outf, *, sep=' '):
    for line in inf:
        line = line.replace(sep, '')
        outf.write(line)


@simpletask
def word_tokenize(inf, outf):
    import nltk
    for line in inf:
        print(*nltk.word_tokenize(line), file=outf)


@simpletask
def unidecode(inf, outf):
    from unidecode import unidecode
    for line in inf:
        decoded_line = unidecode(line.strip().replace(' ', ' '))
        print(decoded_line, file=outf)


def localtarget_task(path):
    path = str(path)

    class task(luigi.ExternalTask):
        def output(self):
            return luigi.LocalTarget(path)

    task.__name__ = path  #path.replace('/', '_').replace(' ', '_')
    return task


class tab_split_file(luigi.Task):
    inputf = luigi.Parameter()
    outputf1 = luigi.Parameter()
    outputf2 = luigi.Parameter()

    def requires(self):
        return localtarget_task(self.inputf)()

    def output(self):
        return {
            'out1': luigi.LocalTarget(self.outputf1),
            'out2': luigi.LocalTarget(self.outputf2)
        }

    def run(self):
        with self.input().open('r') as inf:
            with self.output()['out1'].open(
                'w') as outf1, self.output()['out2'].open('w') as outf2:
                for line in inf:
                    left, right = line.strip().split('\t', 1)
                    print(left, file=outf1)
                    print(right, file=outf2)


def word_diff(task_name, input_task1, input_task2, output_file):
    output_file = str(output_file)

    class task(luigi.Task):
        def requires(self):
            return (input_task1, input_task2)

        def output(self):
            return luigi.LocalTarget(output_file)

        def run(self):
            cmd = shlex.split('dwdiff {} {}'.format(self.input()[0].fn,
                                                    self.input()[1].fn))
            with self.output().open('w') as outf:
                call(cmd, stdout=outf)

    task.__name__ = task_name
    return task

# cat sbc4 - test.wdiff | gsed - nr
# '/\[.*\}/{s#[^[]*\[-([^[]*?)+\}[^[]*#\1\n#g; p}' | gsed - r '/^\s*$/d' |
# tr ' ' '\n' > sbc4 - test.wdiff.errors


@simpletask
def word_diff_errors(inf, outf):
    import re
    for line in inf:
        errors = re.findall(r'\[-.*?-\] \{\+.*?\+\}', line)
        if errors:
            print(*errors, sep='\n', end='\n', file=outf)


@simpletask
def word_diff_src_error_words(inf, outf):
    import re
    for line in inf:
        match = re.match(r'\[-(.*)-\]', line)
        words = match.group(1).split()
        print(*words, sep='\n', file=outf)


def lm(task_name, input_task, lm_file, blm_file, *, input_target_key=None):
    lm_file, blm_file = str(lm_file), str(blm_file)

    class task(luigi.Task):
        kenlm_bin_path = luigi.Parameter(default='kenlm/bin')
        order = luigi.IntParameter(default=6)

        def requires(self):
            return input_task

        def output(self):
            return {
                'lm': luigi.LocalTarget(lm_file),
                'blm': luigi.LocalTarget(blm_file)
            }

        def run(self):
            lmplz = str(Path(self.kenlm_bin_path) / Path('lmplz'))
            build_binary = str(Path(self.kenlm_bin_path) /
                               Path('build_binary'))

            order = str(self.order)

            input_file_target = self.input()[
                input_target_key
            ] if input_target_key else self.input()

            with input_file_target.open('r') as inf, self.output()['lm'].open(
                'w') as outf:
                call(
                    [lmplz, '-o', order, '--discount_fallback', '0.2',
                     '--vocab_estimate', '105'],
                    stdin=inf,
                    stdout=outf)

            call([build_binary, self.output()['lm'].fn,
                  self.output()['blm'].fn])

    task.__name__ = task_name
    return task


from math import log


def phrasetable(task_name, input_task, output_h5_file, *,
                input_target_key=None):
    '''
    'ch ch ch\n
    tag tag tag\n
    \n
    ch ch ch\n
    tag tag tag\n
    \n
    ' -> output_h5_file.h5
    '''
    output_h5_file = str(output_h5_file)

    class PTable(tb.IsDescription):
        zh = tb.StringCol(200)
        zh_seg = tb.StringCol(205)
        tag = tb.StringCol(100)
        count = tb.Int32Col()
        pr = tb.Float64Col()

    class task(luigi.Task):
        def requires(self):
            return input_task

        def output(self):
            return luigi.LocalTarget(output_h5_file)

        @staticmethod
        def ngrams(words, l):
            for ngram in zip(*(words[i:] for i in range(l))):
                yield ' '.join(ngram)

        @staticmethod
        def ngram_pairs(words1, words2):
            for l in range(1, 5 + 1):
                words1_ngrams = task.ngrams(words1, l)
                words2_ngrams = task.ngrams(words2, l)
                yield from zip(words1_ngrams, words2_ngrams)

        @staticmethod
        def ngram_pairs_from_lines(lines):
            for i, (zh, tag, _) in enumerate(tools.group_n_lines(lines,
                                                                 n=3), 1):
                zh, tag = zh.split(), tag.split()
                if i % 100000 == 0:
                    print('{:,}'.format(i))  # show progress
                    # return
                yield from task.ngram_pairs(zh, tag)

        def run(self):
            from collections import Counter
            translate_count = Counter()
            print('Counting ngrams...')
            with self.input().open(
                'r') as inf:  # , self.output().open('w') as outf:
                translate_count.update(task.ngram_pairs_from_lines(inf))

            # print(translate_count.most_common(5))
            print('Building numpy array...')
            print('Calculating translation prob')
            counts_sum = sum(translate_count.values())

            print('Writing translation prob to `{}`'.format(self.output().fn))

            with tb.open_file(self.output().fn,
                              mode='w',
                              title='Phrase Table') as h5file:

                filters = tb.Filters(complevel=9, complib='blosc')
                # group = h5file.create_group("/", 'ptable', 'Phrase Table')

                table = h5file.create_table(
                    '/', 'phrasetable',
                    description=PTable,
                    title='Phrase Table',
                    filters=filters,
                    expectedrows=21626879,  # chunkshape=(21626,)
                )
                print(h5file)
                phrase_data = table.row
                for (zh, tag), count in translate_count.items():

                    data = zh.replace(' ', '').encode('utf8')
                    if len(data) > table.coldtypes['zh'].itemsize:
                        print('zh', len(data))
                        raise AssertionError
                    phrase_data['zh'] = data

                    data = zh.encode('utf8')
                    if len(data) > table.coldtypes['zh_seg'].itemsize:
                        print('zh_seg', len(data))
                        raise AssertionError
                    phrase_data['zh_seg'] = zh.encode('utf8')

                    data = tag.encode('utf8')
                    if len(data) > table.coldtypes['tag'].itemsize:
                        print('tag:', len(data))
                        raise AssertionError
                    phrase_data['tag'] = tag.encode('utf8')

                    phrase_data['count'] = count
                    phrase_data['pr'] = log(count / counts_sum)
                    phrase_data.append()
                    # translate_model[zhs.replace(' ', '')] = (zhs, tags, count / counts_sum)
                table.flush()
                # table.cols.zh.create_index(optlevel=6, kind='medium', filters=filters)
                table.cols.zh.create_csindex(filters=filters)

    task.__name__ = task_name
    return task


def zhtoktag(task_name, input_task, output_file, *, tm, lm):
    output_file = str(output_file)

    class task(luigi.Task):
        parallel_params = luigi.Parameter(default='')
        parallel_blocksize = luigi.Parameter(default='2k')

        def requires(self):
            return {'input': input_task, 'tm': tm, 'lm': lm, }

        def output(self):
            return luigi.LocalTarget(output_file)

        def run(self):
            from subprocess import call
            from pathlib import Path
            import os.path
            import inspect

            import shlex

            tm_path = shlex.quote(self.input()['tm'].fn)
            lm_path = shlex.quote(self.input()['lm']['blm'].fn)

            home = os.path.expanduser('~')
            dir_name = Path(inspect.stack()[-1][1]).absolute().parent
            current_dir_from_home = dir_name.relative_to(home)

            print(current_dir_from_home)

            toktagger_cmd = 'cd {cdir}; source {venv}; ./toktagger.py -t {tm}  -l {lm} -f /'.format(
                cdir='~/' + shlex.quote(str(current_dir_from_home)),
                venv=shlex.quote('venv/bin/activate'),
                tm=tm_path,
                lm=lm_path)
            print(toktagger_cmd)
            parallel_cmd = 'parallel {params} -k --block-size {blocksize} --pipe {cmd}'.format(
                params=self.parallel_params,
                blocksize=self.parallel_blocksize,
                cmd=shlex.quote(toktagger_cmd))

            cmd = shlex.split(parallel_cmd)
            print('running... ', parallel_cmd)

            with self.input()['input'].open(
                'r') as in_file, self.output().open('w') as out_file:
                retcode = call(cmd, stdin=in_file, stdout=out_file)
            assert retcode == 0

    task.__name__ = task_name
    return task
