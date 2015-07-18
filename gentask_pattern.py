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


def ngrams(words, length):
    return zip(*[words[i:] for i in range(0, length)])


class MosesPhraseTable(dict):
    def __init__(self, phrasetable_file):
        for line in phrasetable_file:
            en, ch, scores, aligns, cnt = line.strip().split(' ||| ')
            en, ch = en.strip(), ch.strip()

            inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w = map(
                float, scores.strip().split())

            aligns = (map(int, align.split('-'))
                      for align in aligns.strip().split())
            aligns_ddict = defaultdict(list)
            for en_pos, ch_pos in aligns:
                aligns_ddict[en_pos].append(ch_pos)
            aligns = dict(aligns_ddict)
            bigrams = ngrams(en.split(), 2)

            for bigram in bigrams:
                self.setdefault(bigram, defaultdict(list))

                en = ' ' + en.strip() + ' '

                self[bigram][en].append(PhraseInfo(
                    ch=ch.strip().split(),
                    aligns=aligns,
                    scores={
                        'inv phrase prob': inv_phrase_prob,
                        'inv lex weight': inv_lex_w,
                        'dir phrase prob': dir_phrase_prob,
                        'dir lex weight': dir_lex_w
                    }))

    def __call__(self, sub_phrase):
        first_bigram = tuple(sub_phrase.split()[:2])

        for phrase in self.get(first_bigram, []):
            if ' ' + sub_phrase + ' ' not in phrase:
                continue
            phraseinfos = self[first_bigram][phrase]
            phrase_words = phrase.split()
            sub_phrase_words = sub_phrase.split()
            sm = difflib.SequenceMatcher(None, phrase_words, sub_phrase_words)
            m = max(sm.get_matching_blocks(), key=lambda x: x.size)
            phrase_start = m.a
            phrase_end = m.a + len(sub_phrase_words)

            # print(phrase_words)
            for phraseinfo in phraseinfos:
                aligns = {
                    en_pos - phrase_start: ch_pos
                    for en_pos, ch_pos in phraseinfo.aligns.items()
                    if phrase_start <= en_pos < phrase_end
                }
                new_phraseinfo = PhraseInfo(phraseinfo.ch, aligns,
                                            phraseinfo.scores)
                yield new_phraseinfo


class spg(luigi.Task):
    IMPORTANT_TAGS = frozenset(
        ['V', 'ADJP', 'sth1', 'sth2', 'sth', 'adjp', 'advp', 'inf', 'wh',
         'doing', 'adj', 'ADJ', 'ADJER', 'ADJERP', 'ADJEST', 'ADJP', 'adv',
         'advp', 'do', 'doing', 'done', 'N', 'one\'s', 'oneself', 'prep',
         '-thing', 'v-link'])

    def requires(self):
        return {
            'en patterns': filtered_patterns(),
            'phrase table': phrasetable()
        }

    def output(self):
        return luigi.LocalTarget(str(target_dir / 'spg.json'))

    @staticmethod
    def phraseinfo_to_ch_pattern(phraseinfo, en_tags):
        ch_tags = []
        for en_pos, tag in en_tags.items():
            if (tag in spg.IMPORTANT_TAGS) and (
                en_pos not in phraseinfo.aligns):
                warnings.warn(
                    'important word not aligned: {}:{} not in {}'.format(
                        en_pos, tag, phraseinfo))
                return None
            if en_pos in phraseinfo.aligns:
                ch_tags.append((phraseinfo.aligns[en_pos], tag))

        ch_pattern = []
        important_pos = set()
        for ch_poss, tag in ch_tags:
            if any(ch_pos in important_pos for ch_pos in ch_poss):
                return None

            if tag in spg.IMPORTANT_TAGS:
                important_pos.update(ch_poss)

            if tag.isupper():
                for ch_pos in ch_poss:
                    ch_pattern.append(
                        (ch_pos, '{}:{}'.format(phraseinfo.ch[ch_pos], tag)))

            elif tag in spg.IMPORTANT_TAGS:
                for ch_pos in ch_poss[0:1]:
                    ch_pattern.append((ch_pos, tag))

            else:
                for ch_pos in ch_poss:
                    ch_pattern.append(
                        (ch_pos, '{}:{}'.format(phraseinfo.ch[ch_pos], tag)))

        ch_pattern = ' '.join(map(itemgetter(1), sorted(ch_pattern,
                                                        key=itemgetter(0))))
        return ch_pattern

    @staticmethod
    def en_instance_find_ch_patterns(instance, en_tags, phrasetable):

        # if instance not in phrasetable:  # phrasetable 中沒有 instance
        #     warnings.warn('instance not in phrase-table: {}'.format(instance))
        #     return []

        phraseinfos = phrasetable(instance)
        # print(phraseinfos)
        ch_patterns = (
            spg.phraseinfo_to_ch_pattern(phraseinfo, en_tags)
            for phraseinfo in phraseinfos
        )

        ch_patterns = [ch_pattern for ch_pattern in ch_patterns if ch_pattern]

        return ch_patterns

    @staticmethod
    def en_tags_rename(en_tags):
        tags_cnt = Counter(en_tags.values())
        tags_gt_1_cnt = {tag: cnt for tag, cnt in tags_cnt.items() if cnt > 1}
        for en_pos, tag in sorted(en_tags.items(),
                                  key=itemgetter(0),
                                  reverse=True):
            if tag in tags_gt_1_cnt:
                new_tag = tag + str(tags_gt_1_cnt[tag])
                tags_gt_1_cnt[tag] -= 1
                en_tags[en_pos] = new_tag
        return en_tags

    @staticmethod
    def en_pattern_rename(en_pattern):
        en_pattern = en_pattern.split()
        tags_cnt = Counter(en_pattern)
        tags_gt_1_cnt = {tag: cnt for tag, cnt in tags_cnt.items() if cnt > 1}
        new_en_pattern = []
        for tag in reversed(en_pattern):
            if tag in tags_gt_1_cnt:
                new_tag = tag + str(tags_gt_1_cnt[tag])
                tags_gt_1_cnt[tag] -= 1
                tag = new_tag

            new_en_pattern.append(tag)
        return ' '.join(reversed(new_en_pattern))

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
            # pattern = {
            #    "keyword": "accept:V",
            #    "keyword_count": 108,
            #    "pattern": "accept sth of sth",
            #    "pattern_count": 3,
            #    "instances": [
            #      {
            #        "instance": "accept any form of criticism",
            #        "tag": {
            #          "0": "V",
            #          "2": "sth",
            #          "3": "of",
            #          "4": "sth"
            #        }
            #      },
            #      {
            #        "instance": "accept the primacy of NATO",
            #        "tag": {
            #          "0": "V",
            #          "2": "sth",
            #          "3": "of",
            #          "4": "sth"
            #        }
            #      },
            #      {
            #        "instance": "accept their share of the blame",
            #        "tag": {
            #          "0": "V",
            #          "2": "sth",
            #          "3": "of",
            #          "5": "sth"
            #        }
            #      }
            #    ]
            #  },
            ch_patterns = []

            pattern['pattern'] = spg.en_pattern_rename(pattern['pattern'])

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
                en_tags = spg.en_tags_rename(en_tags)
                ch_pattern = spg.en_instance_find_ch_patterns(instance,
                                                              en_tags,
                                                              phrasetable)

                ch_patterns.extend(ch_pattern)

            ch_patterns_cnter = Counter(ch_patterns)
            pattern[
                'ch_patterns'
            ] = [{"pattern": ch_pattern,
                  "cnt": cnt}
                 for ch_pattern, cnt in ch_patterns_cnter.most_common()]

        with self.output().open('w') as outf:
            json.dump(patterns, outf)
