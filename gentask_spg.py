#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
from subprocess import call, Popen, PIPE, check_call
from gentask import simpletask
import contextlib
from pathlib import Path
import tools
from copy import deepcopy
import tables as tb
import gzip
import json
import operator
from functools import reduce

import time

from collections import namedtuple, defaultdict, Counter

from operator import itemgetter

PhraseInfo = namedtuple('PhraseInfo', ['ch', 'aligns', 'scores', 'ch_phrase',
                                       'en_phrase'])
import difflib

import warnings

ChPattern = namedtuple('ChPattern', ['ch_pattern', 'phrase_prob', 'lex_prob',
                                     'prob', 'en_phrase', 'ch_phrase'])

PTScores = namedtuple('PTScores', ['inv_phrase_prob', 'inv_lex_w',
                                   'dir_phrase_prob', 'dir_lex_w'])


class MosesPhraseTable:
    def __init__(self, h5_file):
        self.h5_file = h5_file
        self.h5 = tb.open_file(self.h5_file)
        self.pttables = self.h5.get_node('/phrasetable')

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.h5.close()

    @staticmethod
    def convert2pytables(phrasetable_path, lexe2f_path, lexf2e_path, h5_path,
                         reverse=False):
        class PTable(tb.IsDescription):
            bigram = tb.StringCol(30)
            en = tb.StringCol(200)
            ch = tb.StringCol(200)
            aligns = tb.StringCol(100)
            scores = tb.Float64Col(shape=4)

        lexe2f = []
        with open(lexe2f_path) as lexe2f_f:
            for line in lexe2f_f:
                # print(line)
                en, ch_prob = line.strip().split(' ', 1)
                ch, prob = ch_prob.rsplit(' ', 1)
                lexe2f.append((en, ch, prob))

        lexf2e = []
        with open(lexf2e_path) as lexf2e_f:
            for line in lexf2e_f:
                ch, en, prob = line.strip().split(' ', 1)
                lexf2e.append((ch, en, prob))

        if reverse:
            lexe2f, lexf2e = lexf2e, lexe2f

        with tb.open_file(h5_path, mode='w', title='PhraseTable') as h5file, \
             gzip.open(phrasetable_path, 'rt') as ptfile:
            filters = tb.Filters(complevel=9, complib='blosc')
            h5file.create_array('/', 'lexe2f', lexe2f, 'lex en to ch prob')
            h5file.create_array('/', 'lexf2e', lexf2e, 'lex ch to en prob')

            table = h5file.create_table(
                '/', 'phrasetable',
                description=PTable,
                title='Phrase Table',
                filters=filters,
                # expectedrows=21626879,  # chunkshape=(21626,)
            )
            print(h5file)
            table_row = table.row
            for line in ptfile:
                en, ch, scores, aligns, cnt = line.strip().split(' ||| ')
                if reverse: en, ch = ch, en

                en, ch = en.strip(), ch.strip()

                inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w, _ = map(
                    float, scores.strip().split())
                if reverse:
                    inv_phrase_prob, inv_lex_w, dir_phrase_prob, dir_lex_w = dir_phrase_prob, dir_lex_w, inv_phrase_prob, inv_lex_w

                aligns = (map(int, align.split('-'))
                          for align in aligns.strip().split())
                if reverse:
                    aligns = ((en_pos, ch_pos) for ch_pos, en_pos in aligns)

                aligns_ddict = defaultdict(list)
                for en_pos, ch_pos in aligns:
                    aligns_ddict[en_pos].append(ch_pos)
                aligns = dict(aligns_ddict)

                bigrams = tools.ngrams(en.split(), 2)
                en = en.strip()
                for bigram in bigrams:
                    table_row['bigram'] = ' '.join(bigram).encode('utf8')
                    table_row['en'] = en.encode('utf8')
                    table_row['ch'] = ch.encode('utf8')
                    table_row['aligns'] = json.dumps(aligns).encode('utf8')
                    table_row['scores'] = (inv_phrase_prob, inv_lex_w,
                                           dir_phrase_prob, dir_lex_w)
                    table_row.append()
            table.flush()
            table.cols.bigram.create_csindex(filters=filters)

    def __call__(self, sub_phrase):
        first_bigram = ' '.join(tuple(sub_phrase.split()[:2])).encode('utf8')

        for row in self.pttables.where(
            'bigram == {}'.format(repr(first_bigram))):

            en = row['en'].decode('utf8')
            if ' ' + sub_phrase + ' ' not in ' ' + en + ' ':
                # print('skip', phrase)
                continue
            ch = row['ch'].decode('utf8')

            # phraseinfos = self[first_bigram][phrase]
            en_words = en.split()
            sub_phrase_words = sub_phrase.split()
            sm = difflib.SequenceMatcher(None, en_words, sub_phrase_words)
            m = max(sm.get_matching_blocks(), key=lambda x: x.size)
            en_start = m.a
            en_end = m.a + len(sub_phrase_words)

            aligns = json.loads(row['aligns'].decode('utf8'))
            aligns = {
                int(en_pos) - en_start: ch_pos
                for en_pos, ch_pos in aligns.items()
                if en_start <= int(en_pos) < en_end
            }
            yield PhraseInfo(ch.split(), aligns, PTScores(*row['scores']), ch,
                             en)


def h5_phrasetable(task_name, input_task, output_file, *, reverse=False):
    output_file = str(output_file)

    class task(luigi.Task):
        def requires(self):
            return input_task()

        def output(self):
            return luigi.LocalTarget(output_file)

        def run(self):
            MosesPhraseTable.convert2pytables(self.input.fn, self.output.fn,
                                              reverse)

    task.__name__ = task_name
    return task


def spg(task_name, patterns, h5_phrasetable, output_file):
    class task(luigi.Task):
        IMPORTANT_TAGS = frozenset(
            ['V', 'ADJP', 'sth1', 'sth2', 'sth', 'adjp', 'advp', 'inf', 'wh',
             'doing', 'adj', 'ADJ', 'ADJER', 'ADJERP', 'ADJEST', 'ADJP', 'adv',
             'advp', 'do', 'doing', 'done', 'N', 'one\'s', 'oneself', 'prep',
             '-thing', 'v-link'])

        def requires(self):
            return {
                'en patterns': patterns(),
                'h5 phrase table': h5_phrasetable()
            }

        def output(self):
            return luigi.LocalTarget(str(output_file))

        @staticmethod
        def phraseinfo_to_ch_pattern(phraseinfo, en_tags):
            ch_tags = []
            for en_pos, tag in en_tags.items():
                if (tag in task.IMPORTANT_TAGS) and (
                    en_pos not in phraseinfo.aligns):
                    warnings.warn(
                        'important word not aligned: {}:{} not in {}'.format(
                            en_pos, tag, phraseinfo))
                    return None
                if en_pos in phraseinfo.aligns:
                    ch_tags.append((phraseinfo.aligns[en_pos], tag))

            ch_pattern = []
            important_pos = set()
            if not all(ch_pos < len(phraseinfo.ch)
                       for ch_poss, tag in ch_tags for ch_pos in ch_poss):
                warnings.warn('phraseinfo corrupt: {}'.format(phraseinfo))
                return None

            for ch_poss, tag in ch_tags:
                if any(ch_pos in important_pos for ch_pos in ch_poss):
                    return None

                if tag in task.IMPORTANT_TAGS:
                    important_pos.update(ch_poss)

                if tag.isupper():
                    for ch_pos in ch_poss:
                        ch_pattern.append(
                            (ch_pos, '{}:{}'.format(phraseinfo.ch[ch_pos],
                                                    tag)))

                elif tag in task.IMPORTANT_TAGS:
                    for ch_pos in ch_poss[0:1]:
                        ch_pattern.append((ch_pos, tag))

                else:
                    for ch_pos in ch_poss:
                        try:
                            ch_pattern.append(
                                (ch_pos, '{}:{}'.format(phraseinfo.ch[ch_pos],
                                                        tag)))
                        except Exception as e:
                            print('ERROR ', phraseinfo, en_tags)
                            raise e

            ch_pattern = ' '.join(map(itemgetter(1),
                                      sorted(ch_pattern,
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
                ChPattern(task.phraseinfo_to_ch_pattern(phraseinfo, en_tags),
                          phraseinfo.scores.inv_phrase_prob *
                          phraseinfo.scores.dir_phrase_prob,
                          phraseinfo.scores.inv_lex_w *
                          phraseinfo.scores.dir_lex_w,
                          reduce(operator.mul, phraseinfo.scores),
                          en_phrase=phraseinfo.en_phrase,
                          ch_phrase=phraseinfo.ch_phrase, )
                for phraseinfo in phraseinfos
            )

            ch_patterns = [ch_pattern for ch_pattern in ch_patterns
                           if ch_pattern.ch_pattern]

            return ch_patterns

        @staticmethod
        def en_tags_rename(en_tags):
            tags_cnt = Counter(en_tags.values())
            tags_gt_1_cnt = {
                tag: cnt
                for tag, cnt in tags_cnt.items() if cnt > 1
            }
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
            tags_gt_1_cnt = {
                tag: cnt
                for tag, cnt in tags_cnt.items() if cnt > 1
            }
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
            print('Reading phrasetable...')
            with MosesPhraseTable(self.input()['h5 phrase table'].fn) as phrasetable, \
                 self.input()['en patterns'].open('r') as patternsf, \
                 self.output().open('w') as outf:
                print('Loading English patterns..')
                for pattern_obj_str in patternsf:
                    pattern = json.loads(pattern_obj_str)
                    del pattern_obj_str

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
                    pattern['pattern'
                            ] = task.en_pattern_rename(pattern['pattern'])

                    for instance_info in pattern['instances']:

                        # instance_info = {
                        #    "instance": "writing an apology",
                        #    "tag": {
                        #        "0": "V",
                        #        "2": "sth"
                        #    }
                        # }

                        instance = instance_info['instance']

                        en_tags = {
                            int(k): v
                            for k, v in instance_info['tag'].items()
                        }
                        en_tags = task.en_tags_rename(en_tags)
                        new_ch_patterns = task.en_instance_find_ch_patterns(
                            instance, en_tags, phrasetable)

                        ch_patterns.extend(new_ch_patterns)

                    pattern['ch_patterns'] = [ch_pattern._asdict()
                                              for ch_pattern in ch_patterns]
                    del pattern['instances']
                    # ch_patterns_cnter = Counter(ch_patterns)
                    # pattern[
                    #     'ch_patterns'
                    # ] = [{"pattern": ch_pattern,
                    #       "cnt": cnt}
                    #      for ch_pattern, cnt in ch_patterns_cnter.most_common()]

                    print(json.dumps(pattern), file=outf)

                    del pattern

    task.__name__ = task_name
    return task


@simpletask
def filter_spg(inf, outf):
    jq_command = ['jq', '-c', '-f', 'spg.filter2.jq']
    call(jq_command, stdin=inf, stdout=outf)


def spg_find_sentence(task_name, filtered_spg_task, en_task, ch_task,
                      output_file):
    class task(luigi.Task):
        def requires(self):
            return {'en': en_task, 'ch': ch_task, 'spg': filtered_spg_task}

        def output(self):
            return luigi.LocalTarget(str(output_file))

        def run(self):
            en_ch_sents = []
            sents_index = defaultdict(set)
            print('building sents index...')
            with self.input()['en'].open('r') as enf, \
                 self.input()['ch'].open('r') as chf:
                for sent_no, (en_sent, ch_sent) in enumerate(zip(enf, chf)):
                    en_ch_sents.append((en_sent.strip(), ch_sent.strip()))
                    bigrams = tools.ngrams(en_sent.strip().split(), 2)
                    for bigram in bigrams:
                        sents_index[bigram].add(sent_no)
            print('finding spg sents...')
            with self.input()['spg'].open('r') as spgf, \
                 self.output().open('w') as outputf:

                for spgs_of_en_pattern_json in spgf:
                    spgs_of_en_pattern = json.loads(spgs_of_en_pattern_json)
                    for spg in spgs_of_en_pattern['ch_patterns']:
                        bigrams = tools.ngrams(spg['en_phrase'].split(), 2)

                        sents_nos_sets = (sents_index[bigram]
                                          for bigram in bigrams)
                        sents_nos = reduce(lambda x, y: x & y, sents_nos_sets)
                        sents = (en_ch_sents[sent_no] for sent_no in sents_nos)
                        spg['sents'] = [(en_sent, ch_sent)
                                        for en_sent, ch_sent in sents
                                        if spg['en_phrase'] in en_sent and
                                        spg['ch_phrase'] in ch_sent]

                    print(json.dumps(spgs_of_en_pattern,
                                     ensure_ascii=False,
                                     check_circular=False),
                          file=outputf)

    task.__name__ = task_name
    return task


@simpletask
def spg_flatten(inf, outf):
    jq_command = [
        'jq', '-c',
        '{en_pattern: .pattern} + (.ch_patterns[] | .sents = .sents[0])'
    ]
    check_call(jq_command, stdin=inf, stdout=outf)


@simpletask
def spg_txt(inf, outf):
    jq_cmd = [
        'jq', '-r',
        ' " |||  ||| \(.en_pattern) ||| \(.ch_pattern) ||| \(.prob| tostring)\n"+.sents[0]+.sents[1]'
    ]
    check_call(jq_cmd, stdin=inf, stdout=outf)
