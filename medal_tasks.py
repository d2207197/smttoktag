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
import operator
import gentask_pattern
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

# en_genia = gentask.geniatagger('medal_en_genia', en_truecase(),
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
# 'medal_en_patterns_allline', en_truecase())

filtered_patterns = gentask_pattern.filtered_patterns_from_sentences(
    'medal_en_filtered_patterns', en_truecase())

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


class phrasetable(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget(str(target_dir / 'moses-train' / 'model' /
                                     'phrase-table.gz'))


import time

from collections import namedtuple

PhraseInfo = namedtuple('PhraseInfo', ['ch', 'aligns', 'scores', 'ch_phrase',
                                       'en_phrase'])

import difflib

import copy


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
                    },
                    en_phrase=en,
                    ch_phrase=ch, ))

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
                                            phraseinfo.scores,
                                            phraseinfo.en_phrase,
                                            phraseinfo.ch_phrase)
                yield new_phraseinfo
                # new_phraseinfo.aligns = aligns
                # print(new_phraseinfo)
                # print(phraseinfo)
                # print(aligns)
                # print()


import warnings

ChPattern = namedtuple('ChPattern', ['ch_pattern', 'phrase_prob', 'lex_prob',
                                     'prob', 'en_phrase', 'ch_phrase'])


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
            ChPattern(spg.phraseinfo_to_ch_pattern(phraseinfo, en_tags),
                      phraseinfo.scores[
                          'inv phrase prob'
                      ] * phraseinfo.scores[
                          'dir phrase prob'
                      ], phraseinfo.scores[
                          'inv lex weight'
                      ] * phraseinfo.scores[
                          'dir lex weight'
                      ], reduce(operator.mul, phraseinfo.scores.values()),
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
        print('Reading phrasetable...')
        with gzip.open(self.input()['phrase table'].fn,
                       mode='rt',
                       encoding='utf8') as ptablef:
            phrasetable = MosesPhraseTable(ptablef)
        print('Loading English patterns..')
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
                new_ch_patterns = spg.en_instance_find_ch_patterns(instance,
                                                                   en_tags,
                                                                   phrasetable)

                ch_patterns.extend(new_ch_patterns)

            pattern['ch_patterns'] = [ch_pattern._asdict()
                                      for ch_pattern in ch_patterns]
            # ch_patterns_cnter = Counter(ch_patterns)
            # pattern[
            #     'ch_patterns'
            # ] = [{"pattern": ch_pattern,
            #       "cnt": cnt}
            #      for ch_pattern, cnt in ch_patterns_cnter.most_common()]

        with self.output().open('w') as outf:
            json.dump(patterns, outf)


if __name__ == '__main__':
    luigi.interface.setup_interface_logging()
    print(luigi.configuration.get_config())
    sch = luigi.scheduler.CentralPlannerScheduler()
    w = luigi.worker.Worker(scheduler=sch)
    # w.add(en_patterns_pretty())
    # w.add(patterns_allline_task)
    # w.add()
    w.add(spg())

    w.run()

    # w.add(giza_task)
    # w.run()
