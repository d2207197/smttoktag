#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import luigi
import subprocess
from pathlib import Path
import tables as tb
from toktagger import ZhTokTagger, KenLM, PyTablesTM
import concurrent.futures
import tools


class DrEye_NPVP(luigi.Task):

    def requires(self):
        return DrEye_Phrases()

    def output(self):
        return {
            'np': luigi.LocalTarget('data/dreye/dreye.np.txt'),
            'pure_np': luigi.LocalTarget('data/dreye/dreye.pure_np.txt'),
            'vp': luigi.LocalTarget('data/dreye/dreye.vp.txt')
        }

    def run(self):
        from geniatagger import GeniaTaggerClient
        gtagger = GeniaTaggerClient()
        with self.input().open('r') as input_file:
            with self.output()['np'].open('w') as np_out, self.output()['vp'].open('w') as vp_out, self.output()['pure_np'].open('w') as pure_np_out:
                for en, ch in tools.group_n_lines(input_file, n=2):

                    en, ch = en.strip(), ch.strip()
                    en_tag_info = gtagger.parse(en)
                    if 'B-VP' == en_tag_info[0][3]:
                        outfile = vp_out
                    elif 'B-VP' not in (wdata[3] for wdata in en_tag_info):
                        outfile = pure_np_out
                    else:
                        outfile = np_out

                    print(en, file=outfile)
                    print(ch, file=outfile)
                    print(*('/'.join(wdata) for wdata in en_tag_info), file=outfile)
                    print(file=outfile)


class DrEye_Phrases(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/dreye/dreye_phrases.txt')


class DrEye_Sents(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/dreye/dreye_sents.txt')


import goslate

gtranslate = goslate.Goslate(retry_times=30, timeout=60).translate


def translate_score(en, ch):
    en_from_ch = gtranslate(ch, 'en')
    en = set(en.lower().split())
    # print(en_from_ch)
    en_from_ch = set(en_from_ch.lower().split())
    score = len(en & en_from_ch) / len(en | en_from_ch)
    return score * len(en)


class LTN_Parallel_Sent_Tok(luigi.Task):

    def requires(self):
        return LTN_News()

    def output(self):
        return luigi.LocalTarget('data/ltn_news.sent_tok.txt')

    def run(self):
        from nltk.tokenize import sent_tokenize
        from nltk.tokenize import RegexpTokenizer
        ch_sent_tokenize = RegexpTokenizer('(?:[^。「」！？]*(「[^」]*」)?[^。「」！？]*)+[。！？；]?').tokenize
        import sys

        with self.input().open('r') as input_file, self.output().open('w') as output_file:
            for en, ch in tools.group_n_lines(input_file, n=2):
                en, ch = en.strip(), ch.strip()
                ens = sent_tokenize(en)
                chs = [sub_ch for sub_ch in ch_sent_tokenize(ch) if sub_ch != '']

                score = 0
                if len(ens) != len(chs):
                    print('Unmatched sentences length:', ens, chs, file=sys.stderr)
                    continue

                score = sum(translate_score(en, ch)
                            for en, ch in zip(ens, chs)) / len(en.split())

                for en, ch in zip(ens, chs):
                    print(score, en, ch, sep='\t', file=output_file)


class LTN_News(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/ltn_news.txt')


class SBC4TokTag(luigi.Task):

    def requires(self):
        return SBC4Zh()

    def output(self):
        return luigi.LocalTarget('data/SBC4.zh.toktag')

    def run(self):
        from subprocess import call
        import os
        import shlex
        cmd = shlex.split(
            '''parallel -k --block-size 2k  --pipe 'source "{}/venv/bin/activate"; ./toktagger.py -t data/zh2toktag.ptable.h5 /phrasetable -l data/zh.pos.tag.blm -f /'
   '''.format(os.getcwd()))
        # cmd = shlex.split('echo hello world')
        with self.output().open('w') as output_file:
            # print('OUTPUT FILE', output_file)
            retcode = call(cmd, stdin=self.input().open('r'), stdout=output_file)
        assert retcode == 0


class NPTokTag(luigi.Task):

    def requires(self):
        return OxfordNP_ch()

    def output(self):
        return luigi.LocalTarget('data/oxford.np.ch.toktag')

    def run(self):
        from subprocess import call
        import os
        import shlex
        cmd = shlex.split(
            '''parallel -k --block-size 2k  --pipe 'source "{}/venv/bin/activate"; ./toktagger.py -t data/zh2toktag.ptable.h5 /phrasetable -l data/zh.pos.tag.blm -f /'
   '''.format(os.getcwd()))
        # cmd = shlex.split('echo hello world')
        with self.output().open('w') as output_file:
            # print('OUTPUT FILE', output_file)
            retcode = call(cmd, stdin=self.input().open('r'), stdout=output_file)
        assert retcode == 0
        assert retcode == 0


class OxfordNP_ch(luigi.Task):

    def requires(self):
        return OxfordNP_ench()

    def output(self):
        return luigi.LocalTarget('data/oxford.np.ch.txt')

    def run(self):
        with self.input().open('r') as input_file, self.output().open('w') as output_file:
            for line in input_file:
                en, ch = line.strip().split('\t')
                ch = ch.strip('。')
                print(ch, file=output_file)


class OxfordNP_ench(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/oxford.np.ench.txt')


class PTable(tb.IsDescription):
    zh = tb.StringCol(200)
    zh_seg = tb.StringCol(205)
    tag = tb.StringCol(100)
    count = tb.Int32Col()
    pr = tb.Float64Col()


class PhraseTable(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/zh2toktag.ptable.h5')

    def requires(self):
        return SBC4()

    @staticmethod
    def ngrams(words, l):
        for ngram in zip(*(words[i:] for i in range(l))):
            yield ' '.join(ngram)

    @staticmethod
    def ngram_pairs(words1, words2):
        for l in range(1, 5 + 1):
            words1_ngrams = PhraseTable.ngrams(words1, l)
            words2_ngrams = PhraseTable.ngrams(words2, l)
            yield from zip(words1_ngrams, words2_ngrams)

    @staticmethod
    def ngram_pairs_from_lines(lines):
        for i, line in enumerate(lines, 1):
            zhs, tags = line.strip().rsplit('|||', 1)
            zhs, tags = zhs.split('\t'), tags.split('\t')
            if i % 100000 == 0:
                print('{:,}'.format(i))
                # return
            yield from PhraseTable.ngram_pairs(zhs, tags)

    def run(self):
        from collections import Counter
        translate_count = Counter()
        print('Counting ngrams...')
        with self.input().open('r') as inf:  # , self.output().open('w') as outf:
            translate_count.update(PhraseTable.ngram_pairs_from_lines(inf))

        # print(translate_count.most_common(5))
        print('Building numpy array...')
        print('Calculating translation prob')
        counts_sum = sum(translate_count.values())

        print('Writing translation prob to `{}`'.format(self.output().fn))

        with tb.open_file(self.output().fn, mode='w', title='Phrase Table') as h5file:

            filters = tb.Filters(complevel=9, complib='blosc')
            # group = h5file.create_group("/", 'ptable', 'Phrase Table')

            table = h5file.create_table(
                '/', 'phrasetable',
                description=PTable,
                title='Phrase Table',
                filters=filters,
                expectedrows=21626879,
                chunkshape=(21626,)
            )
            print(h5file)
            phrase_data = table.row
            for (zh, tag), count in translate_count.items():
                # t.cols.zh.dtype.itemsize
                # t.coldtypes['zh'].itemsize
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
                phrase_data['pr'] = count / counts_sum
                phrase_data.append()
                # translate_model[zhs.replace(' ', '')] = (zhs, tags, count / counts_sum)
            table.flush()
            # table.cols.zh.create_index(optlevel=6, kind='medium', filters=filters)
            table.cols.zh.create_csindex(filters=filters)


class ZhPosTagBLM(luigi.Task):
    kenlm_bin_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget('data/zh.pos.tag.blm')

    def requires(self):
        return ZhPosTagLM(kenlm_bin_path=self.kenlm_bin_path)

    def run(self):
        build_binary = Path(self.kenlm_bin_path) / Path('build_binary')
        subprocess.call([str(build_binary), self.input().fn, self.output().fn])


class ZhPosTagLM(luigi.Task):
    kenlm_bin_path = luigi.Parameter()
    # date_interval = luigi.DateIntervalParameter()

    def output(self):
        return luigi.LocalTarget("data/zh.pos.tag.lm")

    def requires(self):
        return ZhPosData()

    def run(self):
        lmplz = Path(self.kenlm_bin_path) / Path('lmplz')
        with self.output().open('w') as output_file:
            p = subprocess.Popen(
                [str(lmplz), '-o', '5'], stdin=self.input().open('r'), stdout=output_file)
            p.wait()


class ZhPosData(luigi.Task):

    def requires(self):
        return SBC4()

    def output(self):
        return luigi.LocalTarget("data/zh.pos.tag")

    def run(self):

        with self.input().open('r') as inf, self.output().open('w') as outf:
            for line in inf:
                zhs, tags = line.strip().rsplit('|||', 1)
                tags = tags.split('\t')

                print(*tags, file=outf, sep=' ')


class SBC4Zh(luigi.Task):

    def requires(self):
        return SBC4()

    def output(self):
        return luigi.LocalTarget('data/SBC4.zh')

    def run(self):

        with self.input().open('r') as inf, self.output().open('w') as outf:
            for line in inf:
                zh, tag = line.strip().rsplit('|||', 1)
                zh = zh.replace('\t', '')

                print(zh, file=outf)


class TestZhData(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/testzh.txt')


class SBC4(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/SBC4')

if __name__ == "__main__":
    luigi.run(local_scheduler=True)
