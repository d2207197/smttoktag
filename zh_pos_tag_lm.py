#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import luigi
import subprocess
from pathlib import Path


class PTable(luigi.Task):

    def output(self):
        return luigi.LocalTarget('data/zh.pos.tag.ptable.shelve')

    def requires(self):
        return InputText()

    @staticmethod
    def ngrams(words, l):
        for ngram in zip(*(words[i:] for i in range(l))):
            yield ' '.join(ngram)

    @staticmethod
    def ngram_pairs(words1, words2):
        for l in range(1, 5 + 1):
            words1_ngrams = PTable.ngrams(words1, l)
            words2_ngrams = PTable.ngrams(words2, l)
            yield from zip(words1_ngrams, words2_ngrams)

    @staticmethod
    def ngram_pairs_from_lines(lines):
        for i, line in enumerate(lines, 1):
            zhs, tags = line.strip().rsplit('|||', 1)
            zhs, tags = zhs.split('\t'), tags.split('\t')
            if i % 1000 == 0:
                print('{:,}'.format(i))
                # return
            yield from PTable.ngram_pairs(zhs, tags)

    def run(self):
        from collections import Counter
        translate_count = Counter()
        print('Counting ngrams...')
        with self.input().open('r') as inf:  # , self.output().open('w') as outf:
            translate_count.update(PTable.ngram_pairs_from_lines(inf))

        print(translate_count.most_common(5))
        print('Building numpy array...')
        # translate_count = list((zhs, tags, count) for (zhs, tags), count in translate_count.items())
        print('Calculating translation prob')
        counts_sum = sum(translate_count.values())

        print('Writing translation prob to `{}`'.format(self.output().fn))
        # import lzma
        # import json
        import shelve
        with shelve.open(self.output().fn, flag='n', protocol=4) as translate_model:
            for (zhs, tags), count in translate_count.items():
                translate_model[zhs.replace(' ', '')] = (zhs, tags, count / counts_sum)


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
        return InputText()

    def output(self):
        return luigi.LocalTarget("data/zh.pos.tag")

    def run(self):

        with self.input().open('r') as inf, self.output().open('w') as outf:
            for line in inf:
                zhs, tags = line.strip().rsplit('|||', 1)
                tags = tags.split('\t')

                print(*tags, file=outf, sep=' ')


class InputText(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/SBC4')

if __name__ == "__main__":
    luigi.run()

# Local Variables:
# flycheck-python-flake8-executable: "flake83"
# End:
