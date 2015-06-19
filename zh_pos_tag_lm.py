#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import luigi
import subprocess
from pathlib import Path


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


class InputText(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/SBC4')


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

if __name__ == "__main__":
    luigi.run()

# Local Variables:
# flycheck-python-flake8-executable: "flake83"
# End:
