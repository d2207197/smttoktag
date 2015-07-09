#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
from pathlib import Path
from subprocess import call, Popen
import tools
from gentask import localtarget_task, slice_lines_grouped_by_n
import contextlib


class src_tgt_file(luigi.Task):
    inputf = luigi.Parameter()
    outputd = luigi.Parameter()

    def requires(self):
        return localtarget_task(self.inputf)()

    def output(self):
        folder = Path(self.outputd)
        return {
            'src': luigi.LocalTarget(str(folder / 'src.txt')),
            'tgt': luigi.LocalTarget(str(folder / 'tgt.txt'))
        }

    def run(self):
        with self.input().open('r') as inf:
            with self.output()['src'].open(
                'w') as outf1, self.output()['tgt'].open(
                    'w') as outf2:
                for src, tgt, _ in tools.group_n_lines(inf, n=3):
                    outf1.write(src)
                    outf2.write(tgt)


class plain2snt(luigi.Task):
    inputf = luigi.Parameter()
    outputd = luigi.Parameter()

    def requires(self):
        return src_tgt_file(inputf=self.inputf, outputd=self.outputd)

    def output(self):
        # path_prefix = Path(self.outputd)
        return {
            filename: luigi.LocalTarget(str(Path(self.outputd) / filename))
            for filename in ('tgt.vcb', 'src.vcb', 'tgt2src.snt', 'src2tgt.snt')
        }

    def run(self):
        tgt_input = self.input()['tgt'].fn
        src_input = self.input()['src'].fn

        # try:
        # Path(self.outputd).mkdir(parents=True)
        # except FileExistsError:
        # pass
        tgt_vcb = self.output()['tgt.vcb'].fn
        src_vcb = self.output()['src.vcb'].fn
        tgt2src_snt = self.output()['tgt2src.snt'].fn
        src2tgt_snt = self.output()['src2tgt.snt'].fn

        plain2snt_cmd = ['plain2snt', tgt_input, src_input, '-vcb1', tgt_vcb,
                         '-vcb2', src_vcb, '-snt1', tgt2src_snt, '-snt2',
                         src2tgt_snt]
        print('running...', plain2snt_cmd)
        call(plain2snt_cmd)


class mkcls(luigi.Task):
    inputf = luigi.Parameter()
    outputd = luigi.Parameter()

    def requires(self):
        return src_tgt_file(inputf=self.inputf, outputd=self.outputd)

    def output(self):
        return {
            filename: luigi.LocalTarget(str(Path(self.outputd) / filename))
            for filename in ('src.vcb.classes', 'src.vcb.classes.cats',
                             'tgt.vcb.classes', 'tgt.vcb.classes.cats')
        }

    def run(self):
        def mkcls_process(input_key, output_key):
            input = self.input()[input_key].fn
            output = self.output()[output_key].fn
            mkcls_cmd = ['mkcls', '-n10', '-p' + input, '-V' + output]
            print('running... ', mkcls_cmd)
            return Popen(mkcls_cmd)

        p_src = mkcls_process('src', 'src.vcb.classes')
        p_tgt = mkcls_process('tgt', 'tgt.vcb.classes')

        p_src.wait()
        p_tgt.wait()


class cooc(luigi.Task):
    # snt2cooc fbis.en2ch.cooc fbis.en.vcb fbis.ch.vcb fbis.en2ch.snt
    inputf = luigi.Parameter()
    outputd = luigi.Parameter()

    def requires(self):
        return plain2snt(inputf=self.inputf, outputd=self.outputd)

    def output(self):
        return {
            filename: luigi.LocalTarget(str(Path(self.outputd) / filename))
            for filename in ('src2tgt.cooc', 'tgt2src.cooc')
        }

    def run(self):
        cooc_cmd = ['snt2cooc', self.output()['src2tgt.cooc'].fn,
                    self.input()['src.vcb'].fn, self.input()['tgt.vcb'].fn,
                    self.input()['src2tgt.snt'].fn]
        src2tgt_p = Popen(cooc_cmd)
        cooc_cmd = ['snt2cooc', self.output()['tgt2src.cooc'].fn,
                    self.input()['tgt.vcb'].fn, self.input()['src.vcb'].fn,
                    self.input()['tgt2src.snt'].fn]
        tgt2src_p = Popen(cooc_cmd)

        src2tgt_p.wait()
        tgt2src_p.wait()


class gizacfg_template(luigi.ExternalTask):
    def output(self):
        return luigi.LocalTarget('./template.gizacfg')


class gizacfg(luigi.Task):
    inputf = luigi.Parameter()
    outputd = luigi.Parameter()

    def requires(self):

        return {
            'cfg template': gizacfg_template(),
            'vcb snt': plain2snt(inputf=self.inputf,
                                 outputd=self.outputd),
            'cooc': cooc(inputf=self.inputf,
                         outputd=self.outputd),
            'mkcls': mkcls(inputf=self.inputf,
                           outputd=self.outputd),
        }

    def output(self):
        return {
            filename: luigi.LocalTarget(str(Path(self.outputd) / filename))
            for filename in ('src2tgt.gizacfg', 'tgt2src.gizacfg')
        }

    def run(self):
        from string import Template
        cfg_template = Template(self.input()['cfg template'].open('r').read())

        with self.output()['src2tgt.gizacfg'].open('w') as outf:
            outf.write(cfg_template.substitute(
                snt=self.input()['vcb snt']['src2tgt.snt'].fn,
                cooc=self.input()['cooc']['src2tgt.cooc'].fn,
                output_prefix=str(Path(self.outputd) / 'src2tgt'),
                src_vcb=self.input()['vcb snt']['src.vcb'].fn,
                tgt_vcb=self.input()['vcb snt']['tgt.vcb'].fn,
                src_classes=self.input()['mkcls']['src.vcb.classes'].fn,
                tgt_classes=self.input()['mkcls']['tgt.vcb.classes'].fn))

        with self.output()['tgt2src.gizacfg'].open('w') as outf:
            outf.write(cfg_template.substitute(
                snt=self.input()['vcb snt']['tgt2src.snt'].fn,
                cooc=self.input()['cooc']['tgt2src.cooc'].fn,
                output_prefix=str(Path(self.outputd) / 'tgt2src'),
                src_vcb=self.input()['vcb snt']['tgt.vcb'].fn,
                tgt_vcb=self.input()['vcb snt']['src.vcb'].fn,
                src_classes=self.input()['mkcls']['tgt.vcb.classes'].fn,
                tgt_classes=self.input()['mkcls']['src.vcb.classes'].fn))


class giza(luigi.Task):
    inputf = luigi.Parameter()
    outputd = luigi.Parameter()

    def requires(self):
        return gizacfg(inputf=self.inputf, outputd=self.outputd)

    def output(self):
        return {
            filename: luigi.LocalTarget(str(Path(self.outputd) / filename))
            for filename in ('src2tgt.A3.final.part0', 'tgt2src.A3.final.part0')
        }

    def run(self):
        giza_cmd = ['mgiza', self.input()['src2tgt.gizacfg'].fn]
        src2tgt_p = Popen(giza_cmd)
        giza_cmd = ['mgiza', self.input()['tgt2src.gizacfg'].fn]
        tgt2src_p = Popen(giza_cmd)
        src2tgt_p.wait()
        tgt2src_p.wait()


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
