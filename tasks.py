#!/usr/bin/env python
# -*- coding: utf-8 -*-


from invoke import run, task
import zh_word_seg_luigi

import os.path


@task
def buildlm(kenlm_bin_path='~/tools/kenlm/bin'):
    # run("./zh_word_seg.luigi.py ")

    kenlm_bin_path = os.path.expanduser(kenlm_bin_path)

    zh_word_seg_luigi.luigi.interface.setup_interface_logging()
    sch = zh_word_seg_luigi.luigi.scheduler.CentralPlannerScheduler()
    w = zh_word_seg_luigi.luigi.worker.Worker(scheduler=sch)
    w.add(zh_word_seg_luigi.ZhPosTagBLM(kenlm_bin_path))
    w.run()


@task
def buildtm():

    zh_word_seg_luigi.luigi.interface.setup_interface_logging()
    sch = zh_word_seg_luigi.luigi.scheduler.CentralPlannerScheduler()
    w = zh_word_seg_luigi.luigi.worker.Worker(scheduler=sch)
    w.add(zh_word_seg_luigi.PhraseTable())
    w.run()


@task
def cleantm():
    print(zh_word_seg_luigi.PhraseTable().output().fn)
    run('rm {}'.format(zh_word_seg_luigi.PhraseTable().output().fn))

import tables as tb


def querytm(zh):

    with tb.open_file('data/zh.pos.tag.ptable.h5') as h5file:
        t = h5file.root.ptable.ptable
        print([
            (x['zh'].decode('utf8'),
             x['pr'],
             x['count'],
             x['tag'],
             x['zh_seg'].decode('utf8'))
            for x in t.where('zh == {}'.format(repr(zh.encode('utf8'))))])


import timeit


@task
def testtm(zh=''):
    print(timeit.timeit(lambda: querytm(zh), number=5))
