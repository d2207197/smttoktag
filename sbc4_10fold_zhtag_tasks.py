#!/usr/bin/env python
# -*- coding: utf-8 -*-

import gentask
import luigi
from sbc4_tm_lm_tasks import sbc4

sbc4_train = gentask.slice_lines_grouped_by_n(
    'sbc4_train', sbc4(), 'data/sbc4_train/sbc4_train.txt', n=3 * 10, s=slice(0, 3 * 9))

sbc4_test = gentask.slice_lines_grouped_by_n(
    'sbc4_test', sbc4(), 'data/sbc4_test/sbc4_test.txt', n=3 * 10, s=slice(3 * 9, 3 * 10))

sbc4_test_zh = gentask.slice_lines_grouped_by_n(
    'sbc4_test_zh', sbc4_test(), 'data/sbc4_test/sbc4_test.zh.txt', n=3, s=0)


sbc4_test_zh_untok = gentask.untok(
    'sbc4_test_zh_untok', sbc4_test_zh(), 'data/sbc4_test/sbc4_test.zh.untok.txt')


sbc4_train_tag = gentask.slice_lines_grouped_by_n(
    'sbc4_train_tag', sbc4_train(), 'data/sbc4_train/sbc4_train.tag.txt', n=3, s=1)

sbc4_train_tag_lm = gentask.lm(
    'sbc4_train_tag_lm', sbc4_train_tag(), 'data/sbc4_train/sbc4_train.tag.lm', 'data/sbc4_train/sbc4_train.tag.blm')

sbc4_train_zh_to_tok_tag_phrasetable = gentask.phrasetable(
    'sbc4_train_zh_to_tok_tag_phrasetable', sbc4_train(), 'data/sbc4_train/sbc4_train.zh2toktag.phrasetable.h5')


sbc4_train_toktag_sbc4_test = gentask.zhtoktag(
    'sbc4_train_toktag_sbc4_test', sbc4_test_zh_untok(), 'data/sbc4_test/sbc4_test.zh.untok.tok.txt', tm=sbc4_train_zh_to_tok_tag_phrasetable(), lm=sbc4_train_tag_lm())


sbc4_test_slash = gentask.transformat_line2slash(
    'sbc4_test_slash', sbc4_test(), 'data/sbc4_test/sbc4_test.slash.txt')

wdiff_sbc4_test = gentask.word_diff('wdiff_sbc4_test',
                                    sbc4_test_slash(), sbc4_train_toktag_sbc4_test(), 'data/sbc4_test/sbc4-test.wdiff')


wdiff_errors_sbc4_test = gentask.word_diff_errors(
    'wdiff_errors_sbc4_test', wdiff_sbc4_test(), 'data/sbc4_test/sbc4-test.wdiff.errors')

wdiff_src_error_words_sbc4_test = gentask.word_diff_src_error_words(
    'wdiff_src_error_words_sbc4_test', wdiff_errors_sbc4_test(), 'data/sbc4_test/sbc4-test.wdiff.src_error_words')


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
