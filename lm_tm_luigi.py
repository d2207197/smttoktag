#!/usr/bin/env python
# -*- coding: utf-8 -*-

import luigi
import tools
import gentask
from sbc4_tm_lm_tasks import sbc4_zh_to_tok_tag_phrasetable, sbc4_tag_lm


class test_zh_data(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/testzh.txt')


gentask.zhtoktag('test_zh_tok', test_zh_data(), 'tt',
                 tm=sbc4_zh_to_tok_tag_phrasetable(), lm=sbc4_tag_lm())


class oxford_np_ench(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/oxford.np.ench.txt')


class oxford_np_ch(luigi.Task):

    def requires(self):
        return oxford_np_ench()

    def output(self):
        return luigi.LocalTarget('data/oxford.np.ch.txt')

    def run(self):
        with self.input().open('r') as input_file, self.output().open('w') as output_file:
            for line in input_file:
                en, ch = line.strip().split('\t')
                ch = ch.strip('。')
                print(ch, file=output_file)


class dreye_npvp(luigi.Task):

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

# class dreye_purenp_zh(luigi.Task):

#     def requires(self):
#         return DrEye_NPVP()

#     def output(self):
#         return luigi.LocalTarget('data/dreye/dreye.pure_np.zh.txt')

#     def run(self):
#         with self.input()['pure_np'].open('r') as in_file, self.output().open('w') as out_file:
#             for en, ch, entag, _ in tools.group_n_lines(in_file, n=4):
#                 out_file.write(ch)


class dreye_phrases(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/dreye/dreye_phrases.txt')


class dreye_sents(luigi.ExternalTask):

    def output(self):
        return luigi.LocalTarget('data/dreye/dreye_sents.txt')

dreye_purenp_zh = gentask.slice_lines_grouped_by_n(
    'dreye_purenp_zh', dreye_npvp(), 'data/dreye/dreye.pure_np.zh.txt', n=4, s=1, input_target_key='pure_np')

dreye_np_zh = gentask.slice_lines_grouped_by_n(
    'dreye_np_zh', dreye_npvp(), 'data/dreye/dreye.np.zh.txt', n=4, s=1, input_target_key='np')


dreye_vp_zh = gentask.slice_lines_grouped_by_n(
    'dreye_vp_zh', dreye_npvp(), 'data/dreye/dreye.vp.zh.txt', n=4, s=1, input_target_key='vp')


# dreye_purenp_zh_tag = gentask.zhtoktag(
#     'dreye_purenp_zh_tag', dreye_purenp_zh(), 'data/dreye/dreye.pure_np.zh.tag.txt')

# dreye_np_zh_tag = gentask.zhtoktag(
#     'dreye_np_zh_tag', dreye_np_zh(), 'data/dreye/dreye.np.zh.tag.txt')


# dreye_vp_zh_tag = gentask.zhtoktag(
#     'dreye_vp_zh_tag', dreye_vp_zh(), 'data/dreye/dreye.vp.zh.tag.txt')


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


if __name__ == "__main__":
    luigi.run(local_scheduler=True)
