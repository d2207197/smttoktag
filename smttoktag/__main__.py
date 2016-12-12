import sys
import argparse

from . import PyTablesTM, ZhTokTagger, KenLM, RedisTM

def argparser(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description='Chinese tokenzier and Part-Of-Speech tagger.')
    group = parser.add_mutually_exclusive_group(required=True)

    group.add_argument(
        '--pytables-translation-model', '-t', metavar='H5_FILE_PATH', help='Pytables Phrase Table', dest='tm_h5_path')

    group.add_argument(
        '--redis-translation-model', '-r', metavar='DB_NUM', type=int, help='RedisTM DB Number', dest='redis_db_num')

    parser.add_argument(
        '--language-model', '-l', metavar='KENLM_BLM_PATH', help='KenLM BLM', required=True)

    parser.add_argument(
        '--format', '-f',  default='/',  help='output format', choices=['verbose', 'tab', '/'])
    parser.add_argument('FILE', nargs='*', help='input file(text in chinese)')

    return parser.parse_args(args)

if __name__ == '__main__':
    import fileinput
    cmd_options = argparser()

    if cmd_options.redis_db_num is not None:
        tm = RedisTM(cmd_options.redis_db_num)

    elif cmd_options.tm_h5_path is not None:
        tm = PyTablesTM(cmd_options.tm_h5_path)

    


    toktagger = ZhTokTagger(
        tm=tm,
        lm=KenLM(cmd_options.language_model))

    for line in fileinput.input(cmd_options.FILE):
        zh_chars = line.strip()
        tagger_out = toktagger(zh_chars)
        # print(tagger_out)

        if cmd_options.format == 'verbose':
            print(*tagger_out, sep='\t')
        elif cmd_options.format == 'tab':
            print(tagger_out[1], tagger_out[2], sep='\t')
        elif cmd_options.format == '/':
            print(*('{}/{}'.format(zh, tag)
                    for zh, tag in zip(tagger_out[1].split(), tagger_out[2].split())))
