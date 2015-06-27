#!/usr/bin/env python
# -*- coding: utf-8 -*-

from toktagger import ZhTokTagger, PyTablesTM, KenLM
import concurrent.futures
import fileinput


def main():
    toktagger = ZhTokTagger(
        tm=PyTablesTM('data/zh2toktag.ptable.h5', '/phrasetable'),
        lm=KenLM('data/zh.pos.tag.blm'))

    for zh, zh_seg, tag, *_ in map(toktagger, fileinput.input()):
        zh_seg = zh_seg.split()
        tag = tag.split()
        print(*zh_seg, sep='\t', end='|||')
        print(*tag, sep='\t')
if __name__ == '__main__':
    main()
