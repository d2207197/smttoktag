#!/usr/bin/env python
# -*- coding: utf-8 -*-
import fileinput
import sys
import re
from collections import Counter, namedtuple, defaultdict
from itertools import product, chain
from operator import methodcaller
from copy import deepcopy
import PGpatterns
import PGelements
import Colpatterns
import Colelements
import genPatterns
import json
""" Linux cmd:
pv 100000citeseerx.sents.tagged.h | lmr 5m 4 'python pgp_mapper.py' 'python pgp_reducer.py' pgp_test
pv citeseerx.sents.tagged.h | lmr 100m 40 'python pgp_mapper.py' 'python pgp_reducer.py' pgp_
pv bnc_tagged.h | lmr 25m 10 'python pgp_mapper.py' 'python pgp_reducer.py' bnc_pgp_

pv 100000citeseerx.sents.tagged.h | lmr 5m 4 'python col_mapper.py' 'python col_reducer.py' col_test
pv citeseerx.sents.tagged.h | lmr 100m 40 'python col_mapper.py' 'python col_reducer.py' col_
pv bnc_tagged.h | lmr 25m 10 'python col_mapper.py' 'python col_reducer.py' bnc_col_

pv 100000citeseerx.sents.tagged.h | lmr 5m 4 'python com_mapper.py' 'python com_reducer.py' com_test
pv citeseerx.sents.tagged.h | lmr 100m 40 'python com_mapper.py' 'python com_reducer.py' com_
pv bnc_tagged.h | lmr 25m 10 'python com_mapper.py' 'python com_reducer.py' bnc_com_
"""
chose = ['grammar', 'collocation', 'combine'][0]
aklWords = set(line.rstrip() for line in open('writeaway_word.txt'))


def pat_product(word_elements, pos, elem_i, prelen, aftlen, keywordPos):
    def add_element(elements, prods, prod_lens, isignore=False):
        for k, prod in enumerate(prods):
            if prod_lens[k] != 0:
                prod.append([e for e in elements if e not in keywordPos])
                if not isignore:
                    prod_lens[k] -= 1

    def gen_prods(wordElem, prods, prod_lens, limit):
        # if chose == 'grammar':
        limit_len = 3
        # elif chose == 'collocation':
        # limit_len = 6
        # elif chose == 'combine':
        # limit_len = 5
        for j, elements in enumerate(wordElem):
            # print '\t', elements, prod_lens
            if not any(prod_lens) or j > limit * limit_len or not elements:
                break
            if not elements:
                continue
            elif '_' not in elements:
                add_element(elements, prods, prod_lens, isignore=False)
            else:
                if len(elements) == 1:
                    add_element(elements, prods, prod_lens, isignore=True)
                else:
                    tmp_prods = [
                        prod for k, prod in enumerate(deepcopy(prods))
                        if prod_lens[k] != 0
                    ]
                    tmp_prod_lens = [prod_len
                                     for prod_len in deepcopy(prod_lens)
                                     if prod_len != 0]
                    add_element(['_'], tmp_prods, tmp_prod_lens, isignore=True)
                    nonignore = [e for e in elements
                                 if e not in keywordPos and e != '_']
                    add_element(nonignore, prods, prod_lens, isignore=False)
                    prods.extend(tmp_prods)
                    prod_lens.extend(tmp_prod_lens)
        return True

    if elem_i < prelen or len(word_elements) - elem_i - 1 < aftlen:
        return ()
    prods, prod_lens = [[]], [prelen]
    if gen_prods(reversed(word_elements[:elem_i]), prods, prod_lens, prelen):
        for prod in prods:
            prod.reverse()
            prod.append([pos])
        # print 'perv:', prods
        prod_lens = [aftlen] * len(prod_lens)
        if gen_prods(word_elements[elem_i + 1:], prods, prod_lens, aftlen):
            # print 'aftr:', prods
            return chain(*[product(*prod) for prod in prods])
    return ()


def gen_candiate(word_elements, words, lemmas, keywordpos, pat_poslen):
    # print word_elements
    for elem_i, elements in enumerate(word_elements):
        pos = keywordpos & set(elements)
        if pos and (words[elem_i] in aklWords or lemmas[elem_i] in aklWords):
            # if pos and pos & set(['VERB']) and (words[elem_i] in aklWords or
            # lemmas[elem_i] in aklWords):
            pos = pos.pop()
            # for (pat_len, pos_index), pats in Colpatterns.ColPat_poslen[pos].iteritems():
            for (pat_len, pos_index), pats in pat_poslen[pos].items():
                for cand in pat_product(word_elements, pos, elem_i, pos_index,
                                        pat_len - pos_index - 1, keywordpos):
                    # print pat_len, pos_index
                    # print
                    pat_cands, pat_indexs = [], []
                    for i, w in enumerate(cand):
                        if w != '_':
                            pat_cands.append(w)
                            pat_indexs.append(i)
                    # if len(pat_cands) == pat_len and parsleyPatterns.isPattern(' '.join(pat_cands)):
                    # pat_cands, pat_indexs = remove_prep_of(pat_cands, pat_indexs)
                    if len(pat_cands) == pat_len and ' '.join(
                        pat_cands) in pats:
                        pos_i = pat_indexs[pos_index]
                        for i, index in enumerate(pat_indexs):
                            pat_indexs[i] = index + elem_i - pos_i
                        # print('<CAND>', cand, end='')
                        # print('-> ', pos, elem_i, pat_cands, pat_indexs)
                        yield pos, elem_i, pat_cands, pat_indexs  # N 6 ['by', 'N'] [4, 6]


def get_prev_aftr_word(words, chunkposs, index):
    prev_index = aftr_index = -1
    time = 0
    for i in reversed(list(range(0, index[0]))):
        if i == 0:
            prev_index = 0
        elif chunkposs[i][0] == 'H':
            time += 1
        if time == 2:
            if re.search('[^a-zA-Z0-9_]+', words[i + 1]):
                prev_index = i + 2
            else:
                prev_index = i + 1
            break
    # if prev_index > -1: prev = words[prev_index]+' '
    if prev_index > -1:
        prev = ' '.join(words[prev_index:index[0]]) + ' '
    else:
        prev = ''
    for i in range(index[-1] + 1, len(words)):
        if chunkposs[i][0] == 'H':
            # or chunkposs[i][0] == 'O':
            aftr_index = i
            break
    # if aftr_index != -1: aftr = ' '+words[aftr_index]
    if aftr_index != -1:
        aftr = ' ' + ' '.join(words[index[-1] + 1:aftr_index + 1])
    else:
        aftr = ''
    return prev, aftr

# input_lines = 'I have great difficulty in understanding him .\tI have great difficulty in understand him .\tPRP VBP JJ NN IN VBG PRP .\tH-NP H-VP I-NP H-NP H-PP H-VP H-NP O'.split('\n')
# input_lines = 'I have great paper in understanding him .\tI have great paper in understand him .\tPRP VBP JJ NN IN VBG PRP .\tH-NP H-VP I-NP H-NP H-PP H-VP H-NP O'.split('\n')
input_lines = [
    'The second source of difficulty is contributed by an attempt to express the spatially variant nature of atmospheric visibility using a single representative value , distance .\tThe second source of difficulty be contribute by an attempt to express the spatially variant nature of atmospheric visibility use a single representative value , distance .\tDT JJ NN IN NN VBZ VBN IN DT NN TO VB DT RB JJ NN IN JJ NN VBG DT JJ JJ NN , NN .\tI-NP I-NP H-NP H-PP H-NP I-VP H-VP H-PP I-NP H-NP I-VP H-VP I-NP I-NP I-NP H-NP H-PP I-NP H-NP H-VP I-NP I-NP I-NP H-NP O H-NP O',
]


def test_lines():
    import geniatagger
    tagger = geniatagger.GeniaTagger('../geniatagger/geniatagger')
    input_lines = [
        " The king will have to clip his one's wings in order to keep power himself.",
    ]
    lines = []
    for line in input_lines:
        genia = tagger.parse(line)
        tags = genPatterns.convertBIO2IHO(genia)
        lines.append('\t'.join([' '.join(tag) for tag in zip(*tags)]))
    return lines


def mapper():
    table = Counter()
    # for line in input_lines:
    # for line in test_lines():
    for line in fileinput.input():
        # print line
        try:
            words, lemmas, poss, chunkposs = line.strip().split('\t', 3)
        except Exception:
            print(line, file=sys.stderr)
            sys.exit(1)

        words, lemmas = words[0].lower() + words[1:], lemmas[0].lower(
        ) + lemmas[1:]
        words, lemmas = list(map(methodcaller('split'), (words, lemmas)))
        if set(words + lemmas) & aklWords:
            poss, chunkposs = list(map(methodcaller('split'),
                                       (poss, chunkposs)))
            if chose == 'combine':
                wordElement = PGelements.word2Element(words, lemmas, poss,
                                                      chunkposs)
                keywordpos, pat_poslen = PGpatterns.Keywordpos, PGpatterns.Pat_poslen
                gra_pat_dict = defaultdict(list)
                pos_map, gra_pat_map = PGpatterns.Pos_map, PGpatterns.Pat_map
                for pos, elem_i, cand, index in gen_candiate(
                    wordElement, words, lemmas, keywordpos, pat_poslen):
                    gra_pat_dict[(pos_map[pos], elem_i)].append((cand, index))
                wordElement = Colelements.word2Element(words, lemmas, poss,
                                                       chunkposs)
                keywordpos, pat_poslen = Colpatterns.Keywordpos, Colpatterns.Pat_poslen
                col_pat_dict = defaultdict(list)
                pos_map, pat_map = Colpatterns.Pos_map, Colpatterns.Pat_map
                for pos, elem_i, cand, index in gen_candiate(
                    wordElement, words, lemmas, keywordpos, pat_poslen):
                    col_pat_dict[(pos_map[pos], elem_i)].append((cand, index))
                for pos, elem_i in gra_pat_dict:
                    for gra_cand, gra_index in gra_pat_dict[(pos, elem_i)]:
                        pat = gra_pat_map[' '.join(gra_cand)]
                        gra_dict = dict(zip(gra_index, gra_cand))
                        if col_pat_dict[(pos, elem_i)]:
                            for col_cand, col_index in col_pat_dict[
                                (pos, elem_i)
                            ]:
                                col_index = [i for i in col_index
                                             if i < gra_index[0]]
                                if not set(col_index) <= set(gra_index):
                                    # if True:
                                    # print col_index, gra_index, not set(col_index) <=
                                    # set(gra_index)
                                    # print('<<<here', com_index)
                                    com_index = sorted(
                                        list(set(gra_index) | set(col_index)))
                                    com_cand = []
                                    for i in com_index:
                                        if i == elem_i:
                                            com_cand.append(lemmas[i].lower())
                                        elif i in gra_index:
                                            com_cand.append(gra_dict[i])
                                        elif i in col_index:
                                            com_cand.append(lemmas[i].lower())
                                    pat = ' '.join(com_cand)
                                    iocs = ' '.join([lemmas[i].lower()
                                                     for i in com_index])
                                    example = ' '.join(
                                        words[com_index[0]:com_index[-1] + 1])
                                    prev, aftr = get_prev_aftr_word(
                                        words, chunkposs, com_index)
                                    example = '{}[{}]{}'.format(prev, example,
                                                                aftr)
                                    patdata = '{}:{}\t{}\t{}\t{}'.format(
                                        lemmas[elem_i].lower(), pos_map[pos],
                                        pat, iocs, example)
                                    table[patdata] += 1
                                    print(patdata)
            else:
                if chose == 'grammar':
                    wordElement = PGelements.word2Element(words, lemmas, poss,
                                                          chunkposs)
                    keywordpos, pat_poslen = PGpatterns.Keywordpos, PGpatterns.Pat_poslen
                elif chose == 'collocation':
                    wordElement = Colelements.word2Element(words, lemmas, poss,
                                                           chunkposs)
                    keywordpos, pat_poslen = Colpatterns.Keywordpos, Colpatterns.Pat_poslen
                # print '\n'
                # for word, lemma, pos, chunkpos, element in izip(words, lemmas, poss, chunkposs, wordElement):
                # print '{}\t{}\t{}\t{}\t{}'.format(word, lemma, pos, chunkpos, element)
                for pos, elem_i, cand, index in gen_candiate(
                    wordElement, words, lemmas, keywordpos,
                    pat_poslen):  # pos:N elem_i:6 cand:['by', 'N'] index:[4, 6]
                    example = ' '.join(words[index[0]:index[-1] + 1])
                    example_sym = {
                        i - index[0]: c
                        for i, c in zip(index, cand)
                    }  # joe
                    prev, aftr = get_prev_aftr_word(words, chunkposs, index)
                    example = '{}[{}]{}'.format(prev, example, aftr)
                    if chose == 'grammar':  # <-- HERE

                        pat = []
                        for w in cand:
                            if w in keywordpos:
                                if w in ['ADJER', 'ADJEST', 'V-ed', 'ADJERP',
                                         'ADJESTP']:
                                    pat.append(
                                        '{}'.format(words[elem_i].lower()))
                                else:
                                    pat.append(lemmas[elem_i].lower())
                            else:
                                pat.append(w)
                        pat = ' '.join(pat)
                        iocs = ' '.join([lemmas[j] for j in index])
                        pos_map, pat_map = PGpatterns.Pos_map, PGpatterns.Pat_map
                        patdata = '{}:{}\t{}\t{}\t{}\t{}'.format(
                            lemmas[elem_i].lower(), pos_map[pos], pat, iocs,
                            example, repr(example_sym))
                    elif chose == 'collocation':
                        iocs = ' '.join([lemmas[j] for j in index])
                        pat = '({}){}'.format(' '.join(cand), iocs)
                        pos_map, pat_map = Colpatterns.Pos_map, Colpatterns.Pat_map
                        patdata = '{}:{}\t{}\t{}\t{}\t{}'.format(
                            lemmas[elem_i].lower(), pos_map[pos], pat, iocs,
                            example, json.dumps(example_sym))
                    table[patdata] += 1
                    # print(patdata, 1)

    for patdata, cnt in table.items():
        print('{}\t{}'.format(patdata, cnt))
    fileinput.close()


if __name__ == '__main__':
    mapper()
