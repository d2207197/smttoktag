#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re

import genPatterns
import PGpatterns
from operator import methodcaller

# TODO: sth of sth.
# TODO: be V-ed by.
from pathlib import Path
filedir = Path(__file__).absolute().parent

aklWords = set(line.rstrip()
               for line in (filedir / 'writeaway_word.txt').open())

general = '''VERB NOUN ADJ something BE do doing did done ADJER ADJEST wh- det. 
one's oneself clause amount number -thing someway NOUN's ADVER COLOR'''.split()
lexical = '''to as that it for of what in with where how who when be about on 
there from enough than one thing at by towards too over into between against 
under toward split so out onto however among way upon together such since off 
if and after a without within through round not like favor favour behind around across though'''.split(
)
linkingVerbs = '''average be compose comprise constitute cover equal extend feel form go keeo 
lie make measure number pass prove rank rate remain represent stand stay total weigh
become come fall form get go grow make turn creep drift edge inch move
act apear feel look play seem smell sound taste
act amount begin come consist coninue convert double end figure finish function 
tie masquerade operate originate parade pass pose serve rank rate remain reside resolve run shade stand start transmute turn
appear be come fall feel get go keep lie look prove remain seem sound stay
be become remain'''.split()

head_map = {
    'NN': ['sth'],
    'NNS': ['sth'],
    'NNP': ['sth'],
    'NNPS': ['sth'],
    'VB': ['do', 'inf'],
    'VBZ': ['do'],
    'VBD': ['do'],
    'VBG': ['do', 'doing'],
    'VBN': ['do', 'done'],
    'VBP': ['do'],
    'JJ': ['adjp'],
    'JJR': ['adjp'],
    'JJS': ['adjp'],
    'RB': ['advp', '_'],
    'RBR': ['advp', '_'],
    'RBS': ['advp', '_'],
    'CD': ['amount'],
    'TO': ['to', 'prep'],  # to/prep
    'DT': ['THIS', 'sth'],  # this
    'MD': ['_'],
    'SYM': ['_'],
    'LS': ['_'],
    'HYPH': ['_'],
    'POS': ['_'],
    'UH': ['_'],
    'FW': ['sth'],
    'IN': ['prep'],
    'WDT': ['wh'],
    'WP': ['wh'],
    'WRB': ['wh'],
    'CC': ['and'],
    'EX': ['there'],
    'PRP$': ["'one's", '_'],
}
nonhead_map = {
    'NN': ['_'],
    'NNP': ['_'],
    'NNS': ['_'],
    'NNPS': ['_'],
    'VB': ['_'],
    'VBP': ['_'],
    'VBG': ['_'],
    'VBZ': ['_'],
    'VBN': ['_'],
    'VBD': ['_'],
    'JJ': ['adj', '_'],
    'JJR': ['adj', '_'],
    'JJS': ['adj', '_'],
    'RB': ['adv', '_'],
    'RBR': ['adv', '_'],
    'RBS': ['adv', '_'],
    'DT': ['det', '_'],
    'POS': ['_'],
    'IN': ['?'],
    'CD': ['amount', '_'],
    'PRP': ['one', 'sth'],
    'PRP$': ["'one's", '_'],
    'MD': ['_'],
    'WP': ['wh'],
    'WRB': ['wh'],
    'TO': ['to'],
}

Keywordpos = PGpatterns.Keywordpos


def word2Element(words, lemmas, poss, chunkposs):
    sentlen = len(words)
    pat = []
    for i, (word, lemma, pos, chunkpos) in enumerate(zip(words, lemmas, poss,
                                                         chunkposs)):
        element = []
        # The word we focus on.
        if chunkpos[0] != 'O':
            if re.match('N(N|NS|NP|NPS)', pos):
                element.append('N')
                if lemma in ['something', 'anything', 'nothing']:
                    element.append('-thing')
            elif re.match('V(B|BD|BN|BP|BG)', pos):
                # if lemma in linkingVerbs: element.append('VLINK')
                if pos == 'VBN': element.append('V-ed')
                else: element.append('V')
                if lemma in linkingVerbs:
                    element.append('v-link')
            elif re.match('J(J|JR|JS)', pos):
                if chunkpos[0] == 'I':
                    if pos == 'JJ': element.append('ADJ')
                    if pos == 'JJR': element.append('ADJER')
                    elif pos == 'JJS': element.append('ADJEST')
                else:
                    if pos == 'JJ': element.append('ADJP')
                    if pos == 'JJR': element.append('ADJERP')
                    elif pos == 'JJS': element.append('ADJESTP')
            elif re.match('R(B|BR|BS)', pos):
                if pos == 'RB': element.append('ADV')
                if pos == 'RBR': element.append('ADVER')
                elif pos == 'RBS': element.append('ADVEST')
        # Special case 1: [V] to V, [in order] to V
        if re.match('V(B|BD|BN|BP|BG)', pos) and i + 1 < sentlen and poss[
            i + 1
        ] == 'TO' and chunkposs[i + 1][0] == 'I':
            element.extend(head_map[pos])
        elif i + 2 < sentlen and chunkpos == 'H-SBAR' and lemma == 'in' and lemmas[
            i + 1
        ] == 'order' and lemmas[i + 2] == 'to':
            element.append('_')
        elif i > 0 and i + 1 < sentlen and chunkposs[
            i - 1
        ] == 'H-SBAR' and lemmas[
            i - 1
        ] == 'in' and lemma == 'order' and lemmas[i + 1] == 'to':
            element.append('_')
        # Special case 2: one[self]
        elif pos == 'PRP':
            if lemma in 'myself ourself ourselves yourself yourselves himself herself themself'.split(
                ' '):
                element.extend(['oneself', 'sth'])
            else:
                element.extend(['one', 'sth'])
        # Special case 3: one's
        elif i + 1 < sentlen and words[i + 1] == "'s" and re.match(
            'N(N|NS|NP|NPS)', pos):
            element.extend(["one's", 'sth'])
        elif word.endswith('’s') and re.match('N(N|NS|NP|NPS)', pos):
            element.extend(["one's", 'sth'])
        # Special case 4: carry [out].
        elif chunkpos == 'H-PRT' and pos == 'RP':
            element.extend(['PRT', lemma])
        # Special case 5: wh
        elif chunkpos == 'H-SBAR':
            if lemma in 'what when where who which why how'.split(' '):
                element.append('wh')
            else:
                element.append(lemma)
        elif chunkpos == 'I-SBAR':
            element.append(lemma)
        # Special case 6: that
        # save orginal word.
        else:
            # General headword case.
            if chunkpos[0] in ['H', 'O']:
                if pos in head_map:
                    element.extend(head_map[pos])
            # General nonheadword case.
            elif chunkpos[0] == 'I':
                if pos in nonhead_map:
                    element.extend(nonhead_map[pos])
            if lemma in lexical and lemma not in element:
                element.append(lemma)
        pat.append(element)
    return pat


input_lines = 'Specifications of organisational structure usually have a diagrammatic form that abstracts from more detailed dynamics .\tSpecification of organisational structure usually have a diagrammatic form that abstract from more detailed dynamic .\tNNS IN JJ NN RB VBP DT JJ NN WDT VBZ IN RBR JJ NNS .\tH-NP H-PP I-NP H-NP H-ADVP H-VP I-NP I-NP I-NP H-NP H-VP H-PP I-NP I-NP H-NP O'.split(
    '\n')


def testing():
    # import geniatagger
    # tagger = geniatagger.GeniaTagger('../geniatagger/geniatagger')
    # input_lines = [
    # " Please keep quiet about the party .",
    # "I contest that Chelsea fully deserve to win this Premiership.",
    # "The family learned to cope with the disaster.",
    # ]
    # lines = []
    for line in input_lines:
        words, lemmas, poss, chunkposs = line.strip().split('\t', 3)
        words, lemmas = words[0].lower() + words[1:], lemmas[0].lower(
        ) + lemmas[1:]
        words, lemmas = list(map(methodcaller('split'), (words, lemmas)))
        if set(words + lemmas) & aklWords:
            poss, chunkposs = list(map(methodcaller('split'),
                                       (poss, chunkposs)))
            # genia = tagger.parse(line)
            # tags = genPatterns.convertBIO2IHO(genia)
            # words = [tag[0] for tag in tags]
            wordElement = word2Element(words, lemmas, poss, chunkposs)
            for word, element in zip(words, wordElement):
                print('{}\t{}'.format(word, element))


if __name__ == '__main__':
    testing()
