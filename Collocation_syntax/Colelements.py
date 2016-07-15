#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from collections import defaultdict

import genPatterns
import Colpatterns
from operator import methodcaller
stopwordlist = [
    'i',
    'me',
    'my',
    'myself',
    'we',
    'our',
    'ours',
    'ourselves',
    'you',
    'your',
    'yours',
    'yourself',
    'yourselves',
    'he',
    'him',
    'his',
    'himself',
    'she',
    'her',
    'hers',
    'herself',
    'it',
    'its',
    'itself',
    'they',
    'them',
    'their',
    'theirs',
    'themselves',
    # 'what', 'which', 'who', 'whom', 'this', 'that', 'these', 'those',
    'am',
    'is',
    'are',
    'was',
    'were',
    'be',
    'been',
    'being',
    # 'have', 'has', 'had', 'having', 'do', 'does', 'did', 'doing',
    'a',
    'an',
    'the',
    # 'and', 'but', 'if', 'or', 'because', 'as', 'until',
    # 'while', 'of', 'at', 'by', 'for', 'with', 'about', 'against', 'between', 'into',
    # 'through', 'during', 'before', 'after', 'above', 'below', 'to', 'from', 'up', 'down',
    # 'in', 'out', 'on', 'off', 'over', 'under', 'again', 'further', 'then', 'once', 'here',
    # 'there', 'when', 'where', 'why', 'how', 'all', 'any', 'both', 'each', 'few', 'more',
    # 'most', 'other', 'some', 'such', 'no', 'nor', 'not', 'only', 'own', 'same', 'so',
    # 'than', 'too', 'very',
    's',
    't',  # 'can', 'will', 'just', 'don', 'should', 'now'
]

from pathlib import Path
filedir = Path(__file__).absolute().parent

with (filedir / 'EnglishPhrasalVerb.txt').open() as fpr:
    VerbPhrases = defaultdict(lambda: defaultdict(list))
    for line in fpr:
        if line.rstrip():
            VerbPhrases[line.split('_', 1)[
                0
            ]][line.count('_') + 1].append(line.rstrip().split('_', 1)[1])
# print VerbPhrases
with (filedir / 'writeaway_word.txt').open() as fpr:
    aklWords = set([line.rstrip() for line in fpr])

general = '''VERB NOUN ADJ something BE do doing did done ADJER ADJEST wh- det. 
one's oneself clause amount number -thing someway NOUN's ADVER COLOR'''.split()
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
    # 'VB':['do', 'inf'], 'VBZ':['do'], 'VBD':['do'], 'VBG':['do', 'doing'], 'VBN':['do', 'done'], 'VBP':['do'],
    'VB': ['do'],
    'VBZ': ['do'],
    'VBD': ['do'],
    'VBG': ['do'],
    'VBN': ['do'],
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
    'PRP': ['sth'],
    'PRP$': ["'one's", '_'],
}

nonhead_map = {
    'NN': ['n', '_'],
    'NNP': ['n', '_'],
    'NNS': ['n', '_'],
    'NNPS': ['n', '_'],
    'VB': ['v', '_'],
    'VBP': ['v', '_'],
    'VBG': ['v', '_'],
    'VBZ': ['v', '_'],
    'VBN': ['v', '_'],
    'VBD': ['v', '_'],
    'JJ': ['adj', '_'],
    'JJR': ['adj', '_'],
    'JJS': ['adj', '_'],
    'RB': ['adv', '_'],
    'RBR': ['adv', '_'],
    'RBS': ['adv', '_'],
    'DT': ['det.', '_'],
    'IN': ['?'],
    'CD': ['amount', '_'],
    'PRP': ['sth'],
    'PRP$': ["'one's", '_'],
    'MD': ['_'],
    'WP': ['wh'],
    'WRB': ['wh'],
    'CC': ['and'],
    'TO': ['to'],
}

Keywordpos = Colpatterns.Keywordpos


def word2Element(words, lemmas, poss, chunkposs):
    sentlen = len(words)
    pat = []
    verb_tag = 0
    for i, (word, lemma, pos, chunkpos) in enumerate(zip(words, lemmas, poss,
                                                         chunkposs)):
        element = []
        # The word we focus on.
        # if word not in stopwordlist:
        if re.match('N(N|NS|NP|NPS)', pos):
            element.append('N')
        elif re.match('V(B|BZ|BD|BN|BP|BG)', pos):
            element.append('V')
        elif re.match('J(J|JR|JS)', pos):
            if chunkpos[0] == 'H': element.append('ADJP')
            else: element.append('ADJ')
        # Special case 6: Verb Phrase.
        if verb_tag:
            element.append('_')
            verb_tag -= 1
        # Special case 1: [V] to V, [in order] to V
        elif re.match('V(B|BD|BN|BG|BP)', pos) and i + 1 < sentlen and poss[
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
        # Special case 2: one's
        elif i + 1 < sentlen and words[i + 1] == "'s" and re.match(
            'N(N|NS|NP|NPS)', pos):
            element.extend(["one's", 'sth'])
        elif word.endswith('’s') and re.match('N(N|NS|NP|NPS)', pos):
            element.extend(["one's", 'sth'])
        # Special case 3: carry [out].
        elif chunkpos == 'H-PRT' and pos == 'RP':
            element.extend(['PRT', lemma])
        # Special case 4: what
        elif chunkpos == 'H-SBAR':
            if lemma in 'what when where who which why how'.split(' '):
                element.append('wh')
            else:
                element.append(lemma)
        elif chunkpos == 'I-SBAR':
            element.append(lemma)
        # save orginal word.
        else:
            if word in stopwordlist or lemma in stopwordlist:
                # if False:
                element.append('O')
            else:
                # General headword case.
                if chunkpos[0] in ['H', 'O']:
                    if pos in head_map:
                        element.extend(head_map[pos])
                # General nonheadword case.
                elif chunkpos[0] == 'I':
                    if pos in nonhead_map:
                        element.extend(nonhead_map[pos])
        if lemma in VerbPhrases:
            for length in sorted(iter(VerbPhrases[lemma].keys()),
                                 reverse=True):
                if ' '.join(
                    lemmas[i + 1:i + length]) in VerbPhrases[lemma][length]:
                    verb_tag = length - 1
                    break
        # print word, element
        pat.append(element)
    return pat


input_lines = [
    # 'A simulation model is successful if it leads to policy action , i.e. , if it is implemented .\tA simulation model be successful if it lead to policy action , i.e. , if it be implement .\tDT NN NN VBZ JJ IN PRP VBZ TO NN NN , FW , IN PRP VBZ VBN .\tI-NP I-NP H-NP H-VP H-ADJP H-SBAR H-NP H-VP H-PP I-NP H-NP O O O H-SBAR H-NP I-VP H-VP O',
    'The research program that began in Vienna and Berlin continues , even though many of the specific formulations that came out of those circles are flawed and need to be replaced .\tThe research program that begin in Vienna and Berlin continue , even though many of the specific formulation that come out of those circle be flaw and need to be replace .\tDT NN NN WDT VBD IN NNP CC NNP VBZ , RB IN JJ IN DT JJ NNS WDT VBD IN IN DT NNS VBP VBN CC VBP TO VB VBN .\tI-NP I-NP I-NP H-NP H-VP H-PP I-NP I-NP H-NP H-VP O I-SBAR H-SBAR H-NP H-PP I-NP I-NP I-NP H-NP H-VP I-PP H-PP I-NP H-NP I-VP H-VP O I-VP I-VP I-VP H-VP O'
]


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
