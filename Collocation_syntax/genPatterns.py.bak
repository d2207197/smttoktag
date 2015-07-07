#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
# import PGpatterns
from itertools import izip, product
import geniatagger
# import re_pat_map

def convertBIO2IHO(tags):
    tags.reverse()
    res, oldchunk = [], 'O'
    for tag in tags:
        word, lemma, pos, chunkpos, _ = tag
        if chunkpos[0] == 'O':
            res.append(tag[:-1])
        elif oldchunkpos[0] == 'O' or oldchunkpos[0] == 'B':
            res.append((word, lemma, pos, '{}-{}'.format('H', chunkpos[2:])))
        else:
            res.append((word, lemma, pos, '{}-{}'.format('I', chunkpos[2:])))
        oldchunkpos = chunkpos
    res.reverse()
    return res

general = '''VERB NOUN ADJ something BE do doing did done ADJER ADJEST wh- det. 
one's oneself clause amount number -thing someway NOUN's ADVER COLOR'''.split()
lexical = '''to as that it for of what in with where how who when be about on 
there from enough than one thing at by towards too over into between against 
under toward split so out onto however among way upon together such since off 
if and after a without within through round not like favor favour behind around across though'''.split()
linkingVerbs = '''average be compose comprise constitute cover equal extend feel form go keeo 
lie make measure number pass prove rank rate remain represent stand stay total weigh
become come fall form get go grow make turn creep drift edge inch move
act apear feel look play seem smell sound taste
act amount begin come consist coninue convert double end figure finish function 
tie masquerade operate originate parade pass pose serve rank rate remain reside resolve run shade stand start transmute turn
appear be come fall feel get go keep lie look prove remain seem sound stay
be become remain'''.split()
head_map = {'NN': [ 'NOUN', 'something'], 'NNS': [ 'NOUN', 'something'],
            'NNP':[ 'NOUN', 'something'], 'NNPS':[ 'NOUN', 'something'],
            'PRP' : ['one', 'something'],
            'VB':[ 'VERB', 'do'], 'VBZ':[ 'VERB', 'does'], 'VBD':[ 'VERB', 'did'],
            'VBG':[ 'VERB', 'doing'], 'VBN':[ 'VERB', 'done'], 'VBP':[ 'VERB', 'do'],
            'RB':[ 'ADV', 'adv.', '_'], 'RBR':[ 'ADV', 'adv.', '_'], 'RBS':[ 'ADV', 'adv.', '_'], 
            'CD': ['number', 'amount'], 'SYM': ['something'], 'WDT': ['wh'], 'LS': [], 'POS': ["'s"], 'HYPH': ['-'],
            'JJ': [ 'ADJ', 'adj.'], 'JJR': [ 'ADJER', 'adj.'], 'JJS': [ 'ADJEST', 'adj.'], 'EX': ['there'], 'MD': [],   
            'DT': [ 'THIS', 'something'], 'IN': [ 'prep.'], 'RP': [ 'prep.', '_'],
            'WP': [ 'wh'], 'WRB': [ 'wh'], 'UH': [], 'CC': ['and'], 'PRP': ['one'], 'PRP$': ["'one's"], 'FW': ['something'],
}
nonhead_map = {'DT': [ 'det.', '_'], 'JJ': ['adj.','_'], 'NN': ['n.','_'], 'RB': ['adv.', '_'], 'IN': ['?'],
               'VBD': ['_'], 'PRP': ['one'], 'PRP$': ["'one's", '_'], 
               'VB': ['_'], 'VBP': ['_'], 'VBG': ['_'],  'VBZ': ['_'], 'VBN': ['_'], 'NNP': ['n.','_'], 'MD': ['_'], 'WP': [ 'wh'], 'WRB': [ 'wh'],
               'VBP' : ['_'],
}

def word2Element(tags):
    pat = []
    for i, (word, lemma, pos, chunkpos) in enumerate(tags):
        element = []        
        if chunkpos == 'H-SBAR':
            element += ['clause', lemma]
        elif chunkpos == 'I-SBAR':
            element += ['_', lemma]
        elif chunkpos == 'H-ADV' and pos == 'RB':
            element += ['_', lemma]
        elif pos == 'TO':
            element += [ 'to', 'prep.' ]
        elif chunkpos == 'H-PP':
            element += [ lemma, 'prep.' ]
        elif chunkpos[0] == 'H' and lemma != 'be': #and re.match('IN|(N|V|J|R)(N|NS|NP|NPS|B|BD|BN|BP|BG|J|JR|JS)', pos)
            element += head_map[pos] if pos in head_map else [] + [ lemma ] if lemma in lexical else []
            if lemma in linkingVerbs:
                element += ['VLINK']
        elif chunkpos[0] == 'H' and lemma == 'be':
            element += [ 'VLINK' ]
        elif chunkpos[0] != 'H' and lemma in ['be', 'have']:
            element += [ lemma ]
        elif re.match('W(P|D)', pos):
            element += [ lemma, 'wh' ]
        elif pos in ('PRP', 'PRP$'):
            element += nonhead_map[pos]
        elif chunkpos[0] not in ('H', 'O') and lemma not in lexical:
            element += nonhead_map[pos] if pos in nonhead_map else ['_']
        elif chunkpos[0] not in ('H', 'O') and lemma in lexical:
            element += [ lemma ] + nonhead_map[pos] if pos in nonhead_map else ['_']
        elif chunkpos[0] == 'O' and pos in nonhead_map:
            element += nonhead_map[pos] if pos in nonhead_map else ['_']
        pat +=  [ element+[lemma] ] if lemma in lexical and lemma not in element else [ element ]
    return pat

def hasPatterns(tags):
    wordElement = word2Element(tags)
    res = [ (' '.join(cand)).replace(' _ ', '') for cand in product(*wordElement) if (' '.join(cand)).replace(' _ ', '') in PGpatterns.PGPatterns ]
    return res[0] if res else ''

def main():
    tagger = geniatagger.GeniaTagger('../geniatagger/geniatagger')
    input_lines = [
        " The king will have to clip his minister's wings in order to keep power himself.",
    ]
    for line in input_lines:
        genia = tagger.parse(line)
        raw_line = [word[0] for word in genia]
        oldchunkposs = [word[3] for word in genia]
        tags = convertBIO2IHO(genia)
        words, lemmas = [tag[0] for tag in tags], [tag[1] for tag in tags]
        poss, chunkposs = [tag[2] for tag in tags], [tag[3] for tag in tags]
        # wordElement = re_pat_map.word2Element(tags)
        wordElement = word2Element(tags)
        for word, lemma, pos, oldchunkpos, chunkpos, element in izip(words, lemmas, poss, oldchunkposs, chunkposs, wordElement):
            print '{}\t{}\t{}\t{}\t{}\t{}'.format(word, lemma, pos, oldchunkpos, chunkpos, element)
        return
        candidates = []
        for i in range(len(wordElement)):
            for j in range(i+2, i+6): # too short!
                for cand in product(*wordElement[i:j]):
                    if (' '.join(cand)).replace(' _ ', ' ') in PGpatterns.PGPatterns:
                        candidates.append([ (i, j, ' '.join(raw_line[i:j]), (' '.join(cand)).replace(' _ ', ' ')  )])
        if candidates:
            print line.strip()
            print candidates
            print

if __name__ == '__main__':
    main()
