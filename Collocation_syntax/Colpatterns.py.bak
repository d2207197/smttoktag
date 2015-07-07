#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import defaultdict

Coltable = {
    'NOUN':{
        'adj N':['adj N'],
        'n N':['n N'],
        'N n':['N sth'],
        'v N':['do N'],
        'N v':['N do'],
        'N *prep n':['N prep sth'],
        'n *prep N':['sth prep N'],
        'v *prep N':['do prep N'],
        'and/or':['sth and N', 'N and sth']
    },
    'VERB':{
        'adv V':['adv V'],
        'V n':['V sth'],
        'n V':['sth V'],
        'V adj':['V adjp'],
        'V *prep n':['V prep sth'],
        'V to v':['V to v'],
        'v to V':['v to V'],
        'and/or':['do and V', 'V and do']
    },
    'ADJ':{
        'adv ADJ':['adv ADJ'],
        'v ADJ':['do ADJP'],
        'ADJ n':['ADJ sth'],
        'ADJ to v':['ADJ to do'],
        'adj ADJ':['adj ADJ'],
        'ADJ adj':['ADJ adj'],
        'ADJ *prep n':['ADJ prep sth'],
        'and/or':['adj and ADJ', 'ADJ and adj']
    }
    
}

Keywordpos = set(['N', 'V', 'ADJ', 'ADJP'])
Pos_map = {'N':'N', 'V':'V', 'ADJ':'ADJ', 'ADJP':'ADJ'}
Pat_poslen = defaultdict(lambda:defaultdict(list))
Pat_map = {}
for pos in Coltable:
    for pg, pats in Coltable[pos].iteritems():
        for pat in pats:
            pos = (set(pat.split()) & Keywordpos).pop()
            Pat_poslen[pos][(pat.count(' ')+1, pat.split(' ').index(pos))].append(pat.strip())
            Pat_map[pat] = pg

