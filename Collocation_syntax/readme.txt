使用方法：
pv 100000citeseerx.sents.tagged.h | lmr 5m 4 'python mapper.py' 'python reducer.py' pgp_test
pv citeseerx.sents.tagged.h | lmr 100m 40 'python mapper.py' 'python reducer.py' citeseerx_pat
pv bnc_tagged.h | lmr 25m 10 'python mapper.py' 'python reducer.py' bnc_pat


程式：
        mapper.py:
input:
        "<words>\t<lemmas>\t<poss>\t<chunkposs>"
Example:
        "A simulation model is successful if it leads to policy action , i.e. , if it is implemented .   A simulation model be successful if it lead to policy action , i.e. , if it be implement .      DT NN NN VBZ JJ IN PRP VBZ TO NN NN , FW , IN PRP VBZ VBN .     I-NP I-NP H-NP H-VP H-ADJP H-SBAR H-NP H-VP H-PP I-NP H-NP O O O H-SBAR H-NP I-VP H-VP O"
output:
        "<word:pos>\t<pattern>\t<head words>\t<example words>\t<count>"
Example:
        "task:N    task of sth    task of relevance    is a [task of primary relevance]    1"


        reducer.py
input:
         "<word:pos>\t<pattern>\t<head words>\t<example words>\t<count>"
Example:
        "task:N    task of sth    task of relevance    is a [task of primary relevance]    1"
output:
        .json file
Example:
"""
{
    "abandon:N": [
        39, 
        4.875, 
        4.044672421840859, 
        [
            "with abandon", 
            11, 
            [
                [
                    "times [with an uncharacteristically voluptuous abandon]", 
                    11, 
                    1
                ]
            ]
        ]
    ], 
    ...
}
"""

