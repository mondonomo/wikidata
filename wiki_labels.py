from scipy.sparse import csr_matrix, save_npz, lil_matrix, load_npz
import numpy as np
import marisa_trie
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path.joinpath(BASE_DIR, 'data')

#print('loading')
trie = marisa_trie.Trie()
trie.load(f'{DATA_DIR}/labels.trie')
j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
id2lang = {v:set(k.split(';')) for k, v in j['lang2id'].items()}
qid_lab = load_npz(f'{DATA_DIR}/qidlabel.npz')
lab_qid = load_npz(f'{DATA_DIR}/labelqid.npz')
#print('loaded')
tried = marisa_trie.Trie()
tried.load(f'{DATA_DIR}/desc.trie')
descl = np.load(f'{DATA_DIR}/desc.npz')['descl']


def qid_lab_get(qid:int, lang:str=None, include_alt:bool=False):
    try:
        ls = qid_lab[qid].indices
        langs = qid_lab[qid].data
    except:
        return {}
    rec = {}
    for lid, lang_id in zip(ls, langs):
        if lang_id > 0 or include_alt:
            langs = id2lang[abs(lang_id)]
            if not lang or lang in langs:
                rec[trie.restore_key(lid)] = langs
    return rec


def qid_en_desc_get(qid:int):
    did = descl[qid]
    if did > 0:
        return tried.restore_key(did+-1)
    else:
        return ''

def find_qid(l: str, lang:str=None, include_alt:bool=True):
    if l in trie:
        lid = trie[l]
        _, qs = lab_qid[lid].nonzero()
        values = lab_qid[lid] if lang else None
        rec = [a for a in qs if (not lang or id2lang[abs(values[0, a])]) and (include_alt or values[0, a]>0)]
    else:
        return []
    return rec


if __name__ == '__main__':
    #print(qid_lab_get(42, lang='th', include_alt=True))
    #print(qid_lab_get(6348))
    #print(DATA_DIR)
    #print(trie.restore_key(14016), trie.restore_key(3201431))

    #print(qid_en_desc_get(5))

    #print(find_qid('mba'))

    #print(qid_lab_get(191701, include_alt=True))
    #print(qid_lab_get(177053, include_alt=True))
    #print(qid_lab_get(177053, lang='th', include_alt=True))
    print(qid_lab_get(15732892, lang='th'))