from scipy.sparse import csr_matrix, save_npz, lil_matrix, load_npz
import marisa_trie
import json
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = f'{BASE_DIR}/data'

#print('loading')
trie = marisa_trie.Trie()
trie.load(f'{DATA_DIR}/labels.trie')
j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
id2lang = {v:set(k.split(';')) for k, v in j['lang2id'].items()}
qid_lab = load_npz(f'{DATA_DIR}/qidlabel.npz')
#print('loaded')


def qid_lab_get(qid:int, lang:str=None, include_alt:bool=False):
    _, ls = qid_lab[qid].nonzero()
    values = qid_lab[qid]
    rec = {}
    for lid in ls:
        lang_id = values[0, lid]
        if lang_id > 0 or include_alt:
            langs = id2lang[abs(lang_id)]
            if not lang or lang in langs:
                rec[trie.restore_key(lid)] = langs
    return rec


if __name__ == '__main__':
    print(qid_lab_get(42, lang='th', include_alt=True))
    print(qid_lab_get(6348))