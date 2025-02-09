from scipy.sparse import csr_matrix, save_npz, lil_matrix, load_npz
import numpy as np
import marisa_trie
import json
from pathlib import Path
import threading
from functools import lru_cache
from typing import Dict, Set, List, Tuple, Optional
import mmap
import os


class SharedDataManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(SharedDataManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        with self._lock:
            if self._initialized:
                return

            BASE_DIR = Path(__file__).resolve().parent
            DATA_DIR = Path.joinpath(BASE_DIR, 'data')

            # For sparse matrices, we need to load them completely
            # as scipy.sparse doesn't support memory mapping directly
            self.qid_lab = load_npz(f'{DATA_DIR}/qidlabel.npz')
            self.lab_qid = load_npz(f'{DATA_DIR}/labelqid.npz')

            # Memory map the description data
            desc_npz = np.load(f'{DATA_DIR}/desc.npz', mmap_mode='r')
            self.descl = desc_npz['descl']

            # Load tries
            self.label_trie = marisa_trie.Trie()
            self.label_trie.load(f'{DATA_DIR}/labels.trie')

            self.desc_trie = marisa_trie.Trie()
            self.desc_trie.load(f'{DATA_DIR}/desc.trie')

            # Load label mappings
            with open(f'{DATA_DIR}/label4sparse.json') as f:
                j = json.load(f)
            self.id2lang = {v: set(k.split(';')) for k, v in j['lang2id'].items()}

            self._initialized = True

    def qid_lab_get(self, qid: int, filter_lang: Optional[str] = None,
                    include_alt: bool = False, return_alt: bool = False) -> Dict:
        """Implementation of qid_lab_get with direct sparse matrix access"""
        try:
            # Get the sparse row for the QID
            row = self.qid_lab.getrow(qid)
            ls = row.indices
            langs = row.data
        except Exception as e:
            print(f"Error accessing sparse matrix for qid {qid}: {str(e)}")
            return {}

        rec = {}
        for lid, langs_id in zip(ls, langs):
            if not return_alt:
                if langs_id > 0 or include_alt:
                    if filter_lang:
                        if len(filter_lang) == 2:
                            langs = {(k.split('_')[0] if '_' in k else k[:2])
                                     for k in self.id2lang[abs(langs_id)]}
                        else:
                            langs = self.id2lang[abs(langs_id)]
                    if not filter_lang or filter_lang in langs:
                        rec[self.label_trie.restore_key(lid)] = langs
            else:
                l = self.label_trie.restore_key(lid)
                rec[l] = set()
                for lang_id in self.id2lang[abs(langs_id)]:
                    if lang_id == 'sh_Latn':
                        rec[l].add(('hr_Latn', langs_id > 0))
                        rec[l].add(('sr_Latn', langs_id > 0))
                        rec[l].add(('bs_Latn', langs_id > 0))
                    else:
                        rec[l].add((lang_id, langs_id > 0))
        return rec

    def qid_en_desc_get(self, qid: int) -> str:
        """Implementation of qid_en_desc_get"""
        try:
            did = self.descl[qid]
            if did > 0:
                return self.desc_trie.restore_key(did + -1)
        except Exception as e:
            print(f"Error accessing description for qid {qid}: {str(e)}")
        return ''

    def find_qid(self, l: str, lang: Optional[str] = None, include_alt: bool = True) -> List[int]:
        """Implementation of find_qid"""
        try:
            if l in self.label_trie:
                lid = self.label_trie[l]
                row = self.lab_qid[lid]
                _, qs = row.nonzero()
                values = row if lang else None
                rec = [a for a in qs if (not lang or self.id2lang[abs(values[0, a])])
                       and (include_alt or values[0, a] > 0)]
            else:
                return []
            return rec
        except Exception as e:
            print(f"Error in find_qid for label {l}: {str(e)}")
            return []


# Global instance - initialize before forking
data_manager = SharedDataManager()


# API functions
def qid_lab_get(qid: int, filter_lang: str = None,
                include_alt: bool = False, return_alt: bool = False) -> Dict:
    return data_manager.qid_lab_get(qid, filter_lang, include_alt, return_alt)


def qid_en_desc_get(qid: int) -> str:
    return data_manager.qid_en_desc_get(qid)


def find_qid(l: str, lang: str = None, include_alt: bool = True) -> List[int]:
    return data_manager.find_qid(l, lang, include_alt)


if __name__ == '__main__':
    import multiprocessing

    # Initialize data_manager before creating the pool
    _ = data_manager.qid_lab_get(1)  # Force initialization

    with multiprocessing.Pool(processes=4) as pool:
        results = pool.map(qid_lab_get, [901, 902, 903, 904])
        print(results)