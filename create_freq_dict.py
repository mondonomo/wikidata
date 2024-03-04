import json
import gzip
from tqdm.auto import tqdm
from collections import Counter, defaultdict
import pickle

import sys
sys.path.insert(0, '../nelma')
sys.path.insert(0, '../nelma/mondoDB')

from mondoDB.mtokenize import m_tokenize
from mondoDB.get_script import get_script
from wiki_labels import qid_lab_get, qid_lab


def get_ent_type():
    named_ent = {}
    types_c = Counter()
    for i, l in tqdm(enumerate(gzip.open('/backup/wikidata/wikinelma.jsonl.gz', 'rt')), total=25384364):
        j = json.loads(l)
        named_ent[int(j['wiki_id'][1:])] = j['type']
        types_c[j['type']] += 1
    pickle.dump((named_ent, types_c), open('/backup/wikidata/wikinelma_ids.pickle', 'wb'))


if __name__ == '__main__':
    if False:
        get_ent_type()
    (named_ent, types_c) = pickle.load(open('wikinelma_ids.pickle', 'rb'))

    if True:
        START = 30_000_000
        dic = defaultdict(Counter)

        maxq = qid_lab.shape[0]
        # maxq = 1000
        for qid in tqdm(range(START, maxq), total=maxq-START):
            ent_t = named_ent[qid] if qid in named_ent else 'O'
            for label, langs in qid_lab_get(qid, return_alt=True).items():
                lang2 = None
                if len(lang) >= 7 and lang[2] == '_':
                    lang2 = lang[:2].split('_')
                if len(lang) == 2:
                    lang2 = lang
                if lang:
                    script = get_script(label)
                    for token in m_tokenize(label, lang2, script, True):
                        dic[f'{lang2}_{script}_{token}'][ent_t] += 1
            if qid % 1_000_000 == 0:
                pickle.dump(dic, open('wiki_freq_dict_from60M.pickle', 'wb'))
                
        pickle.dump(dic, open('wiki_freq_from60M_dict.pickle', 'wb'))


