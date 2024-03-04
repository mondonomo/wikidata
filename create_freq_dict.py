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
        maxq = qid_lab.shape[0]
        START = 30_000_000
        END = maxq
        fn = '/projekti/mondodb/lists/wiki_freq_dict_from_30.pickle'
        dic = defaultdict(Counter)

        # maxq = 1000
        prog = tqdm(total=END-START)
        for qid in range(START, END):
            ent_t = named_ent[qid] if qid in named_ent else 'O'
            for label, langs in qid_lab_get(qid, return_alt=True).items():
                for lang, _ in langs:
                    lang2 = None
                    if len(lang) >= 7 and lang[2] == '_':
                        lang2 = lang[:2]
                    elif len(lang) == 2:
                        lang2 = lang
                    if lang2:
                        script = get_script(label)
                        for token in m_tokenize(label, lang2, script, True):
                            dic[f'{lang2}_{script}_{token}'][ent_t] += 1
            if qid % 10_000 == 0:
                prog.set_description(f'#{len(dic)}')
                prog.update(10_000)
            if qid % 1_000_000 == 0:
                pickle.dump(dic, open(fn, 'wb'))

        pickle.dump(dic, open(fn, 'wb'))


