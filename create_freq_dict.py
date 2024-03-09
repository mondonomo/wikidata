import json
import gzip
from tqdm.auto import tqdm
from collections import Counter, defaultdict
import pickle

import sys
import glob

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

    if False:
        sys.path.insert(0, '../nelma')
        sys.path.insert(0, '../nelma/mondoDB')

        from mondoDB.mtokenize import m_tokenize
        from mondoDB.get_script import get_script
        from wiki_labels import qid_lab_get, qid_lab

        maxq = qid_lab.shape[0]
        START = 90_000_000+15_000_000
        END = maxq
        fn = 'wiki_freq_dict_from_105_to_end.pickle'
        if True:
            dic = defaultdict(Counter)
        else:
            dic = pickle.load(open(fn, 'rb'))
            START = 4_000_000
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
                prog.set_description(f'S: {qid} #{len(dic)}')
                prog.update(10_000)
            #if qid % 10_000_000 == 0:
            #    pickle.dump(dic, open(fn, 'wb'))

        pickle.dump(dic, open(fn, 'wb'))
        dict = None

    if True:
        fs = sorted(glob.glob('/projekti/wikidata/wiki_freq_dict*.pickle'))[::-1]
        d_all = {}
        for fn in fs:
            print('loading ', fn)
            d = pickle.load(open(fn, 'rb'))
            if not d_all:
                d_all = {k: (v["per"], v["org"], v["loc"], v["O"]) for k, v in d.items()}
            else:
                print('joining ...')
                d_all.update({k: (d[k]["per"]+d_all[k][0], d[k]["org"]+d_all[k][1], d[k]["loc"]+d_all[k][2],
                                d[k]["O"]+d_all[k][3]) for k in d_all.keys() & d.keys()})
                d_all.update({k: (d[k]["per"], d[k]["org"], d[k]["loc"], d[k]["O"]) for k in
                              set(d.keys()).difference(d_all.keys())})

            print('joined ', len(d_all))
        print('saving ...')
        fo = gzip.open('/projekti/mondodb/lists/wikidata_token.tsv.gz', 'wt')
        for k, v in d_all.items():
            fo.write(f'{k}\t'+'\t'.join([str(a) for a in v])+'\n')
        fo.close()
        print('done!')


