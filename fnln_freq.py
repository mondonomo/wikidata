import json
from collections import Counter, defaultdict
import re
from text_utils import remove_reg, tokenise, get_provenance
from tqdm import tqdm
import pickle
from multiprocessing import Pool
from wiki_labels import qid_lab_get

# PAC assess fn ln order
if False:
    fnln_order = Counter()
    fo = open('/backup/wikidata/fnln_order.txt', 'w')
    for l in tqdm(open('/backup/wikidata/wiki_person.jsonl'), total=10_044_572):
        j = json.loads(l)
        qid = int(j['id'][1:])
        fnid = int(j['fn'][0][6:]) if len(j['fn']) > 0 else None
        lnid = int(j['ln'][0][6:]) if len(j['ln']) > 0 else None
        labs = qid_lab_get(qid, include_alt=False)
        if fnid:
            fns = qid_lab_get(fnid, include_alt=True)
            fns = [remove_reg.sub(' ', fn) for fn in fns.keys()]
            fn_re = re.compile('^(' + '|'.join(fns) + ').+')
            for n, langs in labs.items():
                if fn_re.match(n) and ',' not in n and len(langs) == 1:
                    for lang in langs:
                        fnln_order[(lang, 'f')] += 1
                        fo.write(f'fl\t{n}\t{lang}\n')
        if lnid:
            lns = qid_lab_get(lnid, include_alt=True)
            lns = [remove_reg.sub(' ', ln) for ln in lns.keys()]
            ln_re = re.compile('^(' + '|'.join(lns) + ').+')
            for n, langs in labs.items():
                if ln_re.match(n) and ',' not in n and len(langs) == 1:
                    for lang in langs:
                        fnln_order[(lang, 'l')] += 1
                        fo.write(f'lf\t{n}\t{lang}\n')

    pickle.dump(fnln_order, open('/backup/wikidata/fnln_order.pickle', 'wb'))
else:
    fnln_order = pickle.load(open('/backup/wikidata/fnln_order.pickle', 'rb'))


from marisa_trie import Trie
import numpy as np
from scipy.sparse import load_npz
import os

BASE_DIR, DB_DIR = '/projekti/mondoAPI', 'db'
db = Trie()
db.load(os.path.join(BASE_DIR, DB_DIR, 'names.trie'))
mat = load_npz(os.path.join(BASE_DIR, DB_DIR, 'mat.npz'))
cci, cci_id, langi, scripti, provi, tagi, tagsrc2tag, sourcei, sc2prov, detScript, s2f = \
    json.loads(open(os.path.join(BASE_DIR, DB_DIR, 'referencedb.json'), 'rb').read())
detScript = {int(k):v for k,v in detScript.items()}
types = json.load(open(os.path.join(BASE_DIR, DB_DIR, 'types.json'), 'r'))
types_a = np.array([tuple([b if b else '' for b in a]) if len(a) == 4 else ('', '', '', '') for a in types],
                   [('tip', 'U3'), ('lang', 'U2'), ('script', 'U4'), ('cc', 'U2')])

type2fnln = np.zeros((len(types_a), 2))
type2fnln[[True if a[0] in ('fn1', 'fn2', 'fn0') else False for a in types_a], 0] = 1
type2fnln[[True if a[0] == 'ln' else False for a in types_a], 1] = 1

def get_name_type(n):
    if n not in db:
        return None
    ni = db[n]
    name_m = mat[ni].dot(type2fnln)[0]
    if not np.isnan(name_m[0]):
        return 1.
    elif not np.isnan(name_m[1]):
        return 0.
    return round(name_m[0] / (name_m[0]+name_m[1]), 2)


def fnln_freq(l):
    if fnln_order[(l, 'l')] + fnln_order[(l, 'f')] == 0:
        return -1
    return fnln_order[(l, 'l')] / (fnln_order[(l, 'l')] + fnln_order[(l, 'f')])


G = 0.35


def parseit(l, include_dict=False, only2tok=True):
    j = json.loads(l)
    qid = int(j['id'][1:])
    fnid = int(j['fn'][0][6:]) if len(j['fn']) > 0 else None
    lnid = int(j['ln'][0][6:]) if len(j['ln']) > 0 else None
    labs = qid_lab_get(qid, include_alt=False)
    fns = qid_lab_get(fnid, include_alt=True) if fnid else []
    lns = qid_lab_get(lnid, include_alt=True) if lnid else []
    lens = (2, 3) if not only2tok else (2,)
    for n, langs in labs.items():
        for tokrbr, toks in enumerate(tokenise(n)):
            fn, ln, tip = '', '', ''
            if ',' in n and len(toks) == 2:
                toks = [a.strip(',') for a in reversed(toks)]

            if len(toks) in lens:
                tok0f = get_name_type(toks[0])
                tok1f = get_name_type(toks[-1])
                if toks[0] in fns and toks[-1] in lns:
                    fn, ln, tip = toks[0], toks[-1], 'w_fl'
                elif toks[0] in lns and toks[-1] in fns:
                    ln, fn, tip = toks[0], toks[-1], 'w_lf'
                elif not include_dict and toks[0] in fns and not tok1f:
                    fn, ln, tip = toks[0], toks[-1], 'w_f_none'
                elif not include_dict and toks[-1] in lns and not tok0f:
                    fn, ln, tip = toks[0], toks[-1], 'w_l_none'
                elif include_dict and toks[0] in fns and tok1f and tok1f < G:
                    fn, ln, tip = toks[0], toks[-1], 'w_f_d'
                elif include_dict and toks[-1] in lns and tok0f and tok0f > (1 - G):
                    fn, ln, tip = toks[0], toks[-1], 'w_l_d'
                elif include_dict and tok0f and tok0f > (1 - G) and tok1f and tok1f < G:
                    fn, ln, tip = toks[0], toks[-1], 'dict'
                elif include_dict and tok1f and tok1f > (1 - G) and tok0f and tok0f < G:
                    ln, fn, tip = toks[0], toks[-1], 'dict'
                elif include_dict and tok0f and tok0f > (1 - G) and not tok1f:
                    fn, ln, tip = toks[0], toks[-1], 'dict_l0'
                elif include_dict and not tok0f and tok1f and tok1f < G:
                    fn, ln, tip = toks[0], toks[-1], 'dict_f0'
                elif include_dict and tok0f and tok0f < G and not tok1f and fnln_freq(langs[0]) > 0.5:
                    ln, fn, tip = toks[0], toks[-1], 'dict_l0r'
                elif include_dict and not tok0f and tok1f and tok1f > (1 - G) and fnln_freq(langs[0]) > 0.5:
                    ln, fn, tip = toks[0], toks[-1], 'dict_f0r'
                else:
                    return []
            else:
                return []
            gender = 'u'
            if 'gender' in j and j['gender'] == ['WIKI_Q6581097']:
                gender = 'm'
            elif 'gender' in j and j['gender'] == ['WIKI_Q6581072']:
                gender = 'f'
            return [f'{qid}\t{n}\t{lang}\t{fn}\t{ln}\t{tip}\t{gender}\n' for lang in langs]


if __name__ == '__main__':
    print(fnln_freq('zh'))
    print(qid_lab_get(42, 'zh'), tokenise('毛泽东'), tokenise('佐治·華盛頓'), get_provenance('ประยุทธ์ จันทร์โอชา', 'th'))
    print(len(db), mat.shape, len(types), len(provi))
    print(get_name_type('เต๋อหัว'), get_name_type('davor'))
    p = Pool()

    pbar = tqdm(total=10_044_572)
    fi = open('/backup/wikidata/wiki_person.jsonl')
    fo = open('/backup/wikidata/fnln.txt', 'w')
    while True:
        lines = fi.readlines(10_000_000)
        if not lines:
            break
        rec = p.map(parseit, lines)
        for recl in rec:
            if recl:
                for l in recl:
                    if l:
                        fo.write(l)
        fo.flush()
        pbar.update(len(lines))

    fo.close()
