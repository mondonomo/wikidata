import sys
sys.path.insert(0, '/projekti/mondoAPI')
sys.path.insert(0, '/projekti/wikidata')

import json
from collections import Counter, defaultdict
from wiki_labels import qid_lab_get
from wikilang2iso import get_wiki_cc, iso2w, cc2lang, q2cc
from text_utils import cl
from pnu.parse import parse
from text_utils import get_provenance
from api.db import db
from tqdm import tqdm
import random
from json import loads
from multiprocessing import Pool
print(get_wiki_cc({'country': ['Q161885','Q30'], 'birthplace': ['Q494413', 'Q216638'],
                   'deathplace': ['Q731635']} ),
        qid_lab_get(42, 'en').keys(), parse('davor lauc')['tags'][0])


def proc(l):
    j = loads(l)
    qid = int(j['wiki_id'][1:])
    tip = j['type']
    if tip == 'per':
        cc, ccs = get_wiki_cc({k: v for k, v in j in k in cc_weights})
        if j['gender'] == ['WIKI_Q6581097']:
            tip = 'per_1'
        elif j['gender'] == ['WIKI_Q6581072']:
            tip = 'per_2'
    elif tip == 'loc':
        if j['wiki_id'] in q2cc:
            cc = q2cc[j['wiki_id']]
        else:
            cc = get_wiki_cc({'country': j['country'], 'headquarter': j['admin']})
    elif tip == 'org':
        cc, ccs = get_wiki_cc({'country': j['country'], 'headquarter': j['headquarter']})
    else:
        raise NotImplementedError

    names = []
    if 'native_language' in j and j['native_language']:
        langs = Counter([iso2w[q[5:]] for q in j['native_language'] if q[5:] in iso2w])
    else:
        langs = Counter()
    if cc and cc in cc2lang:
        langs.update(cc2lang[cc])
    rows = {}
    for lang, f in langs.most_common():
        for l in qid_lab_get(qid, lang, False):
            if l not in rows:
                rows[l] = lang
        for l in qid_lab_get(qid, lang, True):
            if l not in rows:
                rows[l] = lang
    rec = []
    for l, lang in rows.items():
        prov = get_provenance(l, lang, no_countries=True)
        if prov and l and len(l) > 1 and prov[0] and tip:
            rec.append(f'{l}\t{tip} {prov[0]}\n')
    return rec


if __name__ == '__main__':

    fo = open('/projekti/mondodb_lm/wiki_train.tsv', 'w')
    fot = open('/projekti/mondodb_lm/wiki_test.tsv', 'w')
    fod = open('/projekti/mondodb_lm/wiki_dev.tsv', 'w')
    #fo.write('qid\tname\tfn\tln\tdesc\tplace\tdob\timage\tsort\n')
    uk = 0

    p = Pool(10)

    batch = []
    lines = open('/backup/wikidata/wikinelma.jsonl').readlines()
    for i, l in tqdm(enumerate(lines), total=len(lines)):
        batch.append(l)
        if len(batch) > 1024 or i+1 == len(lines):
            recs = p.map(proc, batch)
            batch = []
            for rec in recs:
                for tow in rec:
                    r = random.random()
                    if r < .01:
                        fot.write(tow)
                    elif r < .02:
                        fod.write(tow)
                    else:
                        fo.write(tow)
    fo.close()
    fod.close()
    fot.close()