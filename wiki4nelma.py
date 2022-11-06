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

print(get_wiki_cc({'country': ['Q161885','Q30'], 'birthplace': ['Q494413', 'Q216638'],
                   'deathplace': ['Q731635']} ),
        qid_lab_get(42, 'en').keys(), parse('davor lauc')['tags'][0])

fo = open('/projekti/mondodb_lm/wiki_train.tsv', 'w')
fot = open('/projekti/mondodb_lm/wiki_test.tsv', 'w')
fod = open('/projekti/mondodb_lm/wiki_dev.tsv', 'w')
#fo.write('qid\tname\tfn\tln\tdesc\tplace\tdob\timage\tsort\n')
uk = 0
for i, l in tqdm(enumerate(open('/backup/wikidata/wikinelma.jsonl')), total=24_969_448):
    j = json.loads(l)
    qid = int(j['wiki_id'][1:])
    tip = j['type']
    if tip == 'per':
        cc = get_wiki_cc({'country': j['country'], 'birthplace': j['birth_place'], 'deathplace': j['death_place'],
                          'language': j['native_language'], 'nationality': j['nationality']} )
    elif tip == 'loc':
        if j['wiki_id'] in q2cc:
            cc = q2cc[j['wiki_id']]
        else:
            cc = get_wiki_cc({'country': j['country'], 'headquarter': j['admin']} )
    elif tip == 'org':
        cc = get_wiki_cc({'country': j['country'], 'headquarter': j['headquarter']} )
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
    for l, lang in rows.items():
        prov = get_provenance(l, lang, no_countries=True)
        if prov:
            tow = f'{l}\t{tip} {prov[0]}\n'
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