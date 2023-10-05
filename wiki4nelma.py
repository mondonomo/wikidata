import logging
import sys
sys.path.insert(0, '/projekti/mondoAPI')
sys.path.insert(0, '/projekti/nelma')
#
import gzip
from collections import Counter, defaultdict
import pyarrow as pa
import pyarrow.parquet as pq

from wiki_labels import qid_lab_get
from wikilang2iso import get_wiki_cc, iso2w, cc2lang, q2cc, cc_weights, w2iso
# from pnu.parse import parse
# from api.db import db
from pnu.detect_lang_scr import get_script
from pnu.parse import parse_known_parts, parse, spans_to_tags, tag_set
from model.dataset import nelma_schema, types_d, cc_d, lang_d, script_d, types_i, cc_i, lang_i, script_i
from tqdm import tqdm
import random
from json import loads
from multiprocessing import Pool
from wiki_trie_ents import extractLabels
from random import random

logging.basicConfig(filename='wiki4melma.log', encoding='utf-8', level=logging.DEBUG)

DO_SAMPLE = True

def proc(lng):
    j = loads(lng)
    qid = int(j['wiki_id'][1:])
    #if qid != 61533472:
    #    return {}
    tip = j['type']
    if tip == 'per':
        cc, cc_weight = get_wiki_cc({k: v1 for k, v1 in j.items() if k in cc_weights})
        if j['gender'] == ['WIKI_Q6581097']:
            tip = 'per_1'
        elif j['gender'] == ['WIKI_Q6581072']:
            tip = 'per_2'
    elif tip == 'loc':
        if j['wiki_id'] in q2cc:
            cc = q2cc[j['wiki_id']]
        else:
            cc, cc_weight = get_wiki_cc({'country': j['country'], 'headquarter': j['admin']})
    elif tip == 'org':
        cc, cc_weight = get_wiki_cc({'country': j['country'], 'headquarter': j['headquarter']})
    else:
        raise NotImplementedError
    if not cc:
        return []
    if cc == 'UK':
        cc = 'GB'

    native_lang = None
    if 'native_language' in j and j['native_language']:
        langs = Counter([iso2w[q[5:]][:2] for q in j['native_language'] if q[5:] in iso2w and iso2w[q[5:]][:2] in lang_i])
        native_lang = langs.most_common()[0][0] if langs else ''
    else:
        langs = Counter()
    langs.update(cc2lang[cc])
    names = {}
    lng_max = 0
    used_langs = set()
    for lng, f in langs.most_common():
        # name_labels = qid_lab_get(qid, lng, include_alt=True)
        if lng in j['labels']:
            name_labels = j['labels'][lng]
            for name in name_labels:
                if name not in names:
                    scr = get_script(name)
                    if tip and cc and lng and scr:
                        names[name] = (tip, cc, lng, scr)
        if f < lng_max:
            break
        else:
            lng_max = f
        used_langs.add(lng)
    if 'name_native' in j and j['name_native'] and j['name_native'][0] not in names:
        name = j['name_native'][0]
        scr = get_script(name)
        if native_lang:
            lng = native_lang
        if tip and cc and lng and scr:
            names[name] = (tip, cc, lng, scr)

    rec = []
    for name, v1 in names.items():

        if v1[0][:3] == 'per' and (not DO_SAMPLE or random() < 0.01):
            # parse
            name_parts = {}
            for tip, wiki_ids in j.items():
                if tip in ('position', 'sufix', 'affiliation'):
                    tip = 'title'
                if tip in tag_set:
                    for wiki_id in wiki_ids:
                        for label in qid_lab_get(int(wiki_id[6:])):
                            if label not in name_parts:
                                name_parts[label] = tip
                            elif name_parts[label] != tip:  #  ambigous
                                name_parts.pop(label)
                            if tip == 'fn' and len(label)>1:
                                short_name = label[0]+'.'
                                name_parts[short_name] = tip
            if name_parts:
                name = name.lower().strip()
                try:
                    tags = parse_known_parts(name, name_parts)
                except Exception as e:
                    logging.error(name + str(name_parts) + str(e))
                    tags = ''
                if name_parts and tags == '':
                    logging.debug('empty' + name + str(name_parts))
                tags = spans_to_tags(tags) if tags else ''

            else:
                tags = '' # TODO  parse ?
        else:
            tags = ''
        rec.append({'name': name, 'tags': tags, 'type': types_i[v1[0]], 'cc': cc_i[v1[1]], 'lang': lang_i[v1[2]], 'script': script_i[v1[3]]})

    return rec


if __name__ == '__main__':

    if False:
        print(get_wiki_cc({'country': ['Q161885', 'Q30'], 'birthplace': ['Q494413', 'Q216638'],
                           'deathplace': ['Q731635']}),
              qid_lab_get(42, 'en').keys(),
              #               parse('davor lauc')['tags'][0]
              )

    uk = 0

    p = Pool()

    batch = []
    BS = 100_000

    lines = gzip.open('/backup/wikidata/wikinelma.jsonl.gz', 'rt').readlines()
   # lines = gzip.open('/Users/davor/Downloads/wikinelma.jsonl.gz', 'rt').readlines()
    writer = pq.ParquetWriter("/projekti/mondodb_lm/wiki.parquet", nelma_schema)
    # writer = pq.ParquetWriter("/Users/davor/Downloads/wiki.parquet", nelma_schema)
    for i, l in tqdm(enumerate(lines), total=len(lines)):
        batch.append(l)
        if len(batch) > BS or i+1 == len(lines):
            recs = p.map(proc, batch)
            batch_d = {k.name: [] for k in nelma_schema}
            for rec_batch in recs:
                for row_dict in rec_batch:
                    for k, v in row_dict.items():
                        batch_d[k].append(v)

            t = pa.Table.from_arrays([pa.array(batch_d['name']),
                                      pa.DictionaryArray.from_arrays(pa.array(batch_d['type'], type=pa.uint8()), types_d),
                                      pa.DictionaryArray.from_arrays(pa.array(batch_d['cc'], type=pa.uint8()), cc_d),
                                      pa.DictionaryArray.from_arrays(pa.array(batch_d['lang'], type=pa.uint8()), lang_d),
                                      pa.DictionaryArray.from_arrays(pa.array(batch_d['script'], type=pa.uint8()), script_d),
                                      pa.array(batch_d['tags']),
                                      ], schema=nelma_schema)
            writer.write(t)
            batch = []
            #break
    writer.close()
