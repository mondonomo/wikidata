import logging
import sys
sys.path.insert(0, '/projekti/mondoAPI')
import gzip
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq

from wiki_labels import qid_lab_get
from wikilang2iso import get_wiki_cc, iso2w, cc2lang, q2cc, cc_weights
# from api.db import db
from pnu.detect_lang_scr import get_script
from pnu.parse_dict import  spans_to_tags, tag_set, tag_to_char, NameParser

from pnu.do_tokenize import do_tokenize
from tqdm import tqdm
from json import loads
from multiprocessing import Pool

logging.basicConfig(filename='wiki4nelma_parsing.log', encoding='utf-8', level=logging.DEBUG, filemode='w')

DO_SAMPLE = False

parser = NameParser()


nelma_schema = pa.schema([pa.field("name", pa.string()),
                      pa.field("type", pa.string()),
                      pa.field("cc", pa.string()),
                      pa.field("lang", pa.string()),
                      pa.field("script", pa.string()),
                      pa.field("tags", pa.string()),
                      ])



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
        langs = Counter([iso2w[q[5:]][:2] for q in j['native_language'] if q[5:] in iso2w])
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

    name_parts = {}
    if tip[:3] == 'per':
        for tip, wiki_ids in j.items():
            if tip in ('position', 'sufix', 'affiliation'):
                tip = 'title'
            if tip in tag_set:
                for wiki_id in wiki_ids:
                    for label in qid_lab_get(int(wiki_id[6:])):
                        if label not in name_parts:
                            name_parts[label] = tip
                        elif name_parts[label] != tip:  # ambigous
                            name_parts.pop(label)
                        if tip == 'fn' and len(label) > 1:
                            short_name = label[0] + '.'
                            name_parts[short_name] = tip
    rec = []
    for name, v1 in names.items():

        if v1[0][:3] == 'per' and name_parts:
            # parse
            tags = ''
            if name_parts:
                try:
                    tags = parser.parse_known_parts(name, name_parts)
                except Exception as e:
                    logging.error(name + str(name_parts) + str(e))
                    tags = ''
                try:
                    tags = spans_to_tags(name, tags) if tags else ''
                except Exception as e:
                    logging.error('spans '+name+'\n'+str(name_parts)+'\n'+str(tags) + str(e))
                    raise

            if not tags:
                parsed = parser.parse_name(name, limit_to_lang=v1[2], ignore_sequence=True, parse_spaced=False)
                if parsed:
                    for parsed1 in parsed:
                        name_parts2 = {a[0]: a[1].split('_')[-1] for a in parsed1[1]}
                        name_tags = tuple(a[1] for a in parsed[0][1])
                        if parsed1[0] > 0.05 or len(set(name_parts.items()) & set(name_parts2.items()))>0:
                            if name_tags in parser.final_seq:
                                tags = parser.parse_known_parts(name, name_parts2)
                                tags = spans_to_tags(name, tags)
                                logging.info(f'parsed {name} to {" ".join(name_tags)}')
                                break
                        if parsed1[0] < 0.1:
                            break
                if not tags:
                    logging.info(f'NOT PARSED {name}')

        elif v1[0][:3] in ('org', 'loc'):
            toks, spans = do_tokenize(name, v1[2])
            tags = ['0']*len(name)
            for bs, es in spans:
                tags[bs] = tag_to_char[(v1[0][:3], True)]
                tags[bs+1:es] = tag_to_char[(v1[0][:3], False)] * (es-bs-1)
            tags = ''.join(tags)
        else:
            tags = ''

        rec.append({'name': name, 'tags': tags, 'type': v1[0], 'cc': v1[1], 'lang': v1[2], 'script': v1[3]})

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

    DEBUG = True
    batch = []
    BS = 10_000


    lenlines = 26852740
    br = 0
    prog = tqdm(total=lenlines)
    for i, l in enumerate(gzip.open('/backup/wikidata/wikinelma.jsonl.gz', 'rt')):
        batch.append(l)
        if len(batch) > BS or i+1 == lenlines:
            #recs = p.map(proc, batch)
            if not DEBUG:
                recs = p.map(proc, batch)
            else:
                recs = [proc(a) for a in batch]

            batch_d = {k.name: [] for k in nelma_schema}
            for rec_batch in recs:
                for row_dict in rec_batch:
                    for k, v in row_dict.items():
                        batch_d[k].append(v)

            t = pa.Table.from_arrays([pa.array(batch_d['name']),
                                      pa.array(batch_d['type'], type=pa.string()),
                                      pa.array(batch_d['cc'], type=pa.string()),
                                      pa.array(batch_d['lang'], type=pa.string()),
                                      pa.array(batch_d['script'], type=pa.string()),
                                      # pa.DictionaryArray.from_arrays(pa.array(batch_d['type'], type=pa.string()), types_d),
                                      # pa.DictionaryArray.from_arrays(pa.array(batch_d['cc'], type=pa.string()), cc_d),
                                      # pa.DictionaryArray.from_arrays(pa.array(batch_d['lang'], type=pa.string()), lang_d),
                                      # pa.DictionaryArray.from_arrays(pa.array(batch_d['script'], type=pa.string()), script_d),
                                      pa.array(batch_d['tags']),
                                      ], schema=nelma_schema)
            writer = pq.ParquetWriter(f"/projekti/mondodb_lm/wiki_{br}.parquet", nelma_schema)
            writer.write(t)
            writer.close()
            br += 1
            prog.update(len(BATCH))
            batch = []
            if DEBUG and br > 2:
                break
