import json
from multiprocessing import Pool
import gzip
from tqdm import tqdm
from data.wiki_types import wikiorg
BASE_DIR = '/backup/wikidata'
from data.wiki_types import wikiloc

vals = [{'name': 'industry', 'props': ['P452']}, {'name': 'location', 'props': ['P159', 'P17']},
        {'name': 'names', 'props': ['P1448', 'P1813']}, {'name': 'web', 'props': ['P856']},
        {'name': 'ids', 'props': ['P1278', 'P4264']},
        ]


def extract(line):
    l = line.decode("utf-8").strip(',\r\n ')
    try:
        l = json.loads(l)
    except Exception as e:
        print(e)
        return None
    if l['type'] != 'item' or l['id'][0] != 'Q' or 'claims' not in l:
        return None
    org = False
    if 'P31' in l['claims']:
        for a in l['claims']['P31']:
            if 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in wikiorg:
                org = True
                break

    wiki_ent = None
    if org:
        wiki_ent = {'id': l['id'] }
        for v in vals:
            wiki_ent[v['name']] = []
            for pid in v['props']:
                if pid in l['claims']:
                    for s in l['claims'][pid]:
                        if 'datavalue' in s['mainsnak']:
                            if s['mainsnak']['datavalue']['type'] in ['monolingualtext']:
                                wiki_ent[v['name']].append(s['mainsnak']['datavalue']['value']['text'])
                            elif s['mainsnak']['datavalue']['type'] in ['string']:
                                wiki_ent[v['name']].append(s['mainsnak']['datavalue']['value'])
                            elif s['mainsnak']['datavalue']['type'] in ['wikibase-entityid']:
                                wiki_ent[v['name']].append(s['mainsnak']['datavalue']['value']['id'])

    return json.dumps(wiki_ent) if wiki_ent else None



if __name__ == '__main__':
    TEST = False
    pmap = Pool(40)
    BATCH_SIZE = 4_000_000_000 if not TEST else 100_000
    fin = gzip.open(f'{BASE_DIR}/latest-all.json.gz', 'rb')
    fin.read(2)  # skip first two bytes: "{\n"

    fo = open(f'{BASE_DIR}/wiki_org.jsonl', 'w')
    pbar = tqdm(fin, total=102_364_283)
    while True:
        lines = fin.readlines(BATCH_SIZE)
        if len(lines) == 0:
            break
        rec = pmap.map(extract, lines) if not TEST else map(extract, lines)
        # print(len(list(rec)), len(lines))
        for l in rec:
            if l:
                fo.write(l+'\n')
        pbar.update(len(lines))
        if TEST:
            break



