import orjson
import marisa_trie
from multiprocessing import Pool
from data.wiki_types import wikiloc, wikiln, wikifn, wikiorg, wiki_title
import gzip
from datetime import datetime
from tqdm import tqdm
import re
from collections import defaultdict

BASE_DIR = '/backup/wikidata'

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')

ONLY_LABELS = False
COMPRESSED = True

fob = open('wikidata_bad.txt', 'w')

def cl(s):
    c = zag.sub('', s).strip()
    return c


def get_types():
    types = defaultdict(set)
    for l in open(f'{BASE_DIR}/wiki_occupation_instance_esco.tsv'):
        qid, lab, fr = l.strip('\n').split('\t')
        qid = qid.replace('http://www.wikidata.org/entity/', '')
        types[('P31', qid)].add('o')
    for l in open(f'{BASE_DIR}/wiki_occupation_subclass_esco.tsv'):
        qid, lab, fr = l.strip('\n').split('\t')
        qid = qid.replace('http://www.wikidata.org/entity/', '')
        types[('P279', qid)].add('o')
    for l in open(f'{BASE_DIR}/wiki_skills_instance_esco.tsv'):
        qid, lab, fr = l.strip('\n').split('\t')
        qid = qid.replace('http://www.wikidata.org/entity/', '')
        types[('P31', qid)].add('s')
    for l in open(f'{BASE_DIR}/wiki_skills_subclass_esco.tsv'):
        qid, lab, fr = l.strip('\n').split('\t')
        qid = qid.replace('http://www.wikidata.org/entity/', '')
        types[('P279', qid)].add('s')
    return types


def processw(line):
    l = line.decode("utf-8").strip(',\r\n ')
    try:
        l = orjson.loads(l)  # orjson
    except Exception as e:
        ukj = l.count('}{')
        print(e, ukj)
        if ukj==1:
            l1, l2 = l.split('}{')
            fob.write(l1+'}\n')
            fob.write('{'+l2 + '\n')
        else:
            print('BAD', l)
        return None
    wiki_ent = {'wiki_id': l['id']}
    t = None
    ent_type = set()
    if 'claims' in l:
        for P in ('P31', 'P279'):
            if P in l['claims']:
                for a in l['claims'][P]:
                    if 'datavalue' in a['mainsnak']:
                        key = (P, a['mainsnak']['datavalue']['value']['id'])
                        if key in types:
                            ent_type.update(types[key])

    if len(ent_type) > 0:
        vals = [{'name': 'fields', 'props': ['P425']},
                {'name': 'studies', 'props': ['P2578']},
                {'name': 'acm_code', 'props': ['P2179']},
                {'name': 'product', 'props': ['P1056']},
                {'name': 'gnd_id', 'props': ['P227']},
                {'name': 'ideo_id', 'props': ['P1043']},
                {'name': 'esco_occupation_id', 'props': ['P4652']},
                {'name': 'esco_skill_id', 'props': ['P4644']},
                {'name': 'soc_code', 'props': ['P919']},
                {'name': 'babelnet_id', 'props': ['P2581']},
                {'name': 'openalex_id', 'props': ['P10283']},
                {'name': 'wordnet_id', 'props': ['P8814']}]

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
                                wiki_ent[v['name']].append('WIKI_' + s['mainsnak']['datavalue']['value']['id'])
                            elif s['mainsnak']['datavalue']['type'] in ['time']:
                                wiki_ent[v['name']].append(s['mainsnak']['datavalue']['value']['time'][1:11])
                        else:
                            pass

    return wiki_ent


if __name__ == '__main__':

    fin = gzip.open(f'{BASE_DIR}/latest-all.json.gz', 'rb')
    fin.read(2)  # skip first two bytes: "{\n"
    occup = open(f'{BASE_DIR}/wiki_occup_skill.jsonl', 'wb')

    print('počinjem ...')

    types = get_types()

    gotovo = False
    pbar = tqdm(total=100_709_722)
    br, brr = 0, 0

    p = Pool(10)

    while not gotovo:
        lines = fin.readlines(10_000_000_000)
        br+=1
        brr += len(lines)
        #if br % 100 == 0:
        #    print(br, brr, datetime.now() - start)

        if len(lines)==0:
            gotovo = True # još samo loši
            fob.close()
            lines = open('wikidata_bad.txt').readlines()

        r = [a for a in p.map(processw, lines) if a]
        for oc in r:
            occup.write(orjson.dumps(oc, option=orjson.OPT_APPEND_NEWLINE))
        pbar.update(len(lines))
    occup.close()
