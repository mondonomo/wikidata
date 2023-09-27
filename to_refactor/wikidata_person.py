# wikizemlja
import gzip, io
import json
import re
from datetime import datetime
# !pip install orjson
import orjson
from multiprocessing import Pool
import pickle
import tqdm

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')
from collections import defaultdict

ONLY_LABELS = False
COMPRESSED = True

fob=open('wikidata_bad.txt', 'w')

vals = [{'name': 'fn', 'props': ['P735']}, {'name': 'ln', 'props': ['P734', 'P1950']},
        {'name': 'pat', 'props': ['P5056']},
        {'name': 'gender', 'props': ['P21']},
        {'name': 'nick', 'props': ['P1449', 'P742', 'P1787']},
        {'name': 'title', 'props': ['P39', 'P410', 'P511', 'P97', 'P410', 'P468', 'P512']},
        {'name': 'sufix', 'props': ['P1035']}, {'name': 'positions', 'props': ['P39', 'P106', 'P8413']},
        {'name': 'country residence', 'props': ['P27', 'P551']}, {'name': 'birth_place', 'props': ['P19']},
{'name': 'place of death burial', 'props': ['P20', 'P119']}, {'name': 'educated at', 'props': ['P69']},
{'name': 'work location', 'props': ['P937']}, {'name': 'employer', 'props': ['P108']},

        {'name': 'language', 'props': ['P103','P1412']},
        {'name': 'name_native', 'props': ['P1559']}, {'name': 'name_born', 'props': ['P1477']},
        {'name': 'dob', 'props': ['P569']},
        {'name': 'picture', 'props': ['P18']},
        ]


def processw(line, onlyLabels=ONLY_LABELS):

    wikiname = {}

    if COMPRESSED:
        l = line.decode("utf-8").strip(',\r\n ')
    else:
        l = line.strip(',\r\n ')
    # l = json.loads(l)
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
    wikiid = l['id']

    wiki_ent = None
    if 'claims' in l:
        ent_type = ''
        if 'P31' in l['claims']:
            for a in l['claims']['P31']:
                if 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] == 'Q5':  # person
                    ent_type = 'per'
        if ent_type=='per':
            wiki_ent = {'id': wikiid}
            labelitems = defaultdict(list)
            if 'labels' in l:
                for k, v in dict(l['labels'].items()).items():
                    labelitems[k].append(v['value'])
            wiki_ent['l'] = labelitems

            aliasitems = defaultdict(list)
            if 'aliases' in l:
                for k, v in dict(l['aliases']).items():
                    for v2 in v:
                        aliasitems[k].append(v2['value'])
            wiki_ent['a'] = aliasitems

            descitems = defaultdict(list)
            if 'descriptions' in l:
                for k, v in dict(l['descriptions']).items():
                    descitems[k].append(v['value'])
            wiki_ent['desc'] = descitems

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
            wiki_ent['sitelinks'] = list(l['sitelinks'])
    return wiki_ent


if __name__ == '__main__':
    # wikidata osobe
    if COMPRESSED:
        fing = gzip.open(r'/backup/wikidata/latest-all.json.gz', 'rb')
        fin = io.BufferedReader(fing, buffer_size=1024 ** 2)
        fin.read(2)  # skip first two bytes: "{\n"
    else:
        fin = open('/projekti/data/latest-all.json')
        fin.readline() # prvi razmak
    p = Pool(16)
    wikinelma_all = {}

    start = datetime.now()
    br, brr = 0, 0
    tmp = []

    print('počinjem ...')

    gotovo = False
    fo = gzip.open('/backup/wikidata/wiki_person.jsonl.gz', 'wt')
    pbar = tqdm.tqdm(total=95_000_000)
    while not gotovo:
        lines = fin.readlines(1_000_000_000)
        br += 1
        brr += len(lines)
        pbar.update(len(lines))

        if len(lines) == 0:
            gotovo = True # još samo loši
            fob.close()
            lines = open('wikidata_bad.txt').readlines()

        #r = [a for a in p.map(processw, lines) if a]
        r = [a for a in p.map(processw, lines) if a]
        for v in r:
            fo.write(json.dumps(v)+'\n')

    fo.close()
