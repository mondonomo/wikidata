# wikizemlja
import gzip, io
import json
import re
from datetime import datetime
# !pip install orjson
import orjson
from multiprocessing import Pool
from wiki_ent_ids import wikiloc, wikiln, wikifn, wikiorg, wiki_title
import pickle
from tqdm import tqdm

BASE_DIR = '/backup/wikidata'

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')
from collections import defaultdict

ONLY_LABELS = True
COMPRESSED = True

fob=open('wikidata_bad.txt', 'w')


def cl(s):
    c = zag.sub('', s).strip()
    return c

def extractLabels(l):
    labelitems = defaultdict(list)
    if 'labels' in l:
        for k, v in dict(l['labels'].items()).items():
            nl = cl(v['value'])
            labelitems[nl].append(('l',v['language']))

    if 'aliases' in l:
        for k, v in dict(l['aliases']).items():
            for v2 in v:
                nl = cl(v2['value'])
                labelitems[nl].append(('a', v2['language']))

    if 'P1705' in l['claims']:
        for s in l['claims']['P1705']:
            if 'datavalue' in s['mainsnak']:
                if s['mainsnak']['datavalue']['type'] in ['monolingualtext']:
                    nl = cl(s['mainsnak']['datavalue']['value']['text'])
                    labelitems[nl].append(('n',''))

    rec = {r: list(v) for r, v in labelitems.items() if r and len(r) > 0}
    return rec


def processw(line, onlyLabels=ONLY_LABELS):
    wiki_ent = {}
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
    t = None
    ent_type = None
    gname = False
    lname = False
    lprefix = False
    ltitle = False
    wname = False
    wikil = None
    if not onlyLabels and 'claims' in l:
        if 'P31' in l['claims']:
            for a in l['claims']['P31']:
                if 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] == 'Q5':  # person
                    ent_type = 'per'
                elif ('datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in wikiloc) or 'P17' in l['claims']:
                    ent_type = 'loc'
                elif 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in wikiorg:
                    ent_type = 'org'
                elif 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in wikifn:  # given name
                    gname = True
                elif 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in ['Q2620828',
                                                                                                  'Q66475447',
                                                                                                  'Q16591923']:  # de
                    lprefix = True
                elif 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in wikiln:
                    lname = True
                elif 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in wiki_title:
                    wikil = True
                elif 'datavalue' in a['mainsnak'] and a['mainsnak']['datavalue']['value']['id'] in ['Q96477712']:
                    wname = True
        if 'P1566' in l['claims'] or 'P1376' in l['claims'] or 'P281' in l['claims'] or 'P1082' in l['claims']:
            vals = [{'name': 'country', 'props': ['P17', 'P159']}, {'name':'todayin', 'props': ['P3842']},  {'name': 'admin', 'props': ['P131']}, {'name': 'geonameid', 'props': ['P1566']}, {'name': 'population', 'props': ['P1082']}]
            wiki_ent['id'] = l['id']
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
                                elif s['mainsnak']['datavalue']['type'] in ['quantity']:
                                    wiki_ent[v['name']].append(s['mainsnak']['datavalue']['value']['amount'])
            #fo_geo.write(json.dumps(wiki_ent)+'\n')


        if ent_type:
            if ent_type=='per':
                vals = [{'name': 'fn', 'props': ['P735']}, {'name': 'ln', 'props': ['P734', 'P1950']},
                        {'name': 'pat', 'props': ['P5056']},
                        {'name': 'gender', 'props': ['P21']},
                        {'name': 'nick', 'props': ['P1449', 'P742', 'P1787', 'P1813']},
                        {'name': 'title', 'props': ['P39', 'P410', 'P511', 'P97', 'P410', 'P468', 'P512']},
                        {'name': 'sufix', 'props': ['P1035']}, {'name': 'positions', 'props': ['P39', 'P106', 'P8413']},
                        {'name': 'country', 'props': ['P27']}, {'name': 'birth_place', 'props': ['P19']},
                        {'name': 'name_native', 'props': ['P1559']}, {'name': 'name_born', 'props': ['P1477']},
                        {'name': 'dob', 'props': ['P569']},
                        {'name': 'picture', 'props': ['P18']},
                        {'name': 'affiliation', 'props': ['P69', 'P108', 'P937']},
                        ]
            elif ent_type=='org':
                vals = [{'name': 'country', 'props': ['P17']}, {'name': 'legal_form', 'props': ['P1454']},
                        {'name': 'headquarter', 'props': ['P159']},
                        ]
            elif ent_type=='loc':
                vals = [{'name': 'country', 'props': ['P17', 'P159']}, {'name': 'geonames', 'props': ['P1566']},
                        ]
            else:
                raise NotImplemented
            wiki_ent['type'] = ent_type
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

        if gname or lname or lprefix or wiki_title or wname or wikil:
            vals = [{'name': 'instance', 'props': ['P31']}, {'name': 'variants', 'props': ['P460']},
                    {'name': 'oposite_gender', 'props': ['P1560']},
                    {'name': 'nickname', 'props': ['P1449', 'P468', 'P512']},
                    {'name': 'soundex', 'props': ['P3878']}, {'name': 'cologne', 'props': ['P3879']},
                    {'name': 'native', 'props': ['P1705']},
                    {'name': 'transliteration', 'props': ['P2440']},
                    {'name': 'short_name', 'props': ['P1813']},
                    ]
            if gname:
                t = 'N.FN'
            elif lname:
                t = 'N.LN'
            elif lprefix:
                t = 'N.LN.PREF'
            elif wiki_title:
                t = 'N.TITLE'
            wikiname = {'type': t}
            for v in vals:
                wikiname[v['name']] = []
                for pid in v['props']:
                    if pid in l['claims']:
                        for s in l['claims'][pid]:
                            if 'datavalue' in s['mainsnak']:
                                if s['mainsnak']['datavalue']['type'] in ['monolingualtext']:
                                    wikiname[v['name']].append(s['mainsnak']['datavalue']['value']['text'])
                                elif s['mainsnak']['datavalue']['type'] in ['string']:
                                    wikiname[v['name']].append(s['mainsnak']['datavalue']['value'])
                                elif s['mainsnak']['datavalue']['type'] in ['wikibase-entityid']:
                                    wikiname[v['name']].append('WIKI_' + s['mainsnak']['datavalue']['value']['id'])

    wikil = extractLabels(l)
    return (wikiid, wikil, wiki_ent, wikiname, ent_type != None, gname or lname or lprefix)


if __name__ == '__main__':
    if True:
        # wikidata osobe
        if COMPRESSED:
            fin = gzip.open(f'{BASE_DIR}/latest-all.json.gz', 'rb')
            #fin = io.BufferedReader(fing, buffer_size=1024*100)
            fin.read(2)  # skip first two bytes: "{\n"
        else:
            fin = open(f'{BASE_DIR}/latest-all.json')
            fin.readline() # prvi razmak
        p = Pool(10)
        wikinelma_all = gzip.open(f'{BASE_DIR}/wikinelma.jsonl.gz', 'wt')
        wikiname_all = gzip.open(f'{BASE_DIR}/wikiname.jsonl.gz', 'wt')
        wikil_all = gzip.open(f'{BASE_DIR}/wikil.jsonl.gz', 'wt')
        wiki_loc = gzip.open(f'{BASE_DIR}/wikiloc.jsonl.gz', 'wt')

        start = datetime.now()
        br, brr = 0, 0
        tmp = []

        print('počinjem ...')

        gotovo = False
        pbar = tqdm(total= 100_000_000)

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
            for id, wl, wp, wn, isent, isname in r:
                if wl:
                    wl['wiki_id'] = id
                    wikil_all.write(orjson.dumps(wl, option=orjson.OPT_APPEND_NEWLINE))
                if isent:
                    wp['wiki_id'] = id
                    wikinelma_all.write(orjson.dumps(wp, option=orjson.OPT_APPEND_NEWLINE))
                if isname:
                    wn['wiki_id'] = id
                    wikiname_all.write(orjson.dumps(wn, option=orjson.OPT_APPEND_NEWLINE))

            # print(wikiperson_all); break
            tmp = []

            #if len(wikil_all) != brr:
            #    print(len(wikil_all), brr)
            #fo_geo.flush()
            pbar.update(len(lines))
        wikinelma_all.close()
        wikiname_all.close()
        wikil_all.close()
        wiki_loc.close()

    if False:
        print('writing nelma labels')

        req = re.compile('Q[0-9]+')
        fns = ['wiki_loc.jsonl', 'wikiname.jsonl', 'wikinelma.jsonl']
        import msgpack

        labels = {}
        qus = set()
        for fn in fns:
            for l in open(f'{BASE_DIR}/{fn}'):
                qs = req.findall(l)
                qus.update(qs)
        len(qus)
        for l in tqdm(open(f'{BASE_DIR}/wikil.jsonl'), total=105_444_694):
            j = orjson.loads(l)
            wikiid = j['wiki_id']
            if wikiid in qus:
                j.pop('wiki_id')
                wikiid = int(wikiid[1:])
                labels[wikiid] = msgpack.packb(j, use_bin_type=True)
        pickle.dump(labels, open(f'{BASE_DIR}/wikil_nelma.pickle', 'wb'))
