# wiki lang to iso
import json
from collections import defaultdict, Counter
from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent
(wikil2cc, cc2lang, iso2w, w2iso) = json.load(open(os.path.join(BASE_DIR, 'data/wikilang2iso.json')))
cc2lang['UK'] = cc2lang['GB']
from wiki_location import q2cc


def load_from_airtable():
    raise NotImplementedError
    #  TODO - popraviti

    WIKI_DIR = '/backup/wikidata'
    api_key = open('airtable_key.txt').read()
    from pyairtable import Table, Api
    import sys

    sys.path.insert(0, '/projekti/nelma')
    from mondoDB.referencedb import provi

    lang2cc = defaultdict(Counter)
    for k, v in provi.items():
        lang, s, c = v['id'].split('_')
        bod = float(v['Percent'])*3 if 'Percent' in v else 0
        if 'Status' in v:
            if v['Status'] == 'official':
                bod += 1
            elif v['Status'] == 'official_regional':
                bod += 0.5
            elif v['Status'] == 'de_facto_official':
                bod += 0.9
            elif v['Status'] == 'romanized':
                bod += 0.2
            elif v['Status'] == 'foreign':
                bod += -0.5
        if bod>1.1:
            lang2cc[f'{lang}'][c] = max(bod, lang2cc[f'{lang}'][c])
    api = Api(api_key)
    w2iso = {t['fields']['WMF']: t['fields']['qid'] if 'qid' in t['fields'] else None for t in Table(api_key, 'appUZvAm9EHZgC1Eg', 'wiki_lang').all()}
    wiki2cc = defaultdict(Counter)
    for k, v in w2iso.items():
        if len(k) == 2:
            wiki2cc[v].update(lang2cc[k])
        elif k.count('-') == 1:
            a, b = k.split('-')
            if len(a) == 2 and len(b) == 2:
                wiki2cc[v][b.upper()] += 5
            elif len(a) == 2 and len(b) == 4:
                #wiki2cc[v].update(lang2cc[f'{k}_{b.uppser()}'])
                wiki2cc[v].update(lang2cc[a])
    wikil2cc = {k: {cc: round(100*v2/max(v.values())) for cc, v2 in v.items()} for k, v in wiki2cc.items() if len(v)>0 and max(v.values())>0}

    cc2lang = defaultdict(set)
    for lang, ccs in lang2cc.items():
        for cc in ccs:
            if ccs[cc]>1:
                cc2lang[cc].add(lang)
    cc2lang = {k: list(v) for k, v in cc2lang.items()
               }
    iso2w = {k : v for v, k in w2iso.items()}

    w2iso = {t['fields']['WMF']: t['fields']['iso3'] if 'iso3' in t['fields'] else None for t in
             Table(api_key, 'appUZvAm9EHZgC1Eg', 'wiki_lang').all()}
    json.dump((wikil2cc, cc2lang, iso2w, w2iso), open('data/wikilang2iso.json', 'w'))
    print(wikil2cc['Q1860']['US'], cc2lang['CH'])


def get_gid(w):
    w = w.upper()
    if w.startswith('WIKI_'):
        w = w[5:]
    elif w.startswith('Q'):
        w = w
    else:
        raise NotImplementedError
    return w


cc_weights = {'birthplace': 2, 'deathplace': 2, 'headquater':2, 'country': 3, 'nationality': 3, 'language': 1,
              'residence': 1, 'birth_place': 2, 'death_place': 2, 'positions': 1, 'educated_at': 1, 'works_at': 1,
              'headquarter': 3, 'workedu': 1, 'native_language': 1}


def get_wiki_cc(args):
    ccs = Counter()
    for typ, bps  in args.items():
        if typ in cc_weights:
            weight = cc_weights[typ]
        else:
            raise NotImplementedError
        if 'language' in typ:
            for lng in bps:
                lng = get_gid(lng)
                if lng in wikil2cc:
                    for cl, bod in wikil2cc[lng].items():
                        ccs[cl] += bod / 100
        else:
            for bp in bps:
                qid = get_gid(bp)
                if qid in q2cc:
                    ccs[q2cc[qid]] += weight
    cc, weight = ccs.most_common()[0] if len(ccs)>0 else ('', 0)
    return cc, weight


if __name__ == '__main__':
    if False:
        load_from_airtable()
        (wikil2cc, cc2lang, iso2w, w2iso) = json.load(open('data/wikilang2iso.json', 'r'))
    print(wikil2cc['Q1860']['US'], cc2lang['CH'], w2iso['hr'])
    print(get_wiki_cc({'country': ['Q161885','Q30'], 'birthplace': ['Q494413', 'Q216638'],
                       'deathplace': ['Q731635']} ))
