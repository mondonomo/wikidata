import re
import os
from pathlib import Path
import json
from collections import Counter
from langs_codes import wikilang2provenance

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = f'{BASE_DIR}/data'

zag = re.compile(' ?\([^\)]*\)')
remove_reg = re.compile('\(|\)|\.|\*|\?')
resplit = re.compile('/|\n')
retok = re.compile("[\u2008\u200B\u202F\u205F\u3000\s・·]+")
NON_SPACED_LANGUAGE_RANGES = (  # from T5
    '\u1000-\u104f',  # Burmese
    '\u4e00-\u9fff',  # CJK Unified Ideographs
    '\u3400-\u4dbf',  # CJK Unified Ideographs Extension A
    '\uf900-\ufaff',  # CJK Compatibility Ideographs
    '\u2e80-\u2eff',  # CJK Radicals Supplement
    '\u31c0-\u31ef',  # CJK Strokes
    '\u3000-\u303f',  # CJK Symbols and Punctuation
    '\u3040-\u309f',  # Japanese Hiragana
    '\u30a0-\u30ff',  # Japanese Katakana
    '\ua980-\ua9df',  # Javanese
    '\u1780-\u17ff',  # Khmer
    '\u19e0-\u19ff',  # Khmer Symbols
    '\u0e80-\u0eff',  # Lao
    '\u1980-\u19df',  # Tai Lue
    '\u1a20-\u1aaf',  # Tai Tham
    '\u0e00-\u0e7f',  # Thai
    '\u0f00-\u0fff',  # Tibetan
    '\uAC00-\uD7A3',  # Korean (!added)
)
non_spaced_re = re.compile(u'([{}])'.format(''.join(NON_SPACED_LANGUAGE_RANGES)))
cci, cci_id, langi, scripti, provi, tagi, tagsrc2tag, sourcei, sc2prov, detScript, s2f = \
    json.loads(open(os.path.join(BASE_DIR, DATA_DIR, 'referencedb.json'), 'rb').read())
detScript = {int(k): v for k, v in detScript.items()}


def cl(s:str) -> str:
    """
    clean wikidata labels
    """
    c = zag.sub('', s).strip()
    return c


def is_non_spaced(s:str) -> bool:
    if not s:
        return False
    return non_spaced_re.match(s) is not None


# ad hoc space tokenisation
def tokenise(s:str) -> list:
    toks = retok.split(s)
    if len(toks)>1:
        return [toks]
    elif is_non_spaced(s):
        toks = []
        for i in range(len(s)-1):
            toks.append([s[:i+1], s[i+1:]])
        return toks
    return []


def get_scripts(s:str) -> dict:
    r = Counter()
    for a in s:
        c = ord(a)
        if c in detScript:
            for sc in detScript[c]:
                r[sc] += 1
    uk = sum(r.values())
    r = {k: round(v / uk, 2) for k, v in r.most_common(n=3)}
    return r


# return all provenance matching wikidata lang and script
def get_provenance(text, wiki_lang, no_countries=False):
    if wiki_lang not in wikilang2provenance:
        return []
    scripts = get_scripts(text)
    provs = set()
    for prov in wikilang2provenance[wiki_lang]:
        lng, scr, cc = prov.split('_')
        if scr in scripts:
            if no_countries:
                provs.add(f'{lng}_{scr}_')
            else:
                provs.add(prov)
    return list(provs)


if __name__ == '__main__':
    print(hex(ord('태')))
    testt = ['佐治·華盛頓', 'Marko markovi', 'ジョージ・ワシントン', 'テミン', '태민', '李泰民', '李泰民']
    for s in testt:
        print(s, tokenise(s), retok.split(s))
    print(get_provenance('ประยุทธ์ จันทร์โอชา', 'th'), get_provenance('Putin', 'ru'))