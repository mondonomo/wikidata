import re

zag = re.compile(' ?\([^\)]*\)')
remove_reg = re.compile('\(|\)|\.|\*|\?')
resplit = re.compile('/|\n')
retok = re.compile("[\u200B\u202F\u205F\u3000\s・]+")

NON_SPACED_LANGUAGE_RANGES = ( # from T5
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
    '\uAC00-\uD7A3',  # Korean (added)
)
non_spaced_re = re.compile(u'([{}])'.format(''.join(NON_SPACED_LANGUAGE_RANGES)))


# clean wikidata label
def cl(s):
    c = zag.sub('', s).strip()
    return c


def is_non_spaced(s):
    if not s:
        return False
    return non_spaced_re.match(s) is not None

# ad hoc space tokenisation
def tokenise(s):
    toks = retok.split(s)
    if len(toks)>1:
        return [toks]
    elif is_non_spaced(s):
        toks = []
        for i in range(len(s)-1):
            toks.append([s[:i+1], s[i+1:]])
        return toks
    return []


if __name__ == '__main__':
    print(hex(ord('태')))
    testt = ['ジョージ・ワシントン', 'テミン', '태민', '李泰民', '李泰民']
    for s in testt:
        print(s, tokenise(s))