import re
from typing import Optional, Dict, DefaultDict, Set, List, Tuple
from collections import defaultdict
import unicodedata
import numpy as np
import sys
sys.path.insert(0, '/projekti/mondoAPI')

Spans = Dict[str, List[Tuple[int, int]]]

# tag, is beginning
tag_to_char = {('unknown', True): '0',
                 ('fn', True): '1', ('fn', False): '2', ('ln', True): '3', ('ln', False): '4',
                 ('title', True): '5', ('title', False): '6', ('pat', True): '7', ('pat', False): '8',
                 ('other', True): '9', ('other', False): 'A',
                 ('loc', True): 'B', ('loc', False): 'C',
                 ('org', True): 'D', ('org', False): 'E',
               # TODO distinction between unrecognized and other
               }

tag_set = {k for k, v in tag_to_char}


# Special case mapping for various scripts
SPECIAL_LOWERCASE = {
    'İ': 'i',  # Turkish uppercase I with dot
    'I': 'ı',  # Turkish uppercase I without dot
    'Ι': 'ι',  # Greek Iota
    'Σ': 'σ',  # Greek Sigma (not final form)
}

type2id = {'per': 1, 'org': 2, 'loc': 3, 'oth': 4}

# tag, is beginning
tag_to_char = {('unknown', True): '0',
                 ('fn', True): '1', ('fn', False): '2', ('ln', True): '3', ('ln', False): '4',
                 ('title', True): '5', ('title', False): '6', ('pat', True): '7', ('pat', False): '8',
                 ('other', True): '9', ('other', False): 'A',
                 ('loc', True): 'B', ('loc', False): 'C',
                 ('org', True): 'D', ('org', False): 'E',
               # TODO distinction between unrecognized and other
               }

tag_set = {k for k, v in tag_to_char}

def normalize_string(s: str) -> str:
    """Normalize string while preserving Unicode characters"""
    normalized = unicodedata.normalize('NFKC', s)
    result = []
    for c in normalized:
        if c in SPECIAL_LOWERCASE:
            result.append(SPECIAL_LOWERCASE[c])
        else:
            result.append(c.lower())
    return ''.join(result)

def spans_to_tags(s: str, spans: Spans) -> str:
    """Convert spans to tags"""
    tags = ['0'] * len(s)
    for tag, span_list in spans.items():
        for span_start, span_end in span_list:
            is_first_tag = True
            for i in range(span_start, span_end):
                try:
                    if tags[i] != '0':
                        return ''
                except Exception as e:
                    print(s, spans, e)
                    raise
                if tag in tag_set:
                    tags[i] = tag_to_char[(tag, is_first_tag)]
                is_first_tag = False
    return ''.join(tags)

def parse_known_parts(self, text: str, parts: Dict[str, str],
                      restrict_lang: Optional[Set[str]] = None) -> Spans:
    """Parse text using known name parts"""
    spans: DefaultDict[str, list] = defaultdict(list)
    text_normalized = normalize_string(text)
    parts_normalized = {normalize_string(k): v for k, v in parts.items()}

    sorted_keys = sorted(parts_normalized.keys(), key=len, reverse=True)
    pattern = '|'.join(re.escape(k) for k in sorted_keys)
    matcher = re.compile(pattern)

    text_len = len(text)
    char_mapping = np.arange(text_len, dtype=np.int32)

    prev_end = 0
    for match in matcher.finditer(text_normalized):
        start, end = match.span()
        if start >= text_len:
            continue

        matched_text = text_normalized[start:end]
        actual_text = matched_text.strip()

        original_key = next((k for k in parts.keys()
                             if normalize_string(k) == actual_text), None)

        if original_key is None:
            continue

        tag_type = parts[original_key]
        actual_start = start + matched_text.find(actual_text)
        actual_end = actual_start + len(actual_text)

        original_start = char_mapping[actual_start]
        original_end = char_mapping[min(actual_end - 1, text_len - 1)]

        if original_end >= original_start:
            spans[tag_type].append((original_start, original_end + 1))
            prev_end = actual_end

    return dict(spans)

