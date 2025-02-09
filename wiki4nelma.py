import logging
import sys
import gzip
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
from json import loads
from tqdm import tqdm
from itertools import islice

sys.path.insert(0, '/projekti/mondoAPI')
from pnu.detect_lang_scr import get_script
from pnu.parse_dict import spans_to_tags, tag_set, tag_to_char, NameParser
from pnu.do_tokenize import do_tokenize
from wiki_labels import qid_lab_get
from wikilang2iso import get_wiki_cc, iso2w, q2cc, cc_weights

sys.path.insert(0, '/projekti/nelma')
from model.cc2lang import cc2lang

logging.basicConfig(filename='wiki4nelma_parsing.log', encoding='utf-8', level=logging.DEBUG, filemode='w')

DO_SAMPLE = False
parser = NameParser()

nelma_schema = pa.schema([
    pa.field("name", pa.string()),
    pa.field("type", pa.string()),
    pa.field("cc", pa.string()),
    pa.field("lang", pa.string()),
    pa.field("script", pa.string()),
    pa.field("tags", pa.string()),
])


def process_record(json_line):
    """Process a single JSON record"""
    try:
        j = loads(json_line)
        qid = int(j['wiki_id'][1:])
        tip = j['type']

        # Handle person records
        if tip == 'per':
            cc, cc_weight = get_wiki_cc({k: v1 for k, v1 in j.items() if k in cc_weights})
            if j['gender'] == ['WIKI_Q6581097']:
                tip = 'per_1'
            elif j['gender'] == ['WIKI_Q6581072']:
                tip = 'per_2'
        # Handle location records
        elif tip == 'loc':
            if j['wiki_id'] in q2cc:
                cc = q2cc[j['wiki_id']]
            else:
                cc, cc_weight = get_wiki_cc({'country': j['country'], 'headquarter': j['admin']})
        # Handle organization records
        elif tip == 'org':
            cc, cc_weight = get_wiki_cc({'country': j['country'], 'headquarter': j['headquarter']})
        else:
            raise NotImplementedError

        if not cc:
            return []
        if cc == 'UK':
            cc = 'GB'

        # Process language information
        native_lang = None

        langs = Counter()
        if 'native_language' in j and j['native_language']:
            native_langs = [iso2w[q[5:]][:2] for q in j['native_language'] if q[5:] in iso2w]
            langs.update(native_langs * 3)  # Triple weight

        cc_info = cc2lang.get(cc.upper())
        if cc_info:
            langs[cc_info['main']] += 2
            langs.update(cc_info['other'])

        # Process names
        names = {}
        lng_max = 0
        langs_most_common = langs.most_common()
        for lng, f in langs_most_common:
            if f < lng_max:
                break
            if lng in j['labels']:
                name_labels = j['labels'][lng]
                for name in name_labels:
                    if name not in names:
                        scr = get_script(name)
                        if tip and cc and lng and scr:
                            names[name] = (tip, cc, lng, scr)
            lng_max = f

        if 'name_native' in j and j['name_native'] and j['name_native'][0] not in names:
            name = j['name_native'][0]
            scr = get_script(name)
            if native_lang:
                lng = native_lang
            if tip and cc and lng and scr:
                names[name] = (tip, cc, lng, scr)

        # english name as transliteration
        if tip in ('per', 'per_1', 'per_2', 'org') and langs_most_common and langs_most_common[0][0] != 'en':
            if 'en' in j['labels']:
                for lab in j['labels']['en']:
                    if lab.isascii() and lab not in names:
                        names[lab] = (tip, cc, langs_most_common[0][0], 'Latn')

        # Process name parts
        name_parts = {}
        if tip[:3] == 'per':
            for tip_key, wiki_ids in j.items():
                if tip_key in ('position', 'sufix', 'affiliation'):
                    tip_key = 'title'
                if tip_key in tag_set:
                    for wiki_id in wiki_ids:
                        for label in qid_lab_get(int(wiki_id[6:])):
                            if label not in name_parts:
                                name_parts[label] = tip_key
                            elif name_parts[label] != tip_key:  # ambiguous
                                name_parts.pop(label)
                            if tip_key == 'fn' and len(label) > 1:
                                short_name = label[0] + '.'
                                name_parts[short_name] = tip_key

        rec = []
        for name, v1 in names.items():
            tags = ''

            if v1[0][:3] == 'per':
                if name_parts:
                    try:
                        tags = parser.parse_known_parts(name, name_parts)
                        tags = spans_to_tags(name, tags) if tags else ''
                    except Exception as e:
                        logging.error(f'Error processing {name}: {str(e)}')
                        continue

                if not tags:
                    try:
                        parsed = parser.parse_name(name, limit_to_lang=v1[2],
                                               ignore_sequence=True, parse_spaced=False)
                        if parsed:
                            for parsed1 in parsed:
                                name_parts2 = {a[0]: a[1].split('_')[-1] for a in parsed1[1]}
                                name_tags = tuple(a[1] for a in parsed[0][1])
                                if parsed1[0] > 0.05 or len(set(name_parts.items()) & set(name_parts2.items())) > 0:
                                    if name_tags in parser.final_seq:
                                        tags = parser.parse_known_parts(name, name_parts2)
                                        tags = spans_to_tags(name, tags)
                                        logging.info(f'parsed {name} to {" ".join(name_tags)}')
                                        break
                                if parsed1[0] < 0.1:
                                    break
                    except TimeoutError:
                        logging.error(f'timeout, name {name}')
                        continue
                    except Exception as e:
                        logging.error(f'Error parsing {name}: {str(e)}')
                        continue

            elif v1[0][:3] in ('org', 'loc'):
                toks, spans = do_tokenize(name, v1[2])
                tags = ['0'] * len(name)
                for bs, es in spans:
                    tags[bs] = tag_to_char[(v1[0][:3], True)]
                    tags[bs + 1:es] = tag_to_char[(v1[0][:3], False)] * (es - bs - 1)
                tags = ''.join(tags)

            rec.append({
                'name': name,
                'tags': tags,
                'type': v1[0],
                'cc': v1[1],
                'lang': v1[2],
                'script': v1[3]
            })

        return rec
    except Exception as e:
        logging.error(f'Error in process_record: {str(e)}')
        return []


def batch_to_parquet(batch_data, batch_number):
    """Write a batch of results to a parquet file"""
    # Flatten the list of lists
    flat_data = [item for sublist in batch_data if sublist for item in sublist]

    if not flat_data:
        return

    batch_d = {k.name: [] for k in nelma_schema}
    for row_dict in flat_data:
        for k, v in row_dict.items():
            batch_d[k].append(v)

    t = pa.Table.from_arrays([
        pa.array(batch_d['name']),
        pa.array(batch_d['type'], type=pa.string()),
        pa.array(batch_d['cc'], type=pa.string()),
        pa.array(batch_d['lang'], type=pa.string()),
        pa.array(batch_d['script'], type=pa.string()),
        pa.array(batch_d['tags']),
    ], schema=nelma_schema)

    writer = pq.ParquetWriter(f"/projekti/mondodb_lm/wiki_{batch_number:04d}.parquet", nelma_schema)
    writer.write(t)
    writer.close()


def process_file(input_file, batch_size=50_000, debug=False):
    """Process the input file sequentially in batches"""
    # Calculate total lines for progress bar
    total_lines = 26852740

    batch_number = 0
    with gzip.open(input_file, 'rt') as f:
        with tqdm(total=total_lines) as pbar:
            while True:
                # Read batch_size lines
                batch = list(islice(f, batch_size))
                if not batch:
                    break

                # Process the batch sequentially
                results = [process_record(line) for line in batch]

                # Write results to parquet
                batch_to_parquet(results, batch_number)
                batch_number += 1

                # Update progress
                pbar.update(len(batch))

                if debug:
                    break


if __name__ == '__main__':
    process_file('/backup/wikidata/wikinelma.jsonl.gz',
                batch_size=100_000,
                debug=False)