import logging
import sys
import gzip
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
from json import loads
from tqdm import tqdm
from itertools import islice
import multiprocessing
from datetime import datetime
import psutil
import os

# Configure logging with process ID for better debugging
logging.basicConfig(
    filename='wiki4nelma_parsing.log',
    encoding='utf-8',
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s - PID:%(process)d - %(levelname)s - %(message)s'
)

print('loading modules pnu')
sys.path.insert(0, '/projekti/mondoAPI')
from pnu.detect_lang_scr import get_script
from pnu.do_tokenize import do_tokenize

print('loading modules parse')
from parse_simple import parse_known_parts, spans_to_tags, tag_set, tag_to_char

print('loading modules wiki')
from wiki_labels import qid_lab_get, data_manager
from wikilang2iso import get_wiki_cc, iso2w, q2cc, cc_weights

print('loading modules cc2lang')
sys.path.insert(0, '/projekti/nelma')
from model.cc2lang import cc2lang

DO_SAMPLE = False
print('loaded modules')

nelma_schema = pa.schema([
    pa.field("name", pa.string()),
    pa.field("type", pa.string()),
    pa.field("cc", pa.string()),
    pa.field("lang", pa.string()),
    pa.field("script", pa.string()),
    pa.field("tags", pa.string()),
])


def log_memory_usage():
    """Log current process memory usage"""
    process = psutil.Process(os.getpid())
    mem_info = process.memory_info()
    logging.info(f"Memory usage - RSS: {mem_info.rss / 1024 / 1024:.2f}MB, VMS: {mem_info.vms / 1024 / 1024:.2f}MB")


def process_record(json_line):
    """Process a single JSON record"""
    try:
        process_id = os.getpid()
        #logging.debug(f"Process {process_id} starting new record")
        log_memory_usage()

        j = loads(json_line)
        qid = int(j['wiki_id'][1:])
        tip = j['type']

        #logging.debug(f"Process {process_id} processing record type {tip}")

        # Handle person records
        if tip == 'per':
            cc, cc_weight = get_wiki_cc({k: v1 for k, v1 in j.items() if k in cc_weights})
            #logging.debug(f"Process {process_id} got cc: {cc}")

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
            logging.info(f"Skipping {j['wiki_id']}, no cc ")
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

        #logging.debug(f"Process {process_id} processing names")

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
                if tip and cc and scr:
                    names[name] = (tip, cc, native_lang, scr)

        # English name as transliteration
        if tip in ('per', 'per_1', 'per_2', 'org') and langs_most_common and langs_most_common[0][0] != 'en':
            if 'en' in j['labels']:
                for lab in j['labels']['en']:
                    if lab.isascii() and lab not in names:
                        names[lab] = (tip, cc, langs_most_common[0][0], 'Latn')

        #logging.debug(f"Process {process_id} processing name parts")

        # Process name parts
        name_parts = {}
        if tip[:3] == 'per':
            for tip_key, wiki_ids in j.items():
                if tip_key in ('position', 'sufix', 'affiliation'):
                    tip_key = 'title'
                if tip_key in tag_set:
                    for wiki_id in wiki_ids:
                        try:
                            #logging.debug(f"Process {process_id} accessing qid_lab_get")
                            for label in qid_lab_get(int(wiki_id[6:])):
                                if label not in name_parts:
                                    name_parts[label] = tip_key
                                elif name_parts[label] != tip_key:  # ambiguous
                                    name_parts.pop(label)
                                if tip_key == 'fn' and len(label) > 1:
                                    short_name = label[0] + '.'
                                    name_parts[short_name] = tip_key
                            #logging.debug(f"Process {process_id} finished qid_lab_get")
                        except Exception as e:
                            logging.error(f"Process {process_id} error in qid_lab_get: {str(e)}")
                            continue

        rec = []
        for name, v1 in names.items():
            tags = ''

            if v1[0][:3] == 'per':
                if name_parts:
                    try:
                        tags = parse_known_parts(name, name_parts)
                        tags = spans_to_tags(name, tags) if tags else ''
                    except Exception as e:
                        logging.error(f'Process {process_id} Error processing {name}: {str(e)}')
                        continue

            rec.append({
                'name': name,
                'tags': tags,
                'type': v1[0],
                'cc': v1[1],
                'lang': v1[2],
                'script': v1[3]
            })

        if len(rec) == 0:
            logging.info(f"Empty rec {j['wiki_id']}")

        return rec

    except Exception as e:
        logging.error(f'Process {process_id} Error in process_record: {str(e)}')
        raise


def batch_to_parquet(batch_data, batch_number):
    """Write a batch of results to a parquet file"""
    try:
        process_id = os.getpid()
        logging.info(f"Process {process_id} starting batch_to_parquet for batch {batch_number}")

        flat_data = [item for sublist in batch_data if sublist for item in sublist]

        if not flat_data:
            logging.warning(f"Process {process_id} Empty batch {batch_number}")
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

        output_path = f"/projekti/mondodb_lm/wiki_{batch_number:04d}.parquet"
        writer = pq.ParquetWriter(output_path, nelma_schema)
        writer.write(t)
        writer.close()

        #logging.info(f"Process {process_id} finished writing batch {batch_number}")

    except Exception as e:
        logging.error(f'Process {process_id} Error in batch_to_parquet: {str(e)}')
        raise


def process_file_parallel(input_file, batch_size=50_000, num_processes=None, debug=False):
    """Process the input file in parallel using multiprocessing."""

    if num_processes is None:
        num_processes = multiprocessing.cpu_count()

    #logging.info(f"Starting parallel processing with {num_processes} processes")

    # Force initialization of data_manager before creating the pool
    _ = data_manager.qid_lab_get(1)

    total_lines = 26852740
    batch_number = 0

    with gzip.open(input_file, 'rt') as f, \
            multiprocessing.Pool(processes=20) as pool, \
            tqdm(total=total_lines) as pbar:

        while True:
            batch = list(islice(f, batch_size))
            if not batch:
                break

            #logging.info(f"Main process starting batch {batch_number} with {len(batch)} records at {datetime.now()}")
            print('processing batch', batch_number, len(batch), datetime.now())

            # Using imap_unordered since order doesn't matter
            results = list(pool.imap_unordered(process_record, batch))

            logging.info(f"Main process writing batch {batch_number}")
            print('writing batch', batch_number, len(batch), datetime.now())
            batch_to_parquet(results, batch_number)

            logging.info(f"Main process finished batch {batch_number}")
            print('finish writing batch', batch_number, datetime.now())

            batch_number += 1
            pbar.update(len(batch))

            if debug:
                break


if __name__ == '__main__':
    try:
        logging.info("Starting script execution")
        process_file_parallel('/backup/wikidata/wikinelma.jsonl.gz',
                              batch_size=100_000,
                              num_processes=6,
                              debug=False)
        logging.info("Script execution completed successfully")
    except Exception as e:
        logging.error(f"Script failed with error: {str(e)}")
        raise