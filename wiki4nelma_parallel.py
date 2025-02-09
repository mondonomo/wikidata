import logging
import sys
from concurrent.futures import ProcessPoolExecutor
import gzip
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
from json import loads
from tqdm import tqdm
from functools import partial
from itertools import islice
import multiprocessing as mp
import numpy as np
from pathlib import Path
import json
import marisa_trie
from scipy.sparse import load_npz, csr_matrix
from multiprocessing import shared_memory

# Global variables for shared data
shared_data = {}

# Set up logging
logging.basicConfig(filename='wiki4nelma_parsing.log', encoding='utf-8', level=logging.DEBUG, filemode='w')

# Define schema for parquet output
nelma_schema = pa.schema([
    pa.field("name", pa.string()),
    pa.field("type", pa.string()),
    pa.field("cc", pa.string()),
    pa.field("lang", pa.string()),
    pa.field("script", pa.string()),
    pa.field("tags", pa.string()),
])


def load_label_data():
    """Load all necessary data for label processing"""
    base_dir = Path(__file__).resolve().parent
    data_dir = Path.joinpath(base_dir, 'data')

    # Load trie
    trie = marisa_trie.Trie()
    trie.load(f'{data_dir}/labels.trie')

    # Load language mappings
    with open(f'{data_dir}/label4sparse.json') as f:
        j = json.load(f)
    id2lang = {v: set(k.split(';')) for k, v in j['lang2id'].items()}

    # Load sparse matrix
    qid_lab_data = load_npz(f'{data_dir}/qidlabel.npz')

    return {
        'trie': trie,
        'id2lang': id2lang,
        'qid_lab_data': qid_lab_data,
        'data_dir': data_dir
    }


def create_shared_memory(data):
    """Create shared memory blocks for the sparse matrix data"""
    # Create shared memory for sparse matrix components
    data_shm = shared_memory.SharedMemory(create=True, size=data['qid_lab_data'].data.nbytes)
    indices_shm = shared_memory.SharedMemory(create=True, size=data['qid_lab_data'].indices.nbytes)
    indptr_shm = shared_memory.SharedMemory(create=True, size=data['qid_lab_data'].indptr.nbytes)

    # Create arrays backed by shared memory
    data_array = np.ndarray(data['qid_lab_data'].data.shape, dtype=data['qid_lab_data'].data.dtype, buffer=data_shm.buf)
    indices_array = np.ndarray(data['qid_lab_data'].indices.shape, dtype=data['qid_lab_data'].indices.dtype,
                               buffer=indices_shm.buf)
    indptr_array = np.ndarray(data['qid_lab_data'].indptr.shape, dtype=data['qid_lab_data'].indptr.dtype,
                              buffer=indptr_shm.buf)

    # Copy data to shared memory
    data_array[:] = data['qid_lab_data'].data[:]
    indices_array[:] = data['qid_lab_data'].indices[:]
    indptr_array[:] = data['qid_lab_data'].indptr[:]

    return {
        'data_shm': data_shm,
        'indices_shm': indices_shm,
        'indptr_shm': indptr_shm,
        'shape': data['qid_lab_data'].shape,
        'trie': data['trie'],
        'id2lang': data['id2lang'],
        'data_dir': data['data_dir']
    }


def init_worker(data_shm_name, indices_shm_name, indptr_shm_name, shape, data_dir, id2lang):
    """Initialize worker process with shared modules and data"""
    global parser, get_script, spans_to_tags, tag_set, tag_to_char
    global get_wiki_cc, iso2w, cc2lang, q2cc, cc_weights, do_tokenize
    global shared_data

    # Add paths to system path
    sys.path.insert(0, '/projekti/mondoAPI')
    sys.path.insert(0, '/projekti/nelma')

    # Import modules
    from pnu.detect_lang_scr import get_script
    from pnu.parse_dict import spans_to_tags, tag_set, tag_to_char, NameParser
    from pnu.do_tokenize import do_tokenize
    from wikilang2iso import get_wiki_cc, iso2w, cc2lang, q2cc, cc_weights
    from model.cc2lang import cc2lang

    # Initialize parser
    parser = NameParser()

    # Access shared memory
    data_shm = shared_memory.SharedMemory(name=data_shm_name)
    indices_shm = shared_memory.SharedMemory(name=indices_shm_name)
    indptr_shm = shared_memory.SharedMemory(name=indptr_shm_name)

    # Create numpy arrays from shared memory
    data = np.ndarray(shape[0], dtype=np.int32, buffer=data_shm.buf)
    indices = np.ndarray(shape[0], dtype=np.int32, buffer=indices_shm.buf)
    indptr = np.ndarray(shape[0] + 1, dtype=np.int32, buffer=indptr_shm.buf)

    # Load trie
    trie = marisa_trie.Trie()
    trie.load(f'{data_dir}/labels.trie')

    # Store all shared data
    shared_data.update({
        'data_shm': data_shm,
        'indices_shm': indices_shm,
        'indptr_shm': indptr_shm,
        'data': data,
        'indices': indices,
        'indptr': indptr,
        'trie': trie,
        'id2lang': id2lang
    })


def qid_lab_get(qid: int, filter_lang: str = None, include_alt: bool = False, return_alt: bool = False):
    """Get labels for a QID using shared memory data"""
    try:
        start = shared_data['indptr'][qid]
        end = shared_data['indptr'][qid + 1]
        indices = shared_data['indices'][start:end]
        data_values = shared_data['data'][start:end]
    except:
        return {}

    rec = {}
    for lid, langs_id in zip(indices, data_values):
        if not return_alt:
            if langs_id > 0 or include_alt:
                current_langs = None
                if filter_lang:
                    if len(filter_lang) == 2:
                        current_langs = {(k.split('_')[0] if '_' in k else k[:2]) for k in
                                         shared_data['id2lang'][abs(langs_id)]}
                    else:
                        current_langs = shared_data['id2lang'][abs(langs_id)]

                if not filter_lang or filter_lang in (current_langs or shared_data['id2lang'][abs(langs_id)]):
                    rec[shared_data['trie'].restore_key(lid)] = shared_data['id2lang'][abs(langs_id)]
        else:
            l = shared_data['trie'].restore_key(lid)
            rec[l] = set()
            for lang_id in shared_data['id2lang'][abs(langs_id)]:
                if lang_id == 'sh_Latn':
                    rec[l].add(('hr_Latn', langs_id > 0))
                    rec[l].add(('sr_Latn', langs_id > 0))
                    rec[l].add(('bs_Latn', langs_id > 0))
                else:
                    rec[l].add((lang_id, langs_id > 0))
    return rec

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

        # English name as transliteration
        if (tip.startswith('per') or tip == 'org') and langs_most_common and langs_most_common[0][0] != 'en':
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
                        labels = qid_lab_get(int(wiki_id[6:]))
                        for label in labels:
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
                                        # logging.info(f'parsed {name} to {" ".join(name_tags)}')
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


def cleanup_shared_memory(shared_blocks):
    """Cleanup all shared memory blocks"""
    for shm in shared_blocks.values():
        if isinstance(shm, shared_memory.SharedMemory):
            try:
                shm.close()
                shm.unlink()
            except Exception as e:
                logging.error(f"Error cleaning up shared memory: {e}")


def process_in_batches(input_file, batch_size=50_000, num_processes=24, debug=False):
    """Process the input file in batches using ProcessPoolExecutor"""
    # Calculate total lines for progress bar
    total_lines = 26852740

    # Load all necessary data
    label_data = load_label_data()

    # Create shared memory blocks
    shared_blocks = create_shared_memory(label_data)

    try:
        # Create a process pool with initialization
        with ProcessPoolExecutor(max_workers=num_processes,
                                 initializer=init_worker,
                                 initargs=(shared_blocks['data_shm'].name,
                                           shared_blocks['indices_shm'].name,
                                           shared_blocks['indptr_shm'].name,
                                           shared_blocks['shape'],
                                           shared_blocks['data_dir'],
                                           shared_blocks['id2lang'])) as executor:
            batch_number = 0
            with gzip.open(input_file, 'rt') as f:
                with tqdm(total=total_lines) as pbar:
                    while True:
                        batch = list(islice(f, batch_size))
                        if not batch:
                            break

                        results = list(executor.map(process_record, batch))
                        batch_to_parquet(results, batch_number)
                        batch_number += 1
                        pbar.update(len(batch))

                        if debug:
                            break
    finally:
        cleanup_shared_memory(shared_blocks)


if __name__ == '__main__':
    process_in_batches('/backup/wikidata/wikinelma.jsonl.gz',
                       batch_size=100_000,
                       num_processes=24,
                       debug=False)