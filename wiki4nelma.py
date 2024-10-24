import logging
import sys
from concurrent.futures import ThreadPoolExecutor
import threading
from queue import Queue
import gzip
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
from json import loads
from tqdm import tqdm

sys.path.insert(0, '/projekti/mondoAPI')
from pnu.detect_lang_scr import get_script
from pnu.parse_dict import spans_to_tags, tag_set, tag_to_char, NameParser
from pnu.do_tokenize import do_tokenize
from wiki_labels import qid_lab_get
from wikilang2iso import get_wiki_cc, iso2w, cc2lang, q2cc, cc_weights

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


class WikiProcessor:
    def __init__(self, num_threads=12, batch_size=10_000):
        self.num_threads = num_threads
        self.batch_size = batch_size
        self.input_queue = Queue()
        self.results = []
        self.lock = threading.Lock()

    def proc(self, j):
        """Process a single JSON record"""
        try:
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
            if 'native_language' in j and j['native_language']:
                langs = Counter([iso2w[q[5:]][:2] for q in j['native_language'] if q[5:] in iso2w])
                native_lang = langs.most_common()[0][0] if langs else ''
            else:
                langs = Counter()
            langs.update(cc2lang[cc])

            # Process names
            names = {}
            lng_max = 0
            used_langs = set()
            for lng, f in langs.most_common():
                if lng in j['labels']:
                    name_labels = j['labels'][lng]
                    for name in name_labels:
                        if name not in names:
                            scr = get_script(name)
                            if tip and cc and lng and scr:
                                names[name] = (tip, cc, lng, scr)
                if f < lng_max:
                    break
                else:
                    lng_max = f
                used_langs.add(lng)

            if 'name_native' in j and j['name_native'] and j['name_native'][0] not in names:
                name = j['name_native'][0]
                scr = get_script(name)
                if native_lang:
                    lng = native_lang
                if tip and cc and lng and scr:
                    names[name] = (tip, cc, lng, scr)

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

                if v1[0][:3] == 'per' and name_parts:
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
            logging.error(f'Error in proc: {str(e)}')
            return []

    def worker(self):
        """Worker thread function"""
        while True:
            batch = self.input_queue.get()
            if batch is None:
                break

            batch_results = []
            for line in batch:
                try:
                    j = loads(line)
                    result = self.proc(j)
                    batch_results.extend(result)
                except Exception as e:
                    logging.error(f'Error processing line: {str(e)}')
                    continue

            with self.lock:
                self.results.extend(batch_results)

            self.input_queue.task_done()

    def write_batch_to_parquet(self, batch_number):
        """Write current results to a parquet file"""
        batch_d = {k.name: [] for k in nelma_schema}
        for row_dict in self.results:
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

        writer = pq.ParquetWriter(f"/projekti/mondodb_lm/wiki_{batch_number}.parquet", nelma_schema)
        writer.write(t)
        writer.close()

    def process_file(self, input_file, debug=False):
        """Main processing function"""
        # Start worker threads
        threads = []
        for _ in range(self.num_threads):
            t = threading.Thread(target=self.worker)
            t.start()
            threads.append(t)

        current_batch = []
        batch_number = 0

        # Calculate total lines for progress bar
        with gzip.open(input_file, 'rt') as f:
            total_lines = sum(1 for _ in f)

        with gzip.open(input_file, 'rt') as f:
            with tqdm(total=total_lines) as pbar:
                for i, line in enumerate(f):
                    current_batch.append(line)

                    if len(current_batch) >= self.batch_size or i + 1 == total_lines:
                        self.input_queue.put(current_batch)

                        # Wait for batch to be processed
                        self.input_queue.join()

                        # Write results to parquet
                        self.write_batch_to_parquet(batch_number)
                        batch_number += 1

                        # Clear results and batch
                        self.results = []
                        current_batch = []

                        # Update progress
                        pbar.update(self.batch_size)

                        if debug:
                            break

        # Signal threads to exit
        for _ in threads:
            self.input_queue.put(None)

        # Wait for all threads to complete
        for t in threads:
            t.join()


if __name__ == '__main__':
    processor = WikiProcessor(num_threads=12, batch_size=10_000)
    processor.process_file('/backup/wikidata/wikinelma.jsonl.gz', debug=False)