import orjson
import pickle
from tqdm import tqdm
from multiprocessing import Pool
import numpy as np
import msgpack
import marisa_trie
import gzip

if __name__ == '__main__':
    WIKI_D = '/backup/wikidata/'
    fi = gzip.open(f'{WIKI_D}wikil.jsonl.gz', 'rt')
    rec = {}
    pbar = tqdm(total=98_123_689)
    labels = set()
    uk = 0
    while True:
        tmp = fi.readlines(150_000_000)
        if len(tmp) == 0:
            break
        for line in tmp:
            j = orjson.loads(line)
            labels.update([a.lower().strip() for a in j if a != 'wiki_id'])
        pbar.update(len(tmp))
        uk += len(tmp)
    pbar.close()
    print('snimam')
    #pickle.dump(labels, open(f'{WIKI_D}wikilabes4trie.pickle', 'wb'))
    labels = tuple(labels)
    trie = marisa_trie.Trie(labels)
    labels = None
    trie.save(f'{WIKI_D}labels.trie')
    print('trie saved', uk)
