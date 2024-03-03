import orjson
import pickle
from tqdm import tqdm
from multiprocessing import Pool
import numpy as np
import msgpack
import gzip

def proc(line):
    rec = {}
    #l = line.decode("utf-8").strip(',\r\n')
    l = orjson.loads(line)  # orjson
    for k, langs in l.items():
        if k != 'wiki_id':
            langs = [a[1] for a in langs]
            if k in rec:
                rec[k] += langs
            else:
                rec[k] = langs
    rec = {l:list(set(langs)) for l, langs in rec.items()}
    return l['wiki_id'], rec

if __name__ == '__main__':
    fi = gzip.open('/backup/wikidata/wikil.jsonl.gz', 'rt')
    rec = {}
    pbar = tqdm(total=95_000_000)
    p = Pool(20)
    for i, l in enumerate(fi):
        tmp = fi.readlines(50_000_000)
        r = p.map(proc, tmp)
        r = {k:v for k,v in r}
        rec.update(r)
        pbar.update(len(tmp))
    pbar.close()
    print('snimam')
    pickle.dump(rec, open('/backup/wikidata/wikilabelsalias.pickle', 'wb'))
    maxl = max([int(k[1:]) for k in rec.keys()])
    print(maxl)
    lnar = np.empty(maxl+1, dtype=object)
    for k, v in rec.items():
        lnar[int(k[1:])] = msgpack.packb(v)
    np.savez(open('/backup/wikidata/wikilabelsalias.npz', 'wb'), lnar)
