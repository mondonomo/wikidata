# wikizemlja
import gzip, io
import json
import numpy as np
import re
from datetime import datetime
# !pip install orjson
import orjson
from multiprocessing import Pool
from scipy.sparse import csr_matrix, save_npz, lil_matrix, load_npz
from collections import defaultdict, Counter
import marisa_trie
from text_utils import cl
#from wiki_ent_ids import wikiloc, wikiln, wikifn, wikiorg, wiki_title
from tqdm import tqdm
from pathlib import Path

BASE_DIR = '/backup/wikidata'
DATA_DIR = f'{Path(__file__).resolve().parent}/data'

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')

PS = ('P31', 'P361', 'P279', 'P460', 'P2354', 'P7084')
p2i = {k:i+1 for i, k in enumerate(PS)}
PS_text = ('instance', 'part', 'subclass', 'same_as', 'has wiki list', 'relatd wiki category')

print('loading trie')
trie = marisa_trie.Trie()
trie.load(f'{BASE_DIR}/labels.trie')
print('trie loaded')


def extract(line):
    l = line.decode("utf-8").strip(',\r\n ')
    try:
        l = orjson.loads(l)
    except Exception as e:
        print(e)
        return None, None#, None, None

    if l['type'] != 'item' or l['id'][0] != 'Q':
        return None, None


    labelitems = set()
    labels_lang = defaultdict(set)
    if 'labels' in l:
        for k, v in dict(l['labels'].items()).items():
            nl = cl(v['value']).lower()
            if nl:
                ni = trie[nl]
                labelitems.add(str(ni)+f'_{v["language"]}_M')
                labels_lang[ni].add(v["language"])

    if 'aliases' in l:
        for k, v in dict(l['aliases']).items():
            for v2 in v:
                nl = cl(v2['value']).lower()
                if nl:
                    ni = trie[nl]
                    labelitems.add(str(ni)+f'_{v2["language"]}_A')
                    labels_lang[ni].add(v2["language"])

    graph = []
    if False:
        for P in PS:
            if P in l['claims']:
                for s in l['claims'][P]:
                   if 'datavalue' in s['mainsnak']:
                       if 'id' in s['mainsnak']['datavalue']['value']:
                           graph.append((P, int(s['mainsnak']['datavalue']['value']['id'][1:])))
                       else:
                            pass

    #lang_comb = Counter([';'.join(sorted(l)) for l in labels_lang.values()])
    return l['id'][1:], labelitems#, lang_comb, graph


if __name__ == '__main__':
    pmap = Pool(20)
    TEST = False
    BATCH_SIZE = 1_000_000_000 if not TEST else 10_000_000
    if False:
        max_q = 0
        #lang_combs = Counter()
        fin = gzip.open(f'{BASE_DIR}/latest-all.json.gz', 'rb')
        fin.read(2)  # skip first two bytes: "{\n"
        label4sparse = open(f'{BASE_DIR}/label4sparse.tmp', 'w')
        #graph4sparse = open(f'{BASE_DIR}/graph4sparse.tmp', 'w')
        pbar = tqdm(fin, total=98_364_283)
        while True:
            lines = fin.readlines(BATCH_SIZE)
            if len(lines) == 0:
                break
            rec = pmap.map(extract, lines)
            #print(len(list(rec)), len(lines))
            for l in rec:
                #qid, labels, lang_comb, graph = extract(l)
                qid, labels = l
                if qid:
                    #lang_combs.update(lang_comb)
                    label4sparse.write(f'{qid}\t{",".join(labels)}\n')
                    #for p, qid2 in graph:
                    #    graph4sparse.write(f'{qid}\t{qid2}\t{p}\n')
                    qid = int(qid)
                    if qid == 6348:
                        print(qid)
                    #print(max_q, qid)
                    if qid > max_q:
                        max_q = qid
            pbar.update(len(lines))
            if TEST:
                break

        #graph4sparse.close()
        label4sparse.close()
        j = {'maxq': max_q}
        with open(f'{DATA_DIR}/label4sparse.json', 'w') as fo:
            json.dump(j, fo)

    if False:
        j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
        QUS = int(j['maxq'])
        lang2id = {}  #{k: i+1 for i, k in enumerate(j['langs_comb'])}
        mat = lil_matrix((QUS+1, len(trie)), dtype=np.int32)
        print('punim ...')
        label4sparse = open(f'{BASE_DIR}/label4sparse.tmp', 'r')
        for l in tqdm(label4sparse, total=QUS):
            qid, labels = l.strip('\n\r').split('\t')
            if labels:
                qid = int(qid)
                l_main = defaultdict(set)
                l_alt = defaultdict(set)
                for lab in labels.split(','):
                    label_id, lang, tip = lab.split('_')
                    label_id = int(label_id)
                    if tip == 'M':
                        l_main[label_id].add(lang)
                    else:
                        l_alt[label_id].add(lang)
                # overlap alternative labels & main - include all
                for label, langs in l_alt.items():
                    if label in l_main:
                        l_main[label].update(langs)
                for label, langs in l_alt.items():
                    langs = ';'.join(sorted(langs))
                    if langs not in lang2id:
                        lang2id[langs] = len(lang2id)+1
                    mat[qid, label] = -lang2id[langs]
                for label, langs in l_main.items():
                    langs = ';'.join(sorted(langs))
                    #print(qid, label, mat.shape, lang2id[langs], type(qid), type(label))
                    if langs not in lang2id:
                        lang2id[langs] = len(lang2id)+1
                    mat[qid, label] = lang2id[langs]

                #print(len(l_main), len(l_alt), len(l_main|l_alt))
        #exit()
        print('snimam ...', '#lang2id', len(lang2id))
        save_npz(f'{DATA_DIR}/qidlabel', csr_matrix(mat))
        j = {'maxq': QUS, 'lang2id': lang2id}
        with open(f'{DATA_DIR}/label4sparse.json', 'w') as fo:
            json.dump(j, fo)
        mat = None
        print('gotovo')

    if False:
        j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
        QUS = int(j['maxq'])
        lang2id = dict(j['lang2id'])
        label4sparse = open(f'{BASE_DIR}/label4sparse.tmp', 'r')
        mat = lil_matrix((len(trie), QUS + 2), dtype=np.int32)
        print('punim ...,', mat.shape)
        for l in tqdm(label4sparse, total=QUS):
            qid, labels = l.strip('\n\r').split('\t')
            if labels:
                qid = int(qid)
                l_main = defaultdict(set)
                l_alt = defaultdict(set)
                for lab in labels.split(','):
                    label_id, lang, tip = lab.split('_')
                    label_id = int(label_id)
                    if tip == 'M':
                        l_main[label_id].add(lang)
                    else:
                        l_alt[label_id].add(lang)
                # overlap alternative labels & main - include all
                for label, langs in l_alt.items():
                    if label in l_main:
                        l_main[label].update(langs)
                for label, langs in l_alt.items():
                    langs = ';'.join(sorted(langs))
                    if langs not in lang2id:
                        lang2id[langs] = len(lang2id) + 1
                    mat[label, qid] = -lang2id[langs]
                for label, langs in l_main.items():
                    langs = ';'.join(sorted(langs))
                    if langs not in lang2id:
                        lang2id[langs] = len(lang2id) + 1
                    mat[label, qid] = lang2id[langs]

        print('snimam ...')
        save_npz(f'{DATA_DIR}/labelqid', csr_matrix(mat))
        print('gotovo')
        j['lang2id_qid2label'] = lang2id
        with open(f'{DATA_DIR}/label4sparse.json', 'w') as fo:
            json.dump(j, fo)

    if True:
        j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
        QUS = int(j['maxq'])
        graph4sparse = open(f'{BASE_DIR}/graph4sparse.tmp', 'r')
        mat = lil_matrix((QUS + 2, QUS + 2), dtype=np.int8)
        print('punim ...')
        br = 0
        for l in tqdm(graph4sparse, total=111_284_985):
            try:
                qid1, qid2, p = l.strip('\n\r').split('\t')
                qid1 = int(qid1)
                qid2 = int(qid2)
                mat[qid1, qid2] = 100 - p2i[p]
            except Exception as e:
                print(e, l)
            br += 1
            #if br>100_000:
            #    break

        for line in tqdm(range(mat.shape[0]), total=mat.shape[0]):
            x, y = mat[line, :].nonzero()
            for y1 in y:
                P = mat[line, y1]
                if P in (1, 2, 3):
                    father = y1
                    for i in range(5): # up to 5 parents
                        _, y2 = mat[father, :].nonzero()
                        y2 = {mat[father, i]: i for i in y2}
                        if P in y2 and not mat[line, y2[P]]:
                                mat[line, y2[P]] = 100-(i+1)*10+P
                        else:
                            break

        print('snimam ...')
        save_npz(f'{DATA_DIR}/graph4sparse', csr_matrix(mat))

