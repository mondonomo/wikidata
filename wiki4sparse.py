# wikizemlja
import gzip, io
import json
import numpy as np
import re
from datetime import datetime
# !pip install orjson
from multiprocessing import Pool
from scipy.sparse import csr_matrix, csc_matrix, save_npz, lil_matrix, load_npz, identity
from collections import defaultdict, Counter
import marisa_trie
from text_utils import cl
#from wiki_ent_ids import wikiloc, wikiln, wikifn, wikiorg, wiki_title
from tqdm import tqdm
from pathlib import Path
from scipy.sparse.linalg import expm

BASE_DIR = '/backup/wikidata'
DATA_DIR = f'{Path(__file__).resolve().parent}/data'

zag = re.compile(' ?\([^\)]*\)')
resplit = re.compile('/|\n')

P2i = {'P31': 1, 'P279': 1, 'P39':.8, 'P631':0.95, 'P106':.8, 'P171': .9,  'P373':.9, 'P010':.9, 'P361': .8,  'P460':.95, 'P2354':.8, 'P7084':.9, 'P452':.8,
       'P527': .8, 'P1151': .95, 'P910': .95, 'P1535': 0.9, 'P2283': 0.8, 'P1056': 0.8, 'P3095': 0.8, 'P5125': 0.8,
       'P6530': 0.8, 'P5973': 0.95, 'P425': 0.9, 'P1269': 0.9, 'P8225': 0.6, 'P277': 0.8, 'P1552': 0.6, 'P2959': 0.8,
       'P101': 0.8, 'P408': 0.6, 'P137': 0.6, 'P2578': 0.8, 'P941': 0.6, 'P176': 0.8, 'P805': 0.8,
       'P366': .8, 'P737': .8, 'P1382': .8, 'P2888': .95, 'P1709': .95, 'P2579': .8}


def extract(line):
    l = line.decode("utf-8").strip(',\r\n ')
    try:
        l = json.loads(l)
    except Exception as e:
        print(e)
        return None, None, None, None

    if l['type'] != 'item' or l['id'][0] != 'Q':
        return None, None, None, None


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

    description = ''
    if 'descriptions' in l:
        if 'en' in l['descriptions']:
            description = l['descriptions']['en']['value']

    graph = []
    for P in P2i:
        if P in l['claims']:
            for s in l['claims'][P]:
               if 'datavalue' in s['mainsnak']:
                   if  s['mainsnak']['datavalue']['type'] == 'wikibase-entityid' and 'id' in s['mainsnak']['datavalue']['value']:
                       graph.append((P, int(s['mainsnak']['datavalue']['value']['id'][1:])))
                   else:
                        pass

    return l['id'][1:], labelitems, graph, description


if __name__ == '__main__':

    print('loading trie')
    trie = marisa_trie.Trie()
    trie.load(f'{DATA_DIR}/labels.trie')
    print('trie loaded')

    pmap = Pool(40)
    TEST = True
    BATCH_SIZE = 4_000_000_000 if not TEST else 100
    if True:
        max_q = 0
        #lang_combs = Counter()
        fin = gzip.open(f'{BASE_DIR}/latest-all.json.gz', 'rb')
        fin.read(2)  # skip first two bytes: "{\n"
        label4sparse = open(f'{BASE_DIR}/label4sparse.tmp', 'w')
        graph4sparse = open(f'{BASE_DIR}/graph4sparse.tmp', 'w')
        desc4sparse = open(f'{BASE_DIR}/desc4sparse.tmp', 'w')
        pbar = tqdm(fin, total=98_364_283)
        while True:
            lines = fin.readlines(BATCH_SIZE)
            if len(lines) == 0:
                break
            if not TEST:
                rec = pmap.map(extract, lines)
            else:
                rec = pmap.map(extract, lines)
            #rec = map(extract, lines)
            #print(len(list(rec)), len(lines))
            for l in rec:
                #qid, labels, lang_comb, graph = extract(l)
                qid, labels, graph, desc = l
                if qid:
                    #lang_combs.update(lang_comb)
                    label4sparse.write(f'{qid}\t{",".join(labels)}\n')
                    if desc:
                        desc4sparse.write(f'{qid}\t{desc}\n')
                    for p, qid2 in graph:
                        graph4sparse.write(f'{qid}\t{qid2}\t{p}\n')
                    qid = int(qid)
                    #print(max_q, qid)
                    if qid > max_q:
                        max_q = qid
            pbar.update(len(lines))
            if TEST:
                break

        graph4sparse.close()
        label4sparse.close()
        desc4sparse.close()
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


    if False:
        j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
        QUS = int(j['maxq'])
        desc4sparse = open(f'{BASE_DIR}/desc4sparse.tmp', 'r')
        descl = []
        for l in tqdm(desc4sparse, total=QUS):
            qid, desc = l.strip('\n\r').split('\t')
            if desc:
                descl.append(desc)
        m = marisa_trie.Trie(tuple(descl))
        m.save(f'{DATA_DIR}/desc.trie')
        print('trie saved')
        descl = np.zeros(QUS+1  , dtype=np.int32)
        uk = 0
        desc4sparse = open(f'{BASE_DIR}/desc4sparse.tmp', 'r')
        for l in tqdm(desc4sparse, total=QUS):
            qid, desc = l.strip('\n\r').split('\t')
            if desc:
                qid = int(qid)
                descl[qid] = m[desc]+1
                uk += 1
        print(uk)
        np.savez_compressed(f'{DATA_DIR}/desc', descl=descl)


    if False:
        j = json.load(open(f'{DATA_DIR}/label4sparse.json'))
        QUS = int(j['maxq'])
        graph4sparse = open(f'{BASE_DIR}/graph4sparse.tmp', 'r')
        mat = lil_matrix((QUS + 2, QUS + 2), dtype=np.float32)
        print('punim ...')
        br = 0
        for l in tqdm(graph4sparse, total=111_284_985):
            try:
                qid1, qid2, p = l.strip('\n\r').split('\t')
                qid1 = int(qid1)
                qid2 = int(qid2)
                mat[qid1, qid2] = P2i[p] - 0.1 # because of transitive
            except Exception as e:
                print(e, l)
                raise
            br += 1
            #if br>100_000:
            #    break

        print('saving, #non zero', mat.count_nonzero())
        mat = mat.maximum(identity(mat.shape[0]))
        mat = mat.maximum(mat.T)
        save_npz(f'{DATA_DIR}/graph4sparse_0', csr_matrix(mat, dtype=np.float32))
        mat = csc_matrix(mat, dtype=np.float32)

        print('closure 1 ...')
        print(mat.count_nonzero())
        m2 = expm(mat)
        print('norm ...')
        m2.data = np.minimum(mat.data / 10., 1)
        mat = mat.maximum(m2)
        print('saving ...')
        save_npz(f'{DATA_DIR}/graph4sparse_1', csr_matrix(mat, dtype=np.float32))

