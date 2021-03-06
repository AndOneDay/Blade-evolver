from __future__ import division
import os
import sys
import json
import numpy as np
import pickle
import gc

NUM_CLASS = 3


def humanize_bytes(size_bytes, precision=1):
    '''
    Return a humanized string representation of a number of bytes.
    Assumes `from __future__ import division`.
    '''
    abbrevs = (
        (1<<50L, 'PB'),
        (1<<40L, 'TB'),
        (1<<30L, 'GB'),
        (1<<20L, 'MB'),
        (1<<10L, 'kB'),
        (1, 'bytes')
    )
    if size_bytes == 1:
        return '1 byte'
    for factor, suffix in abbrevs:
        if size_bytes >= factor:
            break
    return '%.*f %s' % (precision, size_bytes / factor, suffix)


def log_filter(log_name, label='pulp'):
    filtered_list = list()
    normal_filtered_list = list()
    sexy_filtered_list = list()
    # url_lst = []
    # uid_lst = []
    with open(log_name, 'r') as f:
        for line in f.xreadlines():
            temp = json.loads(line.strip())
            #filter by uid
            if temp['uid'] == 1380304165:
                continue
            if temp['label'][0]['data'] == None:
                continue
            elif len(temp['label'][0]['data']) < NUM_CLASS:
                continue
            else:
                scores = [(x['class'],x['score']) for x in temp['label'][0]['data']]
                # filter condition: pulp
                scores_sorted = sorted(scores, key=lambda x: x[1])
                if label == 'normal' and scores_sorted[-1][0] == 'normal':
                    normal_filtered_list.append((os.path.basename(temp['url']), scores_sorted[-1][1]))
                    # url_lst.append(os.path.basename(temp['url']))
                    # uid_lst.append(temp['uid'])
                elif label == 'sexy' and scores_sorted[-1][0] == 'sexy':
                    sexy_filtered_list.append((os.path.basename(temp['url']), scores_sorted[-1][1]))
                    # url_lst.append(os.path.basename(temp['url']))
                    # uid_lst.append(temp['uid'])
                elif scores_sorted[-1][0] == label:
                    filtered_list.append(os.path.basename(temp['url']))
                    # url_lst.append(os.path.basename(temp['url']))
                    # uid_lst.append(temp['uid'])
    if label == 'normal':
        normal_num = 100000
        normal_filtered_list = sorted(normal_filtered_list, key=lambda x: x[1])
        if len(normal_filtered_list) > normal_num:
            normal_filtered_list = normal_filtered_list[:normal_num]
        for item in normal_filtered_list:
            filtered_list.append(item[0])
    elif label == 'sexy':
        sexy_num = 150000
        sexy_filtered_list = sorted(sexy_filtered_list, key=lambda x: x[1])
        #print('sexy len:', len(sexy_filtered_list))
        if len(sexy_filtered_list) > sexy_num:
            sexy_filtered_list = sexy_filtered_list[:sexy_num]
        #import random
        #sexy_filtered_list = random.sample(sexy_filtered_list, sexy_num)
        for item in sexy_filtered_list:
            filtered_list.append(item[0])
    # url_lst = np.array(url_lst)
    # uid_lst = np.array(uid_lst)
    # pickle.dump(url_lst, open(tmp_url_lst, 'wb'))
    # pickle.dump(uid_lst, open(tmp_uid_lst, 'wb'))
    return filtered_list
                
    
def qhash(exec_path, img_list, url_prefix, output_path, thread_num=16):
    if not os.access(exec_path, os.X_OK):
        return 1 
    hash_cmd = '{} {} {} --prefix {} --output {} >/dev/null'.format(exec_path, img_list, thread_num, url_prefix, output_path)
    return os.system(hash_cmd)
    

def deduplicate(basic_file, delta_file, updated_file, uniq_delta_list, remote_img_prefix, url_uid_name, log_name):
    '''
    TODO: optimize code
    '''
    with open(basic_file, 'r') as f1, open(delta_file, 'r') as f2:
        base = json.load(f1)
        delta = json.load(f2)
    hash_set = dict()
    updated = dict()
    update_list = list()

    # construct base hash set
    for img in base:
        if 'md5' not in base[img]:
            continue
        tmp_hash = base[img]['md5']
        if tmp_hash not in hash_set:
            hash_set[tmp_hash] = img

    # deduplication
    for img in delta:
        if 'md5' not in delta[img]:
            continue
        tmp_hash = delta[img]['md5']
        if tmp_hash not in hash_set:
            update_list.append(img)
            hash_set[tmp_hash] = img
    
    for tmp_hash in hash_set:
        updated[hash_set[tmp_hash]] = dict()
        updated[hash_set[tmp_hash]]['md5'] = tmp_hash
        
    with open(updated_file, 'w') as f1, open(uniq_delta_list, 'w') as f2:
        json.dump(updated, f1, indent=2)
        for img in update_list:
            f2.write('{}\n'.format(os.path.join(remote_img_prefix,img)))

    # url_lst = pickle.load(open(tmp_url_lst, 'rb'))
    # uid_lst = pickle.load(open(tmp_uid_lst, 'rb'))
    # index = np.argsort(url_lst)
    # url_lst = url_lst[index]
    # uid_lst = uid_lst[index]

    update_list = set(update_list)
    gc.collect()
    with open(url_uid_name, 'a') as f3:
        with open(log_name, 'r') as f:
            for line in f.xreadlines():
                temp = json.loads(line.strip())
                #filter by uid
                if temp['uid'] == 1380304165:
                    continue
                if temp['label'][0]['data'] == None:
                    continue
                elif len(temp['label'][0]['data']) < NUM_CLASS:
                    continue
                else:
                    url = os.path.basename(temp['url'])
                    if url in update_list:
                        f3.write('{},{}\n'.format(url, temp['uid']))

    # i = 0
    # with open(url_uid_name, 'a') as f3:
    #     for img in update_list:
    #         while i < len(url_lst):
    #             if img == url_lst[i]:
    #                 f3.write('{},{}\n'.format(url_lst[i], uid_lst[i]))
    #                 i += 1
    #                 break
    #             i += 1
            
