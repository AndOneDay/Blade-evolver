from __future__ import division
import os
import sys
import json


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
    with open(log_name, 'r') as f:
        for line in f.readlines():
            temp = json.loads(line.strip())
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
                elif label == 'sexy' and scores_sorted[-1][0] == 'sexy':
                    sexy_filtered_list.append((os.path.basename(temp['url']), scores_sorted[-1][1]))
                elif scores_sorted[-1][0] == label:
                    filtered_list.append(os.path.basename(temp['url']))
    if label == 'normal':
        normal_num = 100000
        normal_filtered_list = sorted(normal_filtered_list, key=lambda x: x[1])
        if len(normal_filtered_list) > normal_num:
            normal_filtered_list = normal_filtered_list[:normal_num]
        for item in normal_filtered_list:
            filtered_list.append(item[0])
    elif label == 'sexy':
        sexy_num = 100000
        sexy_filtered_list = sorted(sexy_filtered_list, key=lambda x: x[1])
        print('sexy len:', len(sexy_filtered_list))
        #if len(sexy_filtered_list) > sexy_num:
        #    sexy_filtered_list = sexy_filtered_list[:sexy_num]
        import random
        sexy_filtered_list = random.sample(sexy_filtered_list, sexy_num)
        for item in sexy_filtered_list:
            filtered_list.append(item[0])

    return filtered_list
                
    
def qhash(exec_path, img_list, url_prefix, output_path, thread_num=16):
    if not os.access(exec_path, os.X_OK):
        return 1 
    hash_cmd = '{} {} {} --prefix {} --output {} >/dev/null'.format(exec_path, img_list, thread_num, url_prefix, output_path)
    return os.system(hash_cmd)
    

def deduplicate(basic_file, delta_file, updated_file, uniq_delta_list, remote_img_prefix):
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
            
