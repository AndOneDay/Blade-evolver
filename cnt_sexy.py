import json
import os

log_name = './runtime_cache/qpulp_origin_20180905.json'
label = 'sexy'
filtered_list = []
with open(log_name, 'r') as f:
    for line in f.readlines():
        temp = json.loads(line.strip())
        if temp['label'][0]['data'] == None:
            continue
        elif len(temp['label'][0]['data']) < 3:
            continue
        else:
            scores = [(x['class'], x['score']) for x in temp['label'][0]['data']]
            # filter condition: pulp
            scores_sorted = sorted(scores, key=lambda x: x[1])
            if scores_sorted[-1][0] == label:
                filtered_list.append(os.path.basename(temp['url']))

print(len(filtered_list))