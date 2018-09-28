import os
import sys
import commands

def log_in(exec_path, aksk):
    return os.system("{} account {} {}".format(exec_path, aksk[0], aksk[1]))

def list_bkt(exec_path, result_path, bucket='qpulp-log'):
    return os.system('{} listbucket {} {}'.format(exec_path, bucket, result_path)) 

def load_bkt(result_path):
    result = list()
    with open(result_path, 'r') as f:
        for buff in f.readlines():
            result.append(buff.strip().split('\t')[0])
    return result

def upload(exec_path, bucket, target_file, source_file, overwrite=''):
    upload_cmd = '{} rput {} {} {}'.format(exec_path, bucket, target_file, source_file)
    if overwrite == 'true':
        upload_cmd += (' ' + overwrite)
    return os.system(upload_cmd)

def get_url():
    return commands.getoutput("{} privateurl {}".format())

def ss_download(exec_path, domain, log_name, save_path, suffix=''):
    '''
    :params:
    domain:  
    '''
    src_st = 'http://nbxs-gate-io.qiniu.com'
    public_url = os.path.join(domain, log_name)
    private_url = commands.getoutput("{} privateurl {}".format(exec_path, public_url)).split('\n')[-1]  # only qshell version v2.1.8 supported
    #print(private_url)
    download_cmd = 'curl "{}" -H "Host: {}" -o {}'.format(private_url.replace(domain, src_st), os.path.basename(domain), os.path.join(save_path, log_name + suffix))
    #print(download_cmd)
    os.system(download_cmd)
    if os.path.exists(os.path.join(save_path, log_name)):
        return 1
    else:
        return 0

