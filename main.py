#!/usr/bin/env python
# -*- coding: utf-8 -*-
# created 2018/08/09
# by Northrend#github.com
#
# "Blade-evolver" self-iterating system 
#

from __future__ import print_function
import os
import sys
import commands
import re
import json
import time
import datetime
import docopt
import logging
import ConfigParser

cur_path = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(cur_path,'lib'))
from log_proxy_wrapper import create_conf, submit_job, check_job
from qshell_wrapper import log_in, list_bkt, load_bkt, ss_download, upload
from data_hub import humanize_bytes, log_filter, qhash, deduplicate 

# init global logger
log_format = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, format=log_format)
logger = logging.getLogger()  
fhandler = None 

# steady params
try:
    with open('account.conf','r') as f:
        account = json.load(f)
except:
    logger.error('Load account configuration failed.')
    sys.exit()
AK = account['AK'] 
SK = account['SK']
LOG_LEVEL = 'INFO'
LOG_PATH = 'runtime_log'
CACHE_PATH = 'runtime_cache'
CUR_TIME = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
CUR_DATE = datetime.datetime.now().strftime('%Y-%m-%d')
YEST_DATE = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
# YEST_DATE = '2018-07-03'
BEF_YEST_DATE = (datetime.datetime.now() - datetime.timedelta(2)).strftime('%Y-%m-%d')
# BEF_YEST_DATE = '2018-07-02'

# mutable params
CHECK_INTERVAL = 120      # second
MAX_CHECK_TIME = 7200   # second
REMOTE_IMG_PREFIX = 'http://oquqvdmso.bkt.clouddn.com/atflow-log-proxy/images/'
ORI_LOG_BKT = 'qpulp-log'
ORI_LOG_DOM = 'http://p4yeqgehs.bkt.clouddn.com'
# ORI_LOG_NAME = 'qpulp_origin_{}.json'.format(''.join(YEST_DATE.split('-')))
FLT_LOG_BKT = 'qpulp-pulp-list'
# FLT_LOG_NAME = 'pulp_{}.lst'.format(''.join(YEST_DATE.split('-')))
DEP_FILE_BKT = 'qpulp-depot'
DEP_FILE_DOM = 'http://p66q12vsa.bkt.clouddn.com'
# DEP_FILE_NAME = 'base_depot_DailyDiary_{}.json'.format(''.join(BEF_YEST_DATE.split('-')))
# UPD_DEP_FILE_NAME = 'base_depot_DailyDiary_{}.json'.format(''.join(YEST_DATE.split('-')))


class generic_error():
    '''
    TODO
    '''
    pass


def _init_():
    """
    "Blade-evolver" self-iterating system 
    Update: 2018/08/13
    Contributor:

    Change log:
    2018/08/13      v1.2                fix manual bug 
    2018/08/12      v1.1                support manual mode 
    2018/08/09      v1.0                basic functions

    Usage:
        main.py                         [-c|--cleanup] [--manual=str] [--cls=str][--only_pull_log][--pull]
        main.py                         -v|--version
        main.py                         -h|--help

    Arguments:

    Options:
        -h --help                       show this screen
        -v --version                    show script version
        -c --cleanup                    just clean up cache dir
        --only_pull_log                 pull log
        --pull                          pull method
        ------------------------------------------------------------------
        --manual=str                    manually run process, original log  
                                        need to be pre-downloaded. input 
                                        date syntax: 2018-08-12
        --cls=str                       [default: pulp]
    """
    # config logger
    logger.setLevel(eval('logging.' + LOG_LEVEL))
    fhandler = logging.FileHandler(os.path.join(LOG_PATH, CUR_TIME+'.log'), mode='w')
    logger.addHandler(fhandler)

    # print arguments
    logger.info('=' * 80 + '\nCalled with arguments:')
    for key in sorted(args.keys()):
        logger.info('{:<20}= {}'.format(key.replace('--', ''), args[key]))
    logger.info('=' * 80)

    # reset logger format
    fhandler.setFormatter(logging.Formatter(log_format))


def whole_routine():
    assert args['--cls'] in ['pulp', 'normal', 'sexy'], 'only pulp or normal class support'
    global YEST_DATE, BEF_YEST_DATE
    # initialization date
    if args['--manual']:
        temp_date = args['--manual'].split('-')
        assert len(temp_date) == 3, logger.error('Manual date syntax error')
        YEST_DATE = datetime.date(int(temp_date[0]),int(temp_date[1]),int(temp_date[2])).strftime('%Y-%m-%d')
        BEF_YEST_DATE = (datetime.date(int(temp_date[0]),int(temp_date[1]),int(temp_date[2])) - datetime.timedelta(1)).strftime('%Y-%m-%d')

    ORI_LOG_NAME = 'qpulp_origin_{}.json'.format(''.join(YEST_DATE.split('-')))
    FLT_LOG_NAME = '{}_{}.lst'.format(args['--cls'], ''.join(YEST_DATE.split('-')))
    UID_LOG_NAME = 'qpulp_uid_{}.csv'.format(''.join(YEST_DATE.split('-')))
    # if args.cls == 'normal':
    #     FLT_LOG_NAME = 'normal_{}.lst'.format(''.join(YEST_DATE.split('-')))
    DEP_FILE_NAME = 'base_depot_DailyDiary_{}.json'.format(''.join(BEF_YEST_DATE.split('-')))
    UPD_DEP_FILE_NAME = 'base_depot_DailyDiary_{}.json'.format(''.join(YEST_DATE.split('-')))
    logger.info('Processing date: {}'.format(YEST_DATE))
    # logger.info('Processing date: {}'.format(BEF_YEST_DATE))

    #login qshell
    exec_path = os.path.join(cur_path, 'tools', 'qshell')
    if log_in(exec_path, (AK,SK)):
        logger.error('Logging failed.')
        return 0
    # ---- phase 1 ----
    logger.info('PHASE[1] => fetching original log')
    if not file_exist('qshell', ORI_LOG_NAME, ORI_LOG_BKT):
        logger.info('Creating log_proxy configuration...')
        conf_path = os.path.join(cur_path, CACHE_PATH, 'log_proxy_{}.conf'.format(YEST_DATE))
        exec_path = os.path.join(cur_path, 'tools', 'log_proxy')
        jobid_path = os.path.join(cur_path, CACHE_PATH, 'job_id.log')

        pull_success_flag = True
        if not args['--pull']:
            pull_log(ORI_LOG_NAME, conf_path, exec_path, jobid_path)
        else:
            pull_times = 8
            cmd = 'cat'
            for i in range(pull_times):
                time_len = 24 / pull_times
                st = i * time_len
                et = (i + 1) * time_len
                start_time = '{:02d}:00:00'.format(st)
                end_time = '{:02d}:59:59'.format(et - 1)
                exec_path = os.path.join(cur_path, 'tools', 'log_proxy')
                pull_log(ORI_LOG_NAME + '.' + str(i), conf_path, exec_path, jobid_path, start_time=start_time,
                         end_time=end_time)
                exec_path = os.path.join(cur_path, 'tools', 'qshell')
                ss_download(exec_path, ORI_LOG_DOM, ORI_LOG_NAME + '.' + str(i), CACHE_PATH)
                tmd_cache_file = os.path.join(CACHE_PATH, ORI_LOG_NAME + '.' + str(i))
                if (not os.path.exists(tmd_cache_file)) or (os.path.getsize(tmd_cache_file) < 20 * 1024 * 1024):
                    pull_success_flag = False
                    break
                cmd += ' runtime_cache/{}'.format(ORI_LOG_NAME + '.' + str(i))

            cmd += ' > runtime_cache/{}'.format(ORI_LOG_NAME)
            if pull_success_flag:
                os.system(cmd)
                ori_log_name = os.path.join(CACHE_PATH, ORI_LOG_NAME)
                if upload(exec_path, ORI_LOG_BKT, ORI_LOG_NAME, ori_log_name):
                    logger.error('Uploading ori_log file failed.')
                    return 1
                else:
                    exec_path = os.path.join(cur_path, 'tools', 'qshell')
                    for i in range(pull_times):
                        delete_log = '{}'.format(ORI_LOG_NAME + '.' + str(i))
                        delete_cmd = '{} delete {} {}'.format(exec_path, ORI_LOG_BKT, delete_log)
                        os.system(delete_cmd)
            else:
                print('pull log failed')

    else:
        logger.info('PHASE[1] skipped because ori_log exist')
    if args['--only_pull_log']:
        logger.info('only pull log')
        return 0
    # else:
    #     logger.info('continue to PHASE 2')
    # ---- phase 2 ----
    logger.info('PHASE[2] => downloading original log file')
    exec_path = os.path.join(cur_path, 'tools', 'qshell')
    logger.info('Login qshell...')
    if log_in(exec_path, (AK,SK)):
        logger.error('Logging failed.')
        return 1
    logger.info('Logged with {}'.format(commands.getoutput('{} account|grep AccessKey'.format(exec_path))))
    #logger.info('Checking log exsistance...')
    if not file_exist('qshell', ORI_LOG_NAME, ORI_LOG_BKT):
        return 1
    logger.info('Downloading original log file...')
    if not ss_download(exec_path, ORI_LOG_DOM, ORI_LOG_NAME, CACHE_PATH):
        logger.error('Downloading failed.')
        return 1
    logger.info('PHASE[2] => success.')

    # ---- phase 3 ----
    logger.info('PHASE[3] => filtering interestesd logs and do deduplication')
    filtered_list, url_uid_map = log_filter(os.path.join(CACHE_PATH, ORI_LOG_NAME), args['--cls'])
    if not filtered_list:
        logger.error('Filter image list failed.')
        return 1
    logger.info('Filtered {} images.'.format(len(filtered_list)))
    temp_file = os.path.join(CACHE_PATH, '_temp_pulp_img.lst')
    temp_hash = os.path.join(CACHE_PATH, '_temp_pulp_hash.json')
    with open(temp_file, 'w') as f:
        for line in filtered_list:
            f.write('{}\n'.format(line))
    logger.info('Filtered image-list saved as ' + temp_file)
    logger.info('Checking depot file exsistance...')
    #add list depot to check yesterday depot file exsist
    if not file_exist('qshell', DEP_FILE_NAME, DEP_FILE_BKT):
        return 1

    logger.info('Downloading depot file...')
    if file_exist('qshell', UPD_DEP_FILE_NAME, DEP_FILE_BKT):
        if not ss_download(exec_path, DEP_FILE_DOM, UPD_DEP_FILE_NAME, CACHE_PATH, '.bak'):
            logger.error('Downloading failed.')
            return 1
        DEP_FILE_NAME = UPD_DEP_FILE_NAME + '.bak'
    else:
        if not ss_download(exec_path, DEP_FILE_DOM, DEP_FILE_NAME, CACHE_PATH):
            logger.error('Downloading failed.')
            return 1
    logger.info('Fetching hash...')
    exec_path = os.path.join(cur_path, 'tools', 'qhash_proxy')
    fetch_hash = qhash(exec_path, temp_file, REMOTE_IMG_PREFIX, temp_hash)
    if fetch_hash:
        logger.info('Fetching hash failed.')
        return 1
    logger.info('Deduplicating...')
    dep_name = os.path.join(CACHE_PATH, DEP_FILE_NAME)
    upd_dep_name = os.path.join(CACHE_PATH, UPD_DEP_FILE_NAME)
    flt_log_name = os.path.join(CACHE_PATH, FLT_LOG_NAME)
    url_uid_name = os.path.join(CACHE_PATH, UID_LOG_NAME)
    deduplicate(dep_name, temp_hash, upd_dep_name, flt_log_name, REMOTE_IMG_PREFIX, url_uid_name, url_uid_map)
    #use to debug
    #return 1
    logger.info('Uploading...')
    exec_path = os.path.join(cur_path, 'tools', 'qshell')
    if upload(exec_path, DEP_FILE_BKT, UPD_DEP_FILE_NAME, upd_dep_name, overwrite='true') or upload(exec_path, FLT_LOG_BKT, FLT_LOG_NAME, flt_log_name):
        logger.error('Uploading result file failed.')
        return 1
    logger.info('PHASE[3] => success.')


def pull_log(ORI_LOG_NAME, conf_path, exec_path, jobid_path, start_time=None, end_time=None):
    if start_time is not None and end_time is not None:
        create_conf(YEST_DATE, (AK, SK), ORI_LOG_NAME, conf_path=conf_path, start_time=start_time, end_time=end_time)
    else:
        create_conf(YEST_DATE, (AK, SK), ORI_LOG_NAME, conf_path=conf_path)
    logger.info('Submitting log_proxy job...')
    submit_ret = submit_job(exec_path, conf_path, jobid_path)
    if not submit_ret:
        logger.error('Submitting log_proxy job failed, try to check exec file exsistance or permission')
        return 1
    else:
        logger.info('\n' + submit_ret)
    checkpoint = 0
    checkflag = False
    while (checkpoint < (float(MAX_CHECK_TIME) / CHECK_INTERVAL)):
        checkpoint += 1
        job_id, check_ret = check_job(exec_path, conf_path, jobid_path)
        if checkpoint == 1:
            logger.info('Checking log_proxy job status...')
            logger.info('Job id -> {}'.format(job_id))
        logger.info('Checkpoint[{}] -> {}'.format(checkpoint, check_ret))
        if check_ret == 'done':
            checkflag = True
            break
        time.sleep(CHECK_INTERVAL)
    if checkflag:
        logger.info('PHASE[1] => success.')
    else:
        logger.error('PHASE[1] => timeout.')
        return 1

def file_exist(tool, bkt_file_name, bkt_name):
    logger.info('Checking log exsistance...')
    lstbkt_path = os.path.join(CACHE_PATH, bkt_name + '.listbucket')
    exec_path = os.path.join(cur_path, 'tools', tool)
    # logger.info('Login qshell...')
    # if log_in(exec_path, (AK, SK)):
    #     logger.error('Logging failed.')
    #     return False
    if list_bkt(exec_path, lstbkt_path, bucket=bkt_name):
        logger.error('Listing bucket failed.')
        return False
    logger.info('Saved listbucket result file as {}'.format(lstbkt_path))
    logger.info('Loading bucket file list...')
    bkt_file_list = load_bkt(lstbkt_path)
    logger.info('{} files found.'.format(len(bkt_file_list)))
    if bkt_file_name not in bkt_file_list:
        print(bkt_file_name)
        logger.info('the file not found.')
        return False
    else:
        return True

def clean_up():
    logger.info('Clean up job...')
    cache_list = os.listdir(CACHE_PATH)
    logger.info('{} cache files found:'.format(len(cache_list)))
    for cache_file in cache_list:
        logger.info('{:<36}\t{}'.format(cache_file, humanize_bytes(os.path.getsize(os.path.join(CACHE_PATH, cache_file)))))
        os.remove(os.path.join(CACHE_PATH, cache_file))
    logger.info('All cache files cleaned up.')

def main():
    if args['--cleanup']:
        clean_up()
    else:
        whole_routine()


if __name__ == '__main__':
    version = re.compile('.*\d+/\d+\s+(v[\d.]+)').findall(_init_.__doc__)[0]
    args = docopt.docopt(
        _init_.__doc__, version='Blade-evolver {}'.format(version))
    _init_()
    logger.info('Start daily job...')
    main()
    logger.info('...Done')
