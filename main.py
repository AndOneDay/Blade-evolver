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
CHECK_INTERVAL = 600      # second 
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
        main.py                         [-c|--cleanup] [--manual=str]
        main.py                         -v|--version
        main.py                         -h|--help

    Arguments:

    Options:
        -h --help                       show this screen
        -v --version                    show script version
        -c --cleanup                    just clean up cache dir
        ------------------------------------------------------------------
        --manual=str                    manually run process, original log  
                                        need to be pre-downloaded. input 
                                        date syntax: 2018-08-12
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
    global YEST_DATE, BEF_YEST_DATE
    # initialization date
    if args['--manual']:
        temp_date = args['--manual'].split('-')
        assert len(temp_date) == 3, logger.error('Manual date syntax error')
        YEST_DATE = datetime.date(int(temp_date[0]),int(temp_date[1]),int(temp_date[2])).strftime('%Y-%m-%d')
        BEF_YEST_DATE = (datetime.date(int(temp_date[0]),int(temp_date[1]),int(temp_date[2])) - datetime.timedelta(1)).strftime('%Y-%m-%d')
    ORI_LOG_NAME = 'qpulp_origin_{}.json'.format(''.join(YEST_DATE.split('-')))
    FLT_LOG_NAME = 'pulp_{}.lst'.format(''.join(YEST_DATE.split('-')))
    DEP_FILE_NAME = 'base_depot_DailyDiary_{}.json'.format(''.join(BEF_YEST_DATE.split('-')))
    UPD_DEP_FILE_NAME = 'base_depot_DailyDiary_{}.json'.format(''.join(YEST_DATE.split('-')))
    logger.info('Processing date: {}'.format(YEST_DATE))
    # logger.info('Processing date: {}'.format(BEF_YEST_DATE))

    # ---- phase 1 ----
    logger.info('PHASE[1] => fetching original log')
    if not args['--manual']:
        logger.info('Creating log_proxy configuration...')
        conf_path = os.path.join(cur_path, CACHE_PATH, 'log_proxy_{}.conf'.format(YEST_DATE))
        exec_path = os.path.join(cur_path, 'tools', 'log_proxy')
        jobid_path = os.path.join(cur_path, CACHE_PATH, 'job_id.log')
        create_conf(YEST_DATE, (AK,SK), ORI_LOG_NAME, conf_path=conf_path)
        logger.info('Submitting log_proxy job...')
        submit_ret = submit_job(exec_path, conf_path, jobid_path)
        if not submit_ret:
            logger.error('Submitting log_proxy job failed, try to check exec file exsistance or permission')
            return 0
        else:
            logger.info('\n'+submit_ret)
        checkpoint = 0
        checkflag = False
        while(checkpoint<(float(MAX_CHECK_TIME)/CHECK_INTERVAL)):
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
            return 0
    else:
        logger.info('PHASE[1] skipped under manual mode')

    # ---- phase 2 ----
    logger.info('PHASE[2] => downloading original log file')
    exec_path = os.path.join(cur_path, 'tools', 'qshell')
    logger.info('Login qshell...')
    if log_in(exec_path, (AK,SK)):
        logger.error('Logging failed.')
        return 0
    logger.info('Logged with {}'.format(commands.getoutput('{} account|grep AccessKey'.format(exec_path))))
    logger.info('Checking log exsistance...')
    lstbkt_path = os.path.join(CACHE_PATH, ORI_LOG_BKT+'.listbucket')
    if list_bkt(exec_path, lstbkt_path, bucket=ORI_LOG_BKT):
        logger.error('Listing bucket failed.')
        return 0
    logger.info('Saved listbucket result file as {}'.format(lstbkt_path))
    logger.info('Loading bucket file list...')
    bkt_file_list = load_bkt(lstbkt_path)
    logger.info('{} files found.'.format(len(bkt_file_list)))
    if ORI_LOG_NAME not in bkt_file_list:
        print(ORI_LOG_NAME)
        logger.error('Original log file not found.')
        return 0
    logger.info('Downloading original log file...')
    if not ss_download(exec_path, ORI_LOG_DOM, ORI_LOG_NAME, CACHE_PATH):
        logger.error('Downloading failed.')
        return 0
    logger.info('PHASE[2] => success.')

    # ---- phase 3 ----
    logger.info('PHASE[3] => filtering interestesd logs and do deduplication')
    filtered_list = log_filter(os.path.join(CACHE_PATH, ORI_LOG_NAME))
    if not filtered_list:
        logger.error('Filter image list failed.')
        return 0
    logger.info('Filtered {} images.'.format(len(filtered_list)))
    temp_file = os.path.join(CACHE_PATH, '_temp_pulp_img.lst')
    temp_hash = os.path.join(CACHE_PATH, '_temp_pulp_hash.json')
    with open(temp_file, 'w') as f:
        for line in filtered_list:
            f.write('{}\n'.format(line))
    logger.info('Filtered image-list saved as ' + temp_file)
    logger.info('Downloading depot file...')
    if not ss_download(exec_path, DEP_FILE_DOM, DEP_FILE_NAME, CACHE_PATH):
        logger.error('Downloading failed.')
        return 0
    logger.info('Fetching hash...')
    exec_path = os.path.join(cur_path, 'tools', 'qhash_proxy')
    fetch_hash = qhash(exec_path, temp_file, REMOTE_IMG_PREFIX, temp_hash)
    if fetch_hash:
        logger.info('Fetching hash failed.')
        return 0
    logger.info('Deduplicating...')
    dep_name = os.path.join(CACHE_PATH, DEP_FILE_NAME)
    upd_dep_name = os.path.join(CACHE_PATH, UPD_DEP_FILE_NAME)
    flt_log_name = os.path.join(CACHE_PATH, FLT_LOG_NAME)
    deduplicate(dep_name, temp_hash, upd_dep_name, flt_log_name, REMOTE_IMG_PREFIX)
    logger.info('Uploading...')
    exec_path = os.path.join(cur_path, 'tools', 'qshell')
    if not upload(exec_path, DEP_FILE_BKT, UPD_DEP_FILE_NAME, upd_dep_name) or not upload(exec_path, FLT_LOG_BKT, FLT_LOG_NAME, flt_log_name):
        logger.error('Uploading result file failed.')
        return 0
    logger.info('PHASE[3] => success.')
    

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
