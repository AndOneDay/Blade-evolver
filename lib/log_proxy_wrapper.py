import os
import sys
import commands
import base64


def create_conf(date, aksk, log_fname, conf_path='./log_proxy.conf', start_time='00:00:00', end_time='23:59:59'):
    '''
    TODO(Northrend@github.com): use recompiler to just modify on template.conf file
    :params: 
    date(str): date to fetch log, eg. 2018-08-02 
    '''
    with open(conf_path,'w') as f:
        f.write('# configuration file for log_proxy.py\n\n')
        f.write('[keys]\n')
        f.write('# avatest\n')
        f.write('ak = {}\n'.format(aksk[0]))
        f.write('sk = {}\n\n'.format(aksk[1]))
        f.write('[params]\n')
        f.write('cmd = pulp\n')
        f.write('start_time = {}T{}\n'.format(date, start_time))
        # f.write('end_time = {}T00:09:59\n'.format(date))    # debugging
        f.write('end_time = {}T{}\n'.format(date, end_time))
        f.write('uid = \n')
        f.write('key = {}\n'.format(log_fname))
        f.write('bucket = qpulp-log\n')
        f.write('prefix = \n')
        f.write('query = label[?type==\'classification\' && name==\'pulp\']\n')


def submit_job(exec_path, conf_path, jobid_path):
    # check file permission
    if not os.access(exec_path, os.X_OK):
        return None 
    return commands.getoutput('{} {} -s --job-id-log {}'.format(exec_path, conf_path, jobid_path))

def check_job(exec_path, conf_path, jobid_path):
    # load job id 
    with open(jobid_path, 'r') as f:
        job_id = f.readlines()[-1].split()[-1]
    return job_id, commands.getoutput('{} {} -c --job-id {}|grep status'.format(exec_path, conf_path, job_id)).split("'")[-2]
