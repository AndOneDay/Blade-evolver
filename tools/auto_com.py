import os
import datetime
import time

start_date_str = '2018-08-12'
end_date_str = '2018-08-16'

def main():
    '''
    '''
    while start_date_str != end_date_str:
        print('fetch date', start_date_str)
        os.system('python ../main.py --manual=' + start_date_str)
        start_time = datetime.datetime.strptime(start_date_str,'%Y-%m-%d')
        start_time += datetime.timedelta(days = 1)
        start_date_str = datetime.datetime.strftime(start_time, '%Y-%m-%d')
        time.sleep(60)


if __name__ == '__main__':
    main()
