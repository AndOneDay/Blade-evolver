import os
import datetime
import time

# start_date_str = '2018-08-19'
# end_date_str = '2018-08-28'

def main():
    start_date_str = '2018-08-19'
    end_date_str = '2018-08-28'
    while start_date_str != end_date_str:
        print('fetch date', start_date_str)
        flag = os.system('python ../main.py --manual=' + start_date_str + ' --cls=pulp')
        if flag:
            print('fetch pulp failed', start_date_str)
            break
        flag = os.system('python ../main.py --manual=' + start_date_str + ' --cls=normal')
        if flag:
            print('fetch normal failed', start_date_str)
            break
        start_time = datetime.datetime.strptime(start_date_str,'%Y-%m-%d')
        start_time += datetime.timedelta(days = 1)
        start_date_str = datetime.datetime.strftime(start_time, '%Y-%m-%d')
        time.sleep(5)


if __name__ == '__main__':
    main()