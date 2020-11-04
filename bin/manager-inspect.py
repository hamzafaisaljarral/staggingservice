#!/bin/python
# -*- encoding:utf8 -*-
import os, sys, time, ConfigParser, optparse, glob

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)

class Main:
    def __init__(self):
        filename = '/var/spool/cron/root'
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                print f.read()
        else:
            with open(filename, 'w') as f:
                f.write("* * * * * /usr/bin/python /data/xbit/bin/manager-log.py  # every minute execute")
            print 'Create crontab'


if __name__ == '__main__':
    if os.getuid() != 0:  # 관리자 권한으로 실행되는지 확인
        print 'Permission denied :$ sudo {manager}'.format(manager=os.path.basename(__file__))
        exit(1)
    Main()