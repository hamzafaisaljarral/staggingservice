#!/bin/python
# -*- encoding:utf8 -*-
import os, sys, time, optparse

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)

class Main:
    def __init__(self, pidfile):
        # 확장명 pid 검사
        fns = pidfile.rsplit('.', 1)
        if len(fns) <= 1:
            validpidfile = '{pidfile}.pid'.format(pidfile=pidfile)
        else:
            if fns[-1].lower() == 'pid':
                validpidfile = pidfile
            else:
                validpidfile = '{pidfile}.pid'.format(pidfile=pidfile)

        RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
        killfile_format = '{pid}.kill'

        if os.path.exists(os.path.join(RUN_DIRECTORY, validpidfile)):
            with open(os.path.join(RUN_DIRECTORY, validpidfile)) as f:
                pid = f.read()
            with open(os.path.join(RUN_DIRECTORY, killfile_format.format(pid=pid)), 'w') as f:
                pass

if __name__ == '__main__':
    # option load
    parser = optparse.OptionParser()
    parser.add_option("-p", "--pidfile", action='store',
                      dest="pidfile")
    options, args = parser.parse_args()
    if options.pidfile is None:
        parser.print_help(sys.stdout)
        exit(1)

    main = Main(pidfile=options.pidfile)
