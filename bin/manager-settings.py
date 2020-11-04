#!/bin/python
# -*- encoding:utf8 -*-
import os, sys, time, ConfigParser, optparse

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)


class Main:
    def __init__(self, options):
        self.github = 'https://github.com/TES-SYSTEM/xbit-settings.git'
        self.options = options
        self.settings_directory = os.path.join(BASE_DIRECTORY, 'settings')

        # 지정된 옵션으로 실행하기
        if self.options.initialize:
            self.initialize()

    def initialize(self):
        ''' settings 파일 초기화 '''
        try:
            os.makedirs(self.settings_directory)
        except OSError:
            pass

        os.chdir(BASE_DIRECTORY)
        cmd = "git clone -b {branch} --single-branch {github} {settings_directory}".format(branch=self.options.branch, github=self.github, settings_directory=self.settings_directory)
        os.system(cmd)

if __name__ == '__main__':
    parser = optparse.OptionParser(version='0.0.1')
    parser.add_option("--init", action='store_true', default=False,
                      help="initialize settings files",
                      dest="initialize")
    parser.add_option("-b", "--branch", action='store',
                      help="select branch",
                      dest="branch")
    options, args = parser.parse_args()

    if options.initialize != False and options.branch is not None:
        Main(options=options)
    else:
        parser.print_help()
