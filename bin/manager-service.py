#!/bin/python
# -*- encoding:utf8 -*-
import os, sys, time, ConfigParser, optparse, glob

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)


class Main:
    def __init__(self, options, args):
        self.options = options
        self.args = args
        self.service_directory = os.path.join(BASE_DIRECTORY, 'service')

        self.run()

    def run(self):
        self.service_set = dict()
        for idx, service in enumerate(sorted(glob.glob(os.path.join(self.service_directory, '*.service'))), 1):
            self.service_set[idx] = {
                'idx': idx,
                'service': os.path.basename(service),
                'src_path': service,
                'dst_path': os.path.join('/usr/lib/systemd/system', os.path.basename(service)),
            }

        if self.options.list:
            self.list()
        elif self.options.add:
            self.add()
        elif self.options.remove:
            self.remove()
        elif self.options.status:
            self.status()

    def list(self):
        s = '{idx:>3} {regist:>6} {service}'
        for idx in sorted(self.service_set):
            service = self.service_set[idx]
            regist = os.path.exists(service['dst_path'])
            print s.format(idx=idx, regist=str(regist), service=service['service'])

    def add(self):
        for idx in self.args:
            idx = int(idx)
            service = self.service_set[idx]
            cmd = 'sudo ln -s {src_path} {dst_path}'.format(src_path=service['src_path'], dst_path=service['dst_path'])
            print cmd
            os.system(cmd)

    def remove(self):
        for idx in self.args:
            idx = int(idx)
            service = self.service_set[idx]
            print 'remove {dst_path}'.format(dst_path=service['dst_path'])
            try:
                os.remove(service['dst_path'])
            except OSError:
                pass

    def status(self):
        cmd = "sudo systemctl daemon-reload"
        print cmd
        os.system(cmd)
        for idx in self.service_set:
            idx = int(idx)
            service = self.service_set[idx]
            regist = os.path.exists(service['dst_path'])
            if regist:
                cmd = "sudo systemctl status -n 0 {service}".format(service=service['service'])
                print cmd
                os.system(cmd)
                print

if __name__ == '__main__':
    # if os.getuid() != 0:  # 관리자 권한으로 실행되는지 확인
    #     print 'Permission denied :$ sudo {manager}'.format(manager=os.path.basename(__file__))
    #     exit(1)

    parser = optparse.OptionParser(version='0.0.2')
    parser.add_option("-l", "--list", action='store_true', default=False,
                      help="service list",
                      dest="list")
    parser.add_option("--status", action='store_true', default=False,
                      help="service status",
                      dest="status")
    parser.add_option("--add", action='store_true', default=False,
                      help="service add",
                      dest="add")
    parser.add_option("--remove", action='store_true', default=False,
                      help="service remove",
                      dest="remove")

    options, args = parser.parse_args()
    if options.list == False and options.add == False and options.remove == False and options.status == False:
        parser.print_help()
        exit(0)
    Main(options=options, args=args)
