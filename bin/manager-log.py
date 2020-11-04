#!/bin/python
# -*- encoding:utf8 -*-
import os, sys, ConfigParser, glob, socket, zipfile
from boto.s3.connection import S3Connection
from boto.s3.key import Key

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)

from lib.exchange import exchangeConfigParser

### CONFIG PARSERING ###
config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'},
    ],
    'S3': [
        {'key': 'aws_bucket', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_access_key_id', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_secret_access_key', 'method': 'get', 'defaultvalue': None},
    ],

}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    print CONFIG.errormsg
    exit(1)


class S3:
    con = None

    @classmethod
    def get_config(cls):
        cls.con = S3Connection(CONFIG.S3['aws_access_key_id'], CONFIG.S3['aws_secret_access_key'])
        cls.bucket = cls.con.get_bucket(CONFIG.S3['aws_bucket'])

    @classmethod
    def connect(cls):
        if cls.con == None:
            cls.get_config()

    @classmethod
    def set_filename(cls, key, filename):
        cls.connect()

        s3key = Key(cls.bucket)
        s3key.key = key
        s3key.set_contents_from_filename(filename)

    @classmethod
    def get(cls, key):
        cls.connect()
        return cls.bucket.get_key(key)


class Main:
    def directory_parser(self, buf, directory, extension=None):
        for fn in glob.glob(os.path.join(directory, '*')):
            if os.path.isdir(fn):
                buf = self.directory_parser(buf=buf, directory=fn, extension=extension)
            else:
                if extension == None:
                    buf.append(os.path.abspath(fn))
                else:
                    if fn.rsplit('.')[-1].lower() == extension.lower():
                        buf.append(os.path.abspath(fn))
        return buf

    def __init__(self):
        ### 로그파일 압축하고 삭제하기

        # 로그 파일만 뽑아내기
        self.logfile_list = list()
        self.logfile_list = self.directory_parser(buf=self.logfile_list, directory=CONFIG.LOG['directory'], extension='log')

        # 로그파일 디렉토리 와 파일명으로 구분짓기
        self.logfile_dict = dict()
        for logfile in self.logfile_list:
            dname, fname = os.path.dirname(logfile), os.path.basename(logfile)

            try:
                self.logfile_dict[dname].append(fname)
            except KeyError:
                self.logfile_dict[dname] = list()
                self.logfile_dict[dname].append(fname)

        # 디렉토리별로 마지막 파일 제외하고 압축 및 삭제
        for dname in self.logfile_dict:
            # os.chdir(dname)
            for fname in sorted(self.logfile_dict[dname])[:-1]:  # 오름차순으로 정렬, 마지막 파일 제외
                zname = fname + '.zip'
                # print dname, fname, zname

                with zipfile.ZipFile(os.path.join(dname, zname), 'w') as zip:
                    zip.write(os.path.join(dname, fname), fname, compress_type=zipfile.ZIP_DEFLATED)
                os.remove(os.path.join(dname, fname))  # 삭제

        ### 압축파일 업로드하고 삭제하기
        # 압축 파일만 뽑아내기
        self.zipfile_list = list()
        self.zipfile_list = self.directory_parser(buf=self.zipfile_list, directory=CONFIG.LOG['directory'], extension='zip')

        # 압축파일 디렉토리 와 파일명으로 구분짓기
        self.zipfile_dict = dict()
        for logfile in self.zipfile_list:
            dname, fname = os.path.dirname(logfile), os.path.basename(logfile)

            try:
                self.zipfile_dict[dname].append(fname)
            except KeyError:
                self.zipfile_dict[dname] = list()
                self.zipfile_dict[dname].append(fname)

        for dname in self.zipfile_dict:
            for zname in sorted(self.zipfile_dict[dname]):
                s3_dname = dname.replace('/', '_')
                s3key = '/logs/{hostname}/{s3_dname}/{zname}'.format(hostname=socket.gethostname(), s3_dname=s3_dname, zname=zname)
                S3.set_filename(key=s3key, filename=os.path.join(dname, zname))
                os.remove(os.path.join(dname, zname))


Main()
