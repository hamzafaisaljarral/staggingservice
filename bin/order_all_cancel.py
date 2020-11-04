# -*- encoding:utf8 -*-
import os, sys, time, json, optparse, traceback, requests, urllib, MySQLdb, uuid, redis, random, multiprocessing
import numpy as np
from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import exchange, exchangeConfigParser

### CONFIG PARSERING ###
config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'}
    ],
    'STORAGEDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'user', 'method': 'get', 'defaultvalue': None},
        {'key': 'passwd', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'get', 'defaultvalue': None},
    ],
    'STORAGEDB_RO': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'user', 'method': 'get', 'defaultvalue': None},
        {'key': 'passwd', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'get', 'defaultvalue': None},
    ],
    'REQUESTDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
        {'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
    'RESPONSEDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
        {'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
    'WALLETCACHE': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
        {'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    print CONFIG.errormsg
    exit(1)

con = MySQLdb.connect(host=CONFIG.STORAGEDB_RO['host'], user=CONFIG.STORAGEDB_RO['user'], passwd=CONFIG.STORAGEDB_RO['passwd'], db=CONFIG.STORAGEDB_RO['db'], port=CONFIG.STORAGEDB_RO['port'])
cur = con.cursor()

requestdb = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'], db=CONFIG.REQUESTDB['db'], password=CONFIG.REQUESTDB['password'])

cur.execute("SELECT product, order_id, member_id FROM `order:OPEN`;")
for product, order_id, member_id in cur.fetchall():
    cur.execute("SELECT wallet_id, NULL FROM member WHERE member_id=%s;", (member_id, ))
    wallet_id, _ = cur.fetchone()
    print product, order_id, member_id, wallet_id
    orderset = {'product': product,
                'order_type': 'CANCEL',
                'member_id': member_id,
                'order_id': order_id,
                'wallet_id': wallet_id,
                }
    requestdb.lpush('ORDERSEND', json.dumps(orderset))

