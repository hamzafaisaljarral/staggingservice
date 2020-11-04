# -*- encoding:utf8 -*-
import os, sys, time, json, optparse, traceback, requests, urllib

import MySQLdb
from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import exchange, S3, exchangeConfigParser
import time, requests, json

config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'}
    ],
    'REQUESTDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
        #{'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
    'RESPONSEDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
        #{'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
    'STORAGEDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'user', 'method': 'get', 'defaultvalue': None},
        {'key': 'passwd', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'get', 'defaultvalue': None},
    ],
    'S3': [
        {'key': 'aws_access_key_id', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_secret_access_key', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_bucket', 'method': 'get', 'defaultvalue': None},
    ],
    'WALLETDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'user', 'method': 'get', 'defaultvalue': None},
        {'key': 'passwd', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'get', 'defaultvalue': None},
    ],
    'WALLETCACHE': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        #{'key': 'password', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': None},
    ],
}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    print CONFIG.errormsg
    exit(1)


class Main(exchange):
    def __init__(self, log):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log

        self.requestdb_init()
        self.responsedb_init()
        self.storagedb_init()
        S3.aws_s3_info_set(**self.CONFIG.S3)
        while True:
            try:
                self.before_run()
                self.run()
                self.after_run()
            except Exception as err:
                if DEBUG:
                    self.log.w('red', 'ERROR', traceback.format_exc(err))
                else:
                    self.log.w('red', 'ERROR', repr(traceback.format_exc(err)))
                exit(1)
            time.sleep(1)

    def before_run(self):
        self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
        if not os.path.exists(self.RUN_DIRECTORY):
            os.makedirs(self.RUN_DIRECTORY)

        ### PID 관련
        with open(os.path.join(self.RUN_DIRECTORY, 'xbit-wallet-withdraw-complete.pid'), 'w') as f:
            f.write(str(os.getpid()))

        self.kill_format = '{pid}.kill'
        pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))
        if os.path.exists(pidkill):
            os.remove(pidkill)
            exit(0)

        self.storagedb_check()

    def after_run(self):
        pass

    def run(self):
        s3_objects = S3.list('WITHDRAW/DONE')
        print("checking the s3_object",s3_objects)
        if s3_objects:
            self.storagedb_check()
            for done_key in s3_objects:
                print("the withdraw keys",done_key)
                SIDE, STATUS, self.currency_code, filename = done_key.key.split('/', 3)
                item = json.loads(done_key.read())
                self.currency_digit = item['currency_digit']
                item['done_time'] = int(time.time())
                r, kwargs = self.insert_wallet_history_done(item)
                if item['status'] == 5:  # 성공
                    # balance minus
                    self.wallet_withdraw(wallet_id=item['wallet_id'], amount=item['amount'], currency_code=item['currency_code'], fee=item['fee_amount'])
                    # notice
                    notice = self.sdb_notice_withdraw(member_id=item['member_id'], amount=item['amount'], fee=item['fee_amount'], notice_type=NOTICE_TYPE['WITHDRAW_COMPLETE'])

                    # response msg
                    response = self.ResponseInfo(response_code='WITHDRAW_COMPLETE', member_id=item['member_id'],
                                                 notice=notice)

                    # email msg
                    email_msg = {
                        'emailtype': EMAILTYPE['WithdrawComplete'],
                        'values': {
                            'wuuid': kwargs['uuid'],
                            'member_id': kwargs['member_id'],
                            'userid': kwargs['userid'],
                            'currency_code': kwargs['currency_code'],
                            'currency_digit': kwargs['currency_digit'],

                            'address': kwargs['address'],
                            'addr_nickname': kwargs['addr_nickname'],
                            'amount': kwargs['amount'],
                            'fee_amount': kwargs['fee_amount'],
                            'req_time': kwargs['req_time'],
                            'done_time': kwargs['done_time'],

                            'txid': kwargs['txid'],
                        },
                    }
                elif item['status'] == 6:  # 실패
                    # hold down
                    self.wallet_holddn(wallet_id=item['wallet_id'], amount=item['amount'] + item['fee_amount'], currency_code=item['currency_code'])
                    # notice
                    notice = self.sdb_notice_withdraw(member_id=item['member_id'], amount=item['amount'], fee=item['fee_amount'], notice_type=NOTICE_TYPE['WITHDRAW_FAIL'])
                    # response msg
                    response = self.ResponseInfo(response_code='WITHDRAW_FAIL', member_id=item['member_id'],
                                                 notice=notice)
                    # email msg
                    email_msg = {
                        'emailtype': EMAILTYPE['WithdrawFail'],
                        'values': {
                            'wuuid': kwargs['uuid'],
                            'member_id': kwargs['member_id'],
                            'userid': kwargs['userid'],
                            'currency_code': kwargs['currency_code'],
                            'currency_digit': kwargs['currency_digit'],

                            'address': kwargs['address'],
                            'addr_nickname': kwargs['addr_nickname'],
                            'amount': kwargs['amount'],
                            'fee_amount': kwargs['fee_amount'],
                            'req_time': kwargs['req_time'],
                            'done_time': kwargs['done_time'],
                            'description': kwargs['description'],
                        },
                    }
                else:
                    raise ValueError('UNKNOWN')

                complete_key = S3.set_string(key='WITHDRAW/COMPLETE/{currency_code}/{seq:0>20}_{uuid}.json'.format(currency_code=item['currency_code'], seq=item['seq'], uuid=item['uuid']),
                                             content=json.dumps(item))
                done_key.delete()
                self.con.commit()

                # response send
                self.responsedb.publish('RESPONSE', json.dumps(response))
                # email send
                self.requestdb.lpush('SEND:EMAIL', json.dumps(email_msg))

                self.log.w('gray', 'DEBUG', complete_key.key)

    def ETH_kwargs(self, item):
        kwargs = dict()
        kwargs['uuid'] = item['uuid']
        kwargs['status'] = item['status']
        kwargs['userid'] = item['userid']
        kwargs['member_id'] = item['member_id']
        kwargs['wallet_id'] = item['wallet_id']
        kwargs['currency_id'] = item['currency_id']
        kwargs['currency_code'] = item['currency_code']
        kwargs['currency_digit'] = item['currency_digit']
        kwargs['side'] = item['side']
        kwargs['amount'] = item['amount']
        kwargs['fee_amount'] = item['fee_amount']
        kwargs['address'] = item['address']
        kwargs['addr_nickname'] = item['addr_nickname'].encode('UTF8')
        kwargs['req_time'] = item['req_time']
        kwargs['act_time'] = item['act_time']
        kwargs['wreq_time'] = item['wreq_time']
        kwargs['done_time'] = item['done_time']
        kwargs['done_time_ordinal'] = datetime.datetime.fromtimestamp(item['done_time']).toordinal()
        kwargs['req_ip'] = item['req_ip']
        kwargs['a_id'] = item['a_id']
        kwargs['adminid'] = item['adminid']

        if item['status'] == 5:  # 성공
            kwargs['txid'] = item['txhash']
            kwargs['description'] = None
        elif item['status'] == 6:  # 실패
            kwargs['txid'] = None
            kwargs['description'] = str(item['description'])
        else:
            raise ValueError('UNKNOWN')
        return kwargs

    def BTC_kwargs(self, item):
        kwargs = dict()
        kwargs['uuid'] = item['uuid']
        kwargs['status'] = item['status']
        kwargs['userid'] = item['userid']
        kwargs['member_id'] = item['member_id']
        kwargs['wallet_id'] = item['wallet_id']
        kwargs['currency_id'] = item['currency_id']
        kwargs['currency_code'] = item['currency_code']
        kwargs['currency_digit'] = item['currency_digit']
        kwargs['side'] = item['side']
        kwargs['amount'] = item['amount']
        kwargs['fee_amount'] = item['fee_amount']
        kwargs['address'] = item['address']
        kwargs['addr_nickname'] = item['addr_nickname'].encode('UTF8')
        kwargs['req_time'] = item['req_time']
        kwargs['act_time'] = item['act_time']
        kwargs['wreq_time'] = item['wreq_time']
        kwargs['done_time'] = item['done_time']
        kwargs['done_time_ordinal'] = datetime.datetime.fromtimestamp(item['done_time']).toordinal()
        kwargs['req_ip'] = item['req_ip']
        kwargs['a_id'] = item['a_id']
        kwargs['adminid'] = item['adminid']

        if item['status'] == 5:  # 성공
            kwargs['txid'] = item['transferinfo']['txid']
            kwargs['description'] = None
        elif item['status'] == 6:  # 실패
            kwargs['txid'] = None
            kwargs['description'] = str(item['description'])
        else:
            raise ValueError('UNKNOWN')
        return kwargs

    def insert_wallet_history_done(self, item):
        print("inside the wallet history done")
        if self.currency_code == 'BTC':
            kwargs = self.BTC_kwargs(item=item)
        elif self.currency_code == 'ETH':
            kwargs = self.ETH_kwargs(item=item)
        else:
            raise ValueError('NOT_IMPLEMENTED')

        sql = "INSERT INTO `wallet_history_done` SET " \
              "`uuid`=%(uuid)s, " \
              "`status`=%(status)s, " \
              "`userid`=%(userid)s, " \
              "`member_id`=%(member_id)s, " \
              "`wallet_id`=%(wallet_id)s, " \
              "`currency_id`=%(currency_id)s, " \
              "`currency_code`=%(currency_code)s, " \
              "`currency_digit`=%(currency_digit)s, " \
              "`side`=%(side)s, " \
              "`amount`=%(amount)s, " \
              "`fee_amount`=%(fee_amount)s, " \
              "`txid`=%(txid)s, " \
              "`address`=%(address)s, " \
              "`addr_nickname`=%(addr_nickname)s, " \
              "`req_time`=%(req_time)s, " \
              "`act_time`=%(act_time)s, " \
              "`wreq_time`=%(wreq_time)s, " \
              "`done_time`=%(done_time)s, " \
              "`done_time_ordinal`=%(done_time_ordinal)s, " \
              "`req_ip`=%(req_ip)s, " \
              "`a_id`=%(a_id)s, " \
              "`adminid`=%(adminid)s, " \
              "`description`=%(description)s;"
        try:
            self.cur.execute(sql, kwargs)
            return True, kwargs
        except MySQLdb.IntegrityError as err:
            if err[0] == 1062:  # Duplicate entry
                return False, err
            else:
                raise MySQLdb.IntegrityError(err)

    def wallet_withdraw(self, wallet_id, amount, currency_code, fee):
        print("inside withdraw")
        querys = {'wallet_id': wallet_id,
                  'amount': amount,
                  'currency_code': currency_code,
                  'fee': fee}
        if self.CONFIG.STORAGEDB['host'] == 'clonedbitstromdb-cluster.cluster-cvmb9kloysdq.ap-northeast-1.rds.amazonaws.com':
            wallet_host = 'https://tapi.bsbex.net/api/v2'
        else:
            wallet_host = 'https://tapi.bsbex.net/api/v2'

        url = wallet_host + '/withdraw'
        r = requests.post(url, data=querys)
        #r = requests.post('{wallet_host}/withdraw?{querys}'.format(wallet_host=wallet_host, querys=urllib.urlencode(querys)))
        response = json.loads(r.text)
        self.log.w('yellow', 'DEBUG', json.dumps(response))
        return True

    def wallet_holddn(self, wallet_id, amount, currency_code):
        print("inside holddn")
        querys = {'wallet_id': wallet_id,
                  'amount': amount,
                  'currency_code': currency_code}
        if self.CONFIG.STORAGEDB['host'] == 'clonedbitstromdb-cluster.cluster-cvmb9kloysdq.ap-northeast-1.rds.amazonaws.com':
            wallet_host = 'https://tapi.bsbex.net/api/v2'
        else:
            wallet_host = 'https://tapi.bsbex.net/api/v2'

        url = wallet_host + '/holddn'
        r = requests.post(url, data=querys)    
        #r = requests.post('{wallet_host}/holddn?{querys}'.format(wallet_host=wallet_host, querys=urllib.urlencode(querys)))
        response = json.loads(r.text)
        self.log.w('yellow', 'DEBUG', json.dumps(response))
        return True

    def sdb_notice_withdraw(self, member_id, amount, fee, notice_type):
        values = {
            'currency_code': self.currency_code,
            'amount': float(amount) / 10 ** self.currency_digit,
            'fee': float(fee) / 10 ** self.currency_digit,
        }
        sql = "INSERT INTO `txtrader`.`notice` SET `member_id`=%(member_id)s, `notice_type`=%(notice_type)s, `notice_time`=%(notice_time)s, `values`=%(values)s;"
        kwargs = {
            'member_id': member_id,
            'notice_type': notice_type,
            'notice_time': DATETIME_UNIXTIMESTAMP(),
            'values': json.dumps(values),
        }
        self.cur.execute(sql, kwargs)
        notice = {
            'notice_id': self.cur.lastrowid,
            'notice_type': notice_type,
            'notice_time': DATETIME_UNIXTIMESTAMP(),
            'currency_code': values['currency_code'],
            'amount': values['amount'],
            'fee': values['fee'],
        }
        return notice


if __name__ == '__main__':
    # option load
    parser = optparse.OptionParser()
    parser.add_option("--debug", action='store_true', default=False,
                      help="debug",
                      dest="debug")
    options, args = parser.parse_args()
    DEBUG = options.debug

    # log
    LoggingDirectory = os.path.join(
        CONFIG.LOG['directory'],
        os.path.basename(FILENAME).rsplit('.', 1)[0]
    )
    log = Log(LoggingDirectory=LoggingDirectory, DEBUG=DEBUG)
    log.f_format = '%Y%m%d_%H.log'
    log.w('gray', 'LoggingDirectory', log.LoggingDirectory)

    # main
    me = singleton.SingleInstance()
    main = Main(log=log)
