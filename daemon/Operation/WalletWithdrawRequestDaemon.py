# -*- encoding:utf8 -*-
import os, sys, time, json, optparse, traceback

from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import STORAGEDB, S3, exchangeConfigParser

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
    'S3': [
        {'key': 'aws_access_key_id', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_secret_access_key', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_bucket', 'method': 'get', 'defaultvalue': None},
    ],
}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    print CONFIG.errormsg
    exit(1)


class Main(STORAGEDB):
    def __init__(self, log):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log
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
        with open(os.path.join(self.RUN_DIRECTORY, 'xbit-wallet-withdraw-request.pid'), 'w') as f:
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
        '''
        status : 1(요청), 2(승인), 3(송금중), 4(반려), 5(완료), 6(실패), 7(취소)
        side : 2(입금), 3(출금)
        '''
        self.con.commit()
        sql = "SELECT `uuid`, `seq`, `status`, `userid`, `member_id`, `wallet_id`, `currency_id`, `currency_code`, " \
              "`currency_digit`, `side`, `amount`, `fee_amount`, `address`, `addr_nickname`, `req_time`, `act_time`, `req_ip`, " \
              "`a_id`, `adminid` " \
              "FROM `wallet_history_ing` WHERE `status`=3 AND `witndraw_daemon_process`=0 ORDER BY `seq` ASC;"
        if self.cur.execute(sql):
            description = [t[0] for t in self.cur.description]
            for item in self.cur.fetchall():
                request = {k: v for (k, v) in zip(description, item)}
                request['wreq_time'] = int(time.time())

                sql = "UPDATE `wallet_history_ing` SET `witndraw_daemon_process`=1, `acount`=`acount`+1 WHERE `uuid`=%(uuid)s AND `status`=3 AND `witndraw_daemon_process`=0;"
                kwargs = {'wreq_time': request['wreq_time'], 'uuid': request['uuid']}
                if self.cur.execute(sql, kwargs):
                    s3_content = {
                        'uuid': request['uuid'],
                        'seq': request['seq'],
                        'status': request['status'],
                        'userid': request['userid'],
                        'member_id': request['member_id'],
                        'wallet_id': request['wallet_id'],
                        'currency_id': request['currency_id'],
                        'currency_code': request['currency_code'],
                        'currency_digit': request['currency_digit'],
                        'side': request['side'],
                        'amount': request['amount'],
                        'fee_amount': request['fee_amount'],
                        'address': request['address'],
                        'addr_nickname': request['addr_nickname'],
                        'req_time': request['req_time'],
                        'act_time': request['act_time'],
                        'wreq_time': request['wreq_time'],
                        'req_ip': request['req_ip'],
                        'a_id': request['a_id'],
                        'adminid': request['adminid'],
                    }

                    s3key = 'WITHDRAW/REQUEST/{currency_code}/{seq:0>20}_{uuid}.json'.format(currency_code=request['currency_code'], seq=request['seq'], uuid=request['uuid'])
                    request_key = S3.set_string(key=s3key, content=json.dumps(s3_content))
                    self.con.commit()
                    self.log.w('gray', 'DEBUG', request_key.key)
                else:
                    self.con.rollback()


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
