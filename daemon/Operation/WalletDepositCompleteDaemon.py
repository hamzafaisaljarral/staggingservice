# -*- encoding:utf8 -*-
import os, sys, time, optparse, traceback, json, requests, urllib, uuid

from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import exchange, S3, exchangeConfigParser, WalletDB
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
        self.wdb = WalletDB(WALLETDB=self.CONFIG.WALLETDB, WALLETCACHE=self.CONFIG.WALLETCACHE)
        print("printing wb",self.wdb)

        S3.aws_s3_info_set(**self.CONFIG.S3)

        while True:
            try:
                self.before_run()
                done_keys = S3.list('DEPOSIT/DONE')
                print("done keys",done_keys)
                if done_keys:
                    self.storagedb_check()
                    for done_key in done_keys:
                        print("checking the list",done_key)
                        self.run(done_key)
                self.after_run()
            except Exception as err:
                if DEBUG:
                    print("there is a exception")
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
        with open(os.path.join(self.RUN_DIRECTORY, 'xbit-wallet-deposit-complete.pid'), 'w') as f:
            f.write(str(os.getpid()))

        self.kill_format = '{pid}.kill'
        pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))
        if os.path.exists(pidkill):
            os.remove(pidkill)
            exit(0)

    def after_run(self):
        pass

    def run(self, done_key):
        print("inside run")
        self.log.w('gray', 'DEBUG', done_key.key)

        SIDE, STATUS, self.currency_code, filename = done_key.key.split('/', 3)
        if self.cur.execute("SELECT `id`, `digit` FROM `currency` WHERE `code`=%(code)s;", {'code': self.currency_code}):
            self.currency_id, self.currency_digit = self.cur.fetchone()
        else:
            self.con.rollback()
            return 'NOT_EXISTS_CURRENCY'  # 등록안된 코인

        if self.currency_code.upper() == 'BTC':
            Currency_Process = self.BTC_Process
        elif self.currency_code.upper() == 'ETH':
            Currency_Process = self.ETH_Process
        else:
            return 'NOT_IMPLEMENTED'

        try:
            done_value = json.loads(done_key.read())
            print("reading the done value",done_value)
        except ValueError as err:  # No JSON object could be decoded
            self.con.rollback()
            return 'No JSON object could be decoded'

        # S3.COMPLETE에 파일이 존재 하는가?
        if S3.get(done_key.key.replace('DONE', 'COMPLETE')):  # 있으면
            if done_value.has_key('wuuid'):  # Update인가?
                done_key.delete()
            else:  # Raw인가?
                S3.set_string(key=done_key.key.replace('DONE', 'MISS'), content=json.dumps(done_value))
                done_key.delete()
        else:  # 없으면
            # S3.COMPLETE 생성(Raw)
            S3.set_string(key=done_key.key.replace('DONE', 'COMPLETE'), content=json.dumps(done_value))

            # wallet_history_done에 입력하고, WalletServer에 입금처리
            wuuid = Currency_Process(done_key, done_value)

            # S3.COMPLETE 생성(UPDATE)
            done_value['wuuid'] = wuuid
            S3.set_string(key=done_key.key.replace('DONE', 'COMPLETE'), content=json.dumps(done_value))

            # S3.DONE 삭제
            done_key.delete()
        self.con.commit()
        return True

    def ETH_Process(self, done_key, done_value):
        # 입금 처리
        DIGIT = 18
        wuuid = str(uuid.uuid4())
        done_time = int(time.time())
        done_time_ordinal = datetime.datetime.fromtimestamp(done_time).toordinal()
        address = done_value['returnValues']['_address']
        print("checking the address",address)
        amount = int(done_value['returnValues']['_value']) / 10 ** (DIGIT - self.currency_digit)
        txid = done_value['transactionHash']

        # address에 할당된 member_id, wallet_id 가져오기
        if self.cur.execute("SELECT a.`member_id`, b.`wallet_id`, b.`userid` FROM `deposit_addr_ETH` as a LEFT JOIN `member` AS b on a.member_id = b.member_id WHERE a.`account`=%(address)s;", {'address': address}):
            member_id, wallet_id, userid = self.cur.fetchone()
            print("things we fetched",member_id,wallet_id,userid)
            if member_id == None:
                self.con.rollback()
                raise ValueError('NOT_ASSIGNMENT_ADDERSS')  # 할당 안된 address
        else:
            self.con.rollback()
            raise ValueError('NOT_REGIST_ADDRESS')  # 등록 안된 address

        self.CommonProcess(wuuid, userid, member_id, wallet_id, amount, txid, address, done_time, done_time_ordinal)  # <- 여기서 commit함
        return wuuid

    def BTC_Process(self, done_key, done_value):
        DIGIT = 8
        wuuid = str(uuid.uuid4())
        done_time = int(time.time())
        done_time_ordinal = datetime.datetime.fromtimestamp(done_time).toordinal()
        member_id = done_value['label']
        amount = int(done_value['valueString'])
        txid = done_value['txid']
        address = ''
        # member_id로 wallet_id 가져오기
        if self.cur.execute("SELECT `userid`, `wallet_id`, NULL FROM `member` WHERE member_id=%(member_id)s;", {'member_id': member_id}):
            userid, wallet_id, _ = self.cur.fetchone()
            if wallet_id == None:
                self.con.rollback()
                raise ValueError('NOT_REG_WALLET_ID')  # wallet_id가 존재하지 않는다.
        else:
            self.con.rollback()
            raise ValueError('NOT_REG_MEMBER_ID')  # member_id가 존재하지 않는다.

        self.CommonProcess(wuuid, userid, member_id, wallet_id, amount, txid, address, done_time, done_time_ordinal)  # <- 여기서 commit함
        return wuuid

    def CommonProcess(self, wuuid, userid, member_id, wallet_id, amount, txid, address, done_time, done_time_ordinal):
        # wallet_history_done에 입력하기
        self.insert_wallet_history_deposit_done(wuuid=wuuid,
                                                userid=userid,
                                                member_id=member_id,
                                                wallet_id=wallet_id,
                                                currency_id=self.currency_id,
                                                currency_code=self.currency_code,
                                                currency_digit=self.currency_digit,
                                                amount=amount,
                                                txid=txid,
                                                address=address,
                                                done_time=done_time,
                                                done_time_ordinal=done_time_ordinal)
        # WalletServer Deposit 처리하기
        self.wallet_deposit(wuuid=wuuid, wallet_id=wallet_id, amount=amount, currency_code=self.currency_code, txid=txid)
        notice = self.sdb_notice_deposit(member_id=member_id, amount=amount)
        self.con.commit()
        response = self.ResponseInfo(response_code='DEPOSIT_COMPLETE', member_id=member_id,
                                     notice=notice, )
        self.responsedb.publish('RESPONSE', json.dumps(response))

        # email send
        email_msg = {
            'emailtype': EMAILTYPE['DepositComplete'],
            'values': {
                'wuuid': wuuid,
                'member_id': member_id,
                'userid': userid,
                'currency_code': self.currency_code,
                'currency_digit': self.currency_digit,

                'amount': amount,
                'txid': txid,
                'done_time': done_time,
            },
        }
        self.requestdb.lpush('SEND:EMAIL', json.dumps(email_msg))

    def wallet_deposit(self, wuuid, wallet_id, amount, currency_code, txid):
        print("inside wallet deposit")
        # deposit_time = DATETIME_UNIXTIMESTAMP()
        #
        # # 입금 history
        # sql = 'INSERT INTO `bmex_wallet`.`bmex:wallet:history:deposit` SET ' \
        #       '`wallet_id`=%(wallet_id)s, ' \
        #       '`currency_code`=%(currency_code)s, ' \
        #       '`amount`=%(amount)s, ' \
        #       '`txid`=%(txid)s, ' \
        #       '`reg_date`=%(reg_date)s;'
        # kwargs = {
        #     'wallet_id': wallet_id,
        #     'currency_code': currency_code,
        #     'amount': amount,
        #     'txid': txid,
        #     'reg_date': int(time.time())
        # }
        # self.cur.execute(sql, kwargs)
        # self.log.w('green', self.cur._executed)
        #
        # # 입금할 금액에 대한 당시 환산 가치 구하기
        # dsp_currency_values = dict()
        # for dsp_currency_code in self.wdb.dsp_currencys:  # detail 입력하기
        #     dsp_currency_funds, dsp_currency_price = self.wdb.ValueConversion(amount=amount, amount_currency_code=currency_code, display_currency_code=dsp_currency_code)
        #     dsp_currency_values[dsp_currency_code] = {'funds': dsp_currency_funds, 'price': dsp_currency_price}
        #
        # # detail
        # sql = "INSERT INTO `bmex:wallet:{currency_code}:detail` SET " \
        #       "`ext_id`=%(ext_id)s, " \
        #       "`wallet_id`=%(wallet_id)s, " \
        #       "`product`=%(currency_code)s, " \
        #       "`inout`=%(inout)s, " \
        #       "`side`=%(side)s, " \
        #       "`time`=%(time)s, " \
        #       "`time_days`=%(time_days)s, " \
        #       "`amount`=%(amount)s, " \
        #       "`digit`=%(digit)s, " \
        #       "`fee`=0, " \
        #       "`description`=NULL, " \
        #       "`reg_date`=%(time)s, " \
        #       "`bcc`=NULL, " \
        #       "`qcc`=%(currency_code)s, " \
        #       "`funds_USD`=%(funds_USD)s, " \
        #       "`funds_BTC`=%(funds_BTC)s, " \
        #       "`funds_ETH`=%(funds_ETH)s, " \
        #       "`funds_BXB`=%(funds_BXB)s;"
        # kwargs = {
        #     'ext_id': wuuid,
        #     'wallet_id': wallet_id,
        #     'currency_code': currency_code,
        #     'inout': WALLET_DETAIL_INOUT['IN'],
        #     'side': WALLET_DETAIL_SIDE['DEPOSIT'],
        #     'time': deposit_time,
        #     'time_days': UNIXTIMESTAMP_DATETIME(deposit_time).toordinal(),
        #     'amount': amount,
        #     'digit': self.currency_digit,
        #     'funds_USD': dsp_currency_values['USD']['funds'],
        #     'funds_BTC': dsp_currency_values['BTC']['funds'],
        #     'funds_ETH': dsp_currency_values['ETH']['funds'],
        #     'funds_BXB': dsp_currency_values['BXB']['funds'],
        # }
        # self.cur.execute(sql.format(currency_code=currency_code), kwargs)
        # self.log.w('green', self.cur._executed)
        #
        # # balance
        # sql = "UPDATE `bmex:wallet:{currency_code}:balance` SET `amount`=`amount` + %(amount)s WHERE `wallet_id=%(wallet_id)s;`"
        # kwargs = {'amount': amount, 'wallet_id': wallet_id}
        # self.cur.execute(sql.format(currency_code=currency_code), kwargs)
        # self.log.w('green', self.cur._executed)
        #
        # # currency_summary average price
        # _executed = self.wdb._execute_summary_averageprice(wallet_id=wallet_id,
        #                                                    amount=float(amount) / 10**self.currency_digit,
        #                                                    amount_currency_code=currency_code,
        #                                                    dsp_currency_price={k: v[k]['price'] for k, v in dsp_currency_values.items()},
        #                                                    inout=WALLET_DETAIL_INOUT['IN'])
        # self.log.w('green', _executed)
        #
        # # # currency_summary accumlate deposit
        # # _executed = b._execute_summary_accum(wallet_id=InputArguments['wallet_id'],
        # #                                      currency_code=InputArguments['currency_code'],
        # #                                      funds={k: float(v['funds']) / 10 ** b.currencys[k]['digit'] for (k, v) in dsp_currency_values.items()},
        # #                                      inout='IN')
        #
        #
        querys = {'wallet_id': wallet_id,
                  'amount': amount,
                  'currency_code': currency_code,
                  'txid': txid,
                  }
        if self.CONFIG.STORAGEDB['host'] == 'bitstrom-02-cluster.cluster-cvmb9kloysdq.ap-northeast-1.rds.amazonaws.com':
            wallet_host = 'http://127.0.0.1:8999/api/v2'
        else:
            wallet_host = 'http://127.0.0.1:8999/api/v2'
        #r = requests.post('{wallet_host}/deposit?{querys}'.format(wallet_host=wallet_host, querys=urllib.urlencode(querys)))
        url = wallet_host + '/deposit'
        r = requests.post(url, data=querys)
        response = json.loads(r.text)
        self.log.w('yellow', 'DEBUG', json.dumps(response))
        return True

    def insert_wallet_history_deposit_done(self, wuuid, userid, member_id, wallet_id, currency_id, currency_code, currency_digit, amount, txid, address, done_time, done_time_ordinal):
        sql = "INSERT INTO `wallet_history_done` SET " \
              "`uuid`=%(uuid)s, " \
              "`status`=5, " \
              "`userid`=%(userid)s, " \
              "`member_id`=%(member_id)s, " \
              "`wallet_id`=%(wallet_id)s, " \
              "`currency_id`=%(currency_id)s, " \
              "`currency_code`=%(currency_code)s, " \
              "`currency_digit`=%(currency_digit)s, " \
              "`side`=2, " \
              "`amount`=%(amount)s, " \
              "`fee_amount`=0, " \
              "`txid`=%(txid)s, " \
              "`address`=%(address)s, " \
              "`done_time`=%(done_time)s, " \
              "`done_time_ordinal`=%(done_time_ordinal)s;"  # status=5(완료), side=2(입금)
        kwargs = {'uuid': wuuid, 'userid': userid, 'member_id': member_id, 'wallet_id': wallet_id, 'currency_id': currency_id, 'currency_code': currency_code,
                  'currency_digit': currency_digit, 'amount': amount, 'txid': txid, 'address': address, 'done_time': done_time, 'done_time_ordinal': done_time_ordinal}
        self.cur.execute(sql, kwargs)

    def sdb_notice_deposit(self, member_id, amount):
        values = {
            'currency_code': self.currency_code,
            'amount': float(amount) / 10 ** self.currency_digit,
        }
        sql = "INSERT INTO `txtrader`.`notice` SET `member_id`=%(member_id)s, `notice_type`=%(notice_type)s, `notice_time`=%(notice_time)s, `values`=%(values)s;"
        kwargs = {
            'member_id': member_id,
            'notice_type': NOTICE_TYPE['DEPOSIT_COMPLETE'],
            'notice_time': DATETIME_UNIXTIMESTAMP(),
            'values': json.dumps(values),
        }
        self.cur.execute(sql, kwargs)
        notice = {
            'notice_id': self.cur.lastrowid,
            'notice_type': NOTICE_TYPE['DEPOSIT_COMPLETE'],
            'notice_time': DATETIME_UNIXTIMESTAMP(),
            'currency_code': values['currency_code'],
            'amount': values['amount'],
        }
        return notice


if __name__ == '__main__':
    try:
        me = singleton.SingleInstance()
    except singleton.SingleInstanceException as err:
        exit(1)

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
    main = Main(log=log)
