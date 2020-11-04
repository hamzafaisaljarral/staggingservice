# -*- encoding:utf8 -*-
import os, sys, time, optparse, traceback, ConfigParser, json

from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import exchange, exchangeConfigParser

config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'}
    ],
    'PROCESSDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        #{'key': 'password', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
    ],
    'STORAGEDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'user', 'method': 'get', 'defaultvalue': None},
        {'key': 'passwd', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'get', 'defaultvalue': None},
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

        self.processdb_init()
        self.storagedb_init()
        self.executed_time = 0.0
        self.accum_fee_data = dict()  # trade_days -> currency_code -> accumulate fee
        while True:
            try:
                msg = self.processdb.brpop(self.PROCESSDBNAME['PROCESS_C'], timeout=1)
                print("message",msg)
                if msg != None:
                    channel, _Feeinfos = msg
                    self.run(Feeinfos=json.loads(_Feeinfos))
                else:
                    pass

                self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
                if not os.path.exists(self.RUN_DIRECTORY):
                    os.makedirs(self.RUN_DIRECTORY)

                ### PID 관련
                with open(os.path.join(self.RUN_DIRECTORY, 'xbit-tradefeecalc.pid'), 'w') as f:
                    f.write(str(os.getpid()))

                self.kill_format = '{pid}.kill'
                pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            except Exception as err:
                self.log.w('red', '[ERROR]', repr(traceback.format_exc(err)))
                exit(1)

    def accum_fee(self, trade_days, currency_code, fee, real_amount):
        trade_days = str(trade_days)
        try:
            self.accum_fee_data[trade_days]
        except KeyError:
            self.accum_fee_data[trade_days] = dict()

        try:
            self.accum_fee_data[trade_days][currency_code]['fee'] += fee
            self.accum_fee_data[trade_days][currency_code]['real_amount'] += real_amount
        except KeyError:
            self.accum_fee_data[trade_days][currency_code] = {'fee': fee, 'real_amount': real_amount}

    def run(self, Feeinfos):
        for Feeinfo in Feeinfos:
            currency_code = Feeinfo['currency_code']
            fee = Feeinfo['fee']
            real_amount = Feeinfo['real_amount']
            trade_time = Feeinfo['trade_time'] / 10 ** 6
            trade_days = trade_time - (trade_time % 3600)
            self.accum_fee(trade_days=trade_days,
                           currency_code=currency_code,
                           fee=fee,
                           real_amount=real_amount)

        sql = "INSERT INTO `bmex_wallet`.`bmex:wallet:fee:accum:hours` (`trade_time`,`currency_code`,`amount`,`real_amount`) VALUES " \
              "(%(trade_time)s,%(currency_code)s,%(amount)s,%(real_amount)s) " \
              "ON DUPLICATE KEY UPDATE `amount`=`amount`+%(amount)s,`real_amount`=`real_amount`+%(real_amount)s;"
        if time.time() - self.executed_time >= 1:
            if self.accum_fee_data:
                self.storagedb_check()
                for trade_time in self.accum_fee_data:
                    for currency_code in self.accum_fee_data[trade_time]:
                        fee = self.accum_fee_data[trade_time][currency_code]['fee']
                        real_amount = self.accum_fee_data[trade_time][currency_code]['real_amount']
                        kwargs = {'trade_time': trade_time, 'currency_code': currency_code, 'amount': fee, 'real_amount': real_amount}
                        self.cur.execute(sql, kwargs)
                self.con.commit()
                self.accum_fee_data.clear()  # 누적값 초기화
            self.executed_time = time.time()


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
