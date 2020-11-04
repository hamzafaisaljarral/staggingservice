# -*- encoding:utf8 -*-
import os, sys, time, optparse, traceback, ConfigParser

from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import exchange, S3, exchangeConfigParser, WalletDB


class rconfig:
    ini_file = 'TradeFeeRate.ini'
    config = None

    @classmethod
    def inspect(cls):
        if cls.config is None:
            cls.config = ConfigParser.ConfigParser()
        cls.config.read(os.path.join(BASE_DIRECTORY, 'config', cls.ini_file))

    @classmethod
    def get(cls, section, key):
        cls.inspect()
        return cls.config.get(section, key)

    @classmethod
    def getint(cls, section, key):
        cls.inspect()
        return cls.config.getint(section, key)

    @classmethod
    def getfloat(cls, section, key):
        cls.inspect()
        return cls.config.getfloat(section, key)

    @classmethod
    def getboolean(cls, section, key):
        cls.inspect()
        return cls.config.getboolean(section, key)


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

        self.storagedb_init()

        while True:
            try:
                self.storagedb_check()
                self.con.commit()
                self.run()
                time.sleep(rconfig.getint('GENERAL', 'interval_seconds'))
            except Exception as err:
                self.log.w('red', '[ERROR]', repr(traceback.format_exc(err)))
                exit(1)

    def run(self):
        FeeGradeTradeAmountDays = self.getFeeGradeTradeAmountDays()  # 거래수수료 구하는 기준 일수 가져오기
        FeeTable = self.getFeeTable()  # 거래수수료 table 가져오기
        AccumTradeAmount = self.getAccumTradeAmount(FeeGradeTradeAmountDays)

        # 등급 결정하기
        member_infos = dict()
        new_members = list()
        for wallet_id in AccumTradeAmount:
            member_id = self.getMemberid(wallet_id)
            new_members.append(member_id)
            tradeamount = AccumTradeAmount[wallet_id]
            for feerate in FeeTable:
                if tradeamount >= feerate['tradeamount']:
                    member_infos[member_id] = {
                        'tfr': feerate['tfr'],
                        'mfr': feerate['mfr'],
                        'grade': feerate['grade'],
                        'tradeamount': tradeamount,
                        'update_date': int(time.time()),
                    }
                    break

        # 데이터 처리
        # | New | Old | Rule
        # | O   | O   | A    | UPDATE < feerate, amount
        # | 0   | O   | M    | UPDATE < amount
        # | 0   | X   |      | INSERT
        # | X   | O   | A    | DELETE
        # | X   | O   | M    | UPDATE < amount=0

        # 신규 데이터 처리
        for member_id in member_infos:
            memberinfo = member_infos[member_id]
            sql = "INSERT INTO `mem_feerate_relation` (`member_id`, `tfr`, `mfr`, `update_date`, `tradeamount`) " \
                  "VALUES (%(member_id)s, %(tfr)s, %(mfr)s, %(update_date)s, %(tradeamount)s) ON DUPLICATE KEY " \
                  "UPDATE `tfr`=IF(`feegrade_rule`=1, %(tfr)s, `tfr`), `mfr`=IF(`feegrade_rule`=1, %(mfr)s, `mfr`), `update_date`=%(update_date)s, `tradeamount`=%(tradeamount)s;"
            kwargs = {
                'member_id': member_id,
                'tfr': memberinfo['tfr'],
                'mfr': memberinfo['mfr'],
                'update_date': memberinfo['update_date'],
                'tradeamount': memberinfo['tradeamount'],
            }
            self.cur.execute(sql, kwargs)
            self.log.w('gray', 'QUERY', self.cur._executed)
            self.con.commit()

        # 기존 데이터 처리
        self.cur.execute("SELECT `member_id`, `feegrade_rule` FROM `mem_feerate_relation`;")
        for member_id, feegrade_rule in self.cur.fetchall():
            if not (member_id in member_infos):
                if feegrade_rule == 0:  # Manual
                    self.cur.execute("UPDATE `mem_feerate_relation` SET `update_date`=%(ud)s, `tradeamount`=%(ta)s WHERE `member_id`=%(mi)s AND `feegrade_rule`=0;", {'mi': member_id, 'ud': int(time.time()), 'ta': 0})
                    self.log.w('gray', 'QUERY', self.cur._executed)
                elif feegrade_rule == 1:  # Auto
                    self.cur.execute("DELETE FROM `mem_feerate_relation` WHERE `member_id`=%(member_id)s AND `feegrade_rule`=1;", {'member_id': member_id})
                    self.log.w('gray', 'QUERY', self.cur._executed)
                self.con.commit()

    def getMemberid(self, wallet_id):
        if self.cur.execute("SELECT `member_id` FROM `member` WHERE `wallet_id`=%(wallet_id)s;", {'wallet_id': wallet_id}):
            return self.cur.fetchone()[0]
        else:
            return None

    def getFeeGradeTradeAmountDays(self):
        self.cur.execute("SELECT `value`, NULL FROM `settings` WHERE `key`='FeeGradeTradeAmountDays';")
        FeeGradeTradeAmountDays, _ = self.cur.fetchone()
        return int(FeeGradeTradeAmountDays)

    def getFeeTable(self):
        FeeTable = list()
        self.cur.execute("SELECT `grade`,`tfr`, `mfr`, `tradeamount` FROM `mem_feerate` WHERE `enable`=1 ORDER BY `grade` DESC;")
        for grade, tfr, mfr, tradeamount in self.cur.fetchall():
            FeeTable.append({'grade': grade, 'tfr': tfr, 'mfr': mfr, 'tradeamount': tradeamount})
        return FeeTable

    def getAccumTradeAmount(self, FeeGradeTradeAmountDays):
        start = 0
        count = 100
        data = dict()
        sql = "SELECT `wallet_id`, `amount` FROM `bmex_wallet`.`bmex:wallet:trade_amount_hour` WHERE `timestamp` >= %(from)s LIMIT %(start)s, %(count)s"
        while True:
            kwargs = {'from': time.time() - (86400 * FeeGradeTradeAmountDays), 'start': start, 'count': count}
            if self.cur.execute(sql, kwargs):
                for wallet_id, amount in self.cur.fetchall():
                    try:
                        data[wallet_id] += amount
                    except KeyError:
                        data[wallet_id] = amount
            else:
                return data
            start += count


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
