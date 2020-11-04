# -*- encoding:utf8 -*-
import os, sys, time, json, optparse, traceback, ConfigParser

from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.function import *
from lib.exchange import exchange, exchangeConfigParser


class rconfig:
    ini_file = 'xbitProfitRank.ini'
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
       # {'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
    'RESPONSEDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
       # {'key': 'password', 'method': 'get', 'defaultvalue': None},
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
        self.CONFIG.STORAGEDB = self.CONFIG.STORAGEDB_RO
        self.log = log

        self.requestdb_init()
        self.storagedb_init(AUTOCOMMIT=True)  # 대량의 데이터를 처리하기때문에 autocommit을 활성화하지 않으면 db에 락이 걸리고 그만큼 delay가 발생하게 된다.
        self.responsedb_init()

        self.currency_load()
        self.product_load()

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

    def before_run(self):
        self.storagedb_check()

    def after_run(self):
        time.sleep(rconfig.getint('GENERAL', 'interval_seconds'))

    def run(self):
        start_time = time.time()
        member_dict = dict()
        #self.cur.execute("SELECT `member_id`, `wallet_id`, `userid` FROM `member` WHERE `wallet_id` IS NOT NULL;")
        self.cur.execute("SELECT `member_id`, `wallet_id`, `userid` FROM `member` WHERE `wallet_id` IS NOT NULL AND `userid` NOT LIKE %(userid)s;", {'userid': rconfig.get('GENERAL', 'IGNORE_USERID')})
        self.log.w('gray', self.cur._executed)
        description = [_t[0] for _t in self.cur.description]
        for row in self.cur.fetchall():
            member = {_k: _v for (_k, _v) in zip(description, row)}
            member['is_trader'] = self.is_trader(member_id=member['member_id'])
            member['total_dposit'] = dict()
            member['total_equity'] = dict()
            for currency_code in self.currencys:
                member[currency_code] = {'balance': 0, 'deposit': {_t: 0 for _t in self.currencys}}
                member['total_dposit'][currency_code] = 0
                member['total_equity'][currency_code] = 0

            member_dict[member['wallet_id']] = member

        # balance
        sql = "SELECT `wallet_id`, `amount` FROM `bmex_wallet`.`bmex:wallet:{}:balance` WHERE `amount` > 0;"
        for currency_code in self.currencys:
            self.cur.execute(sql.format(currency_code))
            self.log.w('gray', self.cur._executed)
            for wallet_id, amount in self.cur.fetchall():
                try:
                    member_dict[wallet_id][currency_code]['balance'] = amount
                except KeyError:
                    pass

        # accumulate deposit
        sql = "SELECT `wallet_id`, `accumdeposit_USDT`, `accumdeposit_BTC`, `accumdeposit_ETH` " \
              "FROM `bmex_wallet`.`bmex:wallet:{}:summary` WHERE (`accumdeposit_USDT` + `accumdeposit_BTC` + `accumdeposit_ETH`) > 0;"
        for currency_code in self.currencys:
            self.cur.execute(sql.format(currency_code))
            self.log.w('gray', self.cur._executed)
            for wallet_id, self.USDT, self.BTC, self.ETH in self.cur.fetchall():
                try:
                    member_dict[wallet_id]
                except KeyError:
                    continue
                for cc in self.currencys:
                    #adding check other currency stop coming here 
                    if member_dict[wallet_id][currency_code]['deposit'][cc] != 0:
                       member_dict[wallet_id][currency_code]['deposit'][cc] = int(getattr(self, cc) * 10 ** self.currencys[cc]['digit'])

        # value conversion
        for dsp_currency_code in self.currencys:
            for member in member_dict.values():
                for currency_code in self.currencys: 
                    if member[currency_code]['balance'] != 0 :
                       print("checking the currency balance",member[currency_code]['balance'],currency_code,dsp_currency_code)
                       funds = self.value_conversion(amount=member[currency_code]['balance'], src_cc=currency_code, dst_cc=dsp_currency_code)
                       if funds != None:
                          print("the funds type creating problem",funds)
                          member['total_equity'][dsp_currency_code] += funds
                          member['total_dposit'][dsp_currency_code] += member[currency_code]['deposit'][dsp_currency_code]

        # equity & profit
        for dsp_currency_code in self.currencys:
            members = list()
            for member in member_dict.values():
                if not member['is_trader']:  # 주문을 한번도 하지 않으면 rank에 넣지 않는다.
                    continue
                if member['USDT']['balance'] + member['BTC']['balance'] + member['ETH']['balance'] <= 0:
                    continue
                profit = member['total_equity'][dsp_currency_code] - member['total_dposit'][dsp_currency_code]
                if profit != 0:
                    real_profit = float(profit) / 10 ** self.currencys[dsp_currency_code]['digit']
                    if member['total_dposit'][dsp_currency_code]!=0: 
                       profit_rate = float(profit) / member['total_dposit'][dsp_currency_code] * 100
                else:
                    real_profit = 0.0
                    profit_rate = 0.0
                real_profit = 0.0
                profit_rate = 0.0    
                print(real_profit)
                print(profit_rate)   
                member['profit'] = real_profit
                member['profit_r'] = profit_rate
                members.append(member)

            profitrank_channel_name = 'XBIT:PROFIT:RANK:{currency_code}'.format(currency_code=dsp_currency_code)
            pipeline = self.requestdb.pipeline(transaction=True)
            pipeline.delete(profitrank_channel_name)
            ranking = sorted([member['profit'] for member in members], reverse=True)
            for member in members:
                member['rank'] = ranking.index(member['profit']) + 1
                pipeline.zadd(profitrank_channel_name, json.dumps(member), member['rank'])
            pipeline.execute()
        end_time = time.time()
        self.requestdb.set('XBIT:PROFIT:RANK', int(end_time))
        self.requestdb.hset('XBIT:PROFIT:RUNTIME', int(end_time), end_time - start_time)
        self.log.w('gray', 'end', end_time - start_time)

    def is_trader(self, member_id):
        if self.cur.execute("SELECT 1 FROM `order:OPEN` WHERE `member_id`=%(member_id)s LIMIT 1;", {'member_id': member_id}):
            return True
        elif self.cur.execute("SELECT 1 FROM `order:DONE` WHERE `member_id`=%(member_id)s LIMIT 1;", {'member_id': member_id}):
            return True
        else:
            return False

    def value_conversion(self, amount, src_cc, dst_cc, FIAT='USDT'):
        src_cu = int(10 ** self.currencys[src_cc]['digit'])
        dst_cu = int(10 ** self.currencys[dst_cc]['digit'])
        #print("check the value we are getting for conversation",src_cc,dst_cc)
        if src_cc == dst_cc:
            return amount
   
        elif src_cc == FIAT:
            print("inside if")
            #price_a = int(self.responsedb.get('TRADEPRICE:{dst}{src}'.format(src=src_cc, dst=dst_cc)))
            price_a = int('1')
            price_b = (src_cu * dst_cu) / price_a
            funds = (amount * price_b) / src_cu
            return funds
        elif dst_cc == FIAT:
            #price_a = int(self.responsedb.get('TRADEPRICE:{src}{dst}'.format(src=src_cc, dst=dst_cc)))
            price_a = int('1')
            funds = (amount * price_a) / src_cu
            return funds
        else:
              # 앞에도 뒤에도 fiat가 닌녀석들
            checkkk = self.responsedb.get('TRADEPRICE:{src}{fiat}'.format(src=src_cc, fiat=FIAT)) 
            check = self.responsedb.get('TRADEPRICE:{dst}{fiat}'.format(dst=dst_cc, fiat=FIAT)) 
            price_a = checkkk
            price_b = check
            print(price_a) 
            print(price_b) 
            #price_a = int(self.responsedb.get('TRADEPRICE:{src}{fiat}'.format(src=src_cc, fiat=FIAT)))
            #price_b = int(self.responsedb.get('TRADEPRICE:{dst}{fiat}'.format(dst=dst_cc, fiat=FIAT)))
            if price_a != None and price_b != None:
               print("inside the if after getting none")
               price = (price_a * src_cu) / price_b
               return amount * price / src_cu


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