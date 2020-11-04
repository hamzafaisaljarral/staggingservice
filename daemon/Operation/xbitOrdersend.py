# -*- encoding:utf8 -*-
import os, sys, time, json, optparse, traceback, random, multiprocessing, signal, math, ConfigParser, requests, urllib, copy

import redis
import numpy as np
from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.exchange import exchange, exchangeConfigParser
from lib.function import *


class rconfig:
    ini_file = 'xbitOrdersend.ini'
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
}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    exit(1)


class STAT:
    con = None

    @classmethod
    def send(cls, name):
        if cls.con is None:
            cls.con = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'], db=0)
        msg = json.dumps({'name': name, 'time': int(time.time())})
        print(msg)
        cls.con.publish('STAT', msg)


def random_walk(length=10000, proba=0.5, min_lookback=1, max_lookback=100, cumprod=False):
    length += 1
    assert (min_lookback >= 1)
    assert (max_lookback >= min_lookback)

    if max_lookback > length:
        max_lookback = length
        print "max_lookback parameter has been set to the length of the random walk series."

    directions = list()
    if not cumprod:  # ordinary increments
        series = [0.] * length  # array of prices
        for i in range(1, length):
            if i < min_lookback + 1:
                direction = np.sign(np.random.randn())
            else:
                lookback = np.random.randint(min_lookback, min(i - 1, max_lookback) + 1)
                direction = np.sign(series[i - 1] - series[i - 1 - lookback]) * np.sign(proba - np.random.uniform())
            series[i] = series[i - 1] + np.fabs(np.random.randn()) * direction
            directions.append(direction)
    else:  # percent changes
        series = [1.] * length  # array of prices
        for i in range(1, length):
            if i < min_lookback + 1:
                direction = np.sign(np.random.randn())
            else:
                lookback = np.random.randint(min_lookback, min(i - 1, max_lookback) + 1)
                direction = np.sign(series[i - 1] / series[i - 1 - lookback] - 1.) * np.sign(proba - np.random.uniform())
            series[i] = series[i - 1] * np.fabs(1 + np.random.randn() / 1000. * direction)
            directions.append(direction)

    return directions, series


class TimerProcessor(exchange):
    def __init__(self, log, _type, RUN_PRODUCT, TimerQueue):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log
        self.type = _type
        self.RUN_PRODUCT = RUN_PRODUCT
        self.TimerQueue = TimerQueue

        while True:
            self.TimerQueue.put(time.time())
            s = rconfig.getfloat('%s_%s' % (self.RUN_PRODUCT, self.type), 'order_per_second')
            time.sleep(1. / s)


class OrdersendProcessor(exchange):
    SIDE = {
        1: 'BUY',
        -1: 'SELL',
    }
    print("inside order send")


    def __init__(self, log, _type, RUN_PRODUCT, TimerQueue, Upbit):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log
        self.type = _type
        self.RUN_PRODUCT = RUN_PRODUCT
        self.TimerQueue = TimerQueue
        self.Upbit = Upbit

        self.storagedb_init(AUTOCOMMIT=True)
        self.responsedb_init()
        self.requestdb_init()

        self.currency_load()
        self.product_load()

        ### 주문 날리는 계정
        self.members = dict()
        self.members['MARKET'] = None
        self.members['LIMIT'] = None
        self.members['default'] = list()
        self.cur.execute("SELECT `userid`, `member_id`, `wallet_id` FROM `member` WHERE `userid` LIKE %(userid)s;", {'userid': rconfig.get('HURST', 'hurst_sendonly_userid')})
        for userid, member_id, wallet_id in self.cur.fetchall():
            member = {
                'userid': userid,
                'member_id': member_id,
                'wallet_id': wallet_id,
            }
            print(member)
            self.members['default'].append(member)

        try:
            userid = rconfig.get('%s_%s' % (self.RUN_PRODUCT, self.type), 'hurst_market_sendonly_userid')
            if self.cur.execute("SELECT `userid`, `member_id`, `wallet_id` FROM `member` WHERE `userid` = %(userid)s;", {'userid': userid}):

                userid, member_id, wallet_id = self.cur.fetchone()
                print(userid)
                print(member_id)
                print(wallet_id)
                self.members['MARKET'] = {'userid': userid, 'member_id': member_id, 'wallet_id': wallet_id}
        except ConfigParser.NoOptionError:
            pass
        try:
            userid = rconfig.get('%s_%s' % (self.RUN_PRODUCT, self.type), 'hurst_limit_sendonly_userid')
            if self.cur.execute("SELECT `userid`, `member_id`, `wallet_id` FROM `member` WHERE `userid` = %(userid)s;", {'userid': userid}):
                userid, member_id, wallet_id = self.cur.fetchone()
                print(userid)
                print(member_id)
                print(wallet_id)
                self.members['LIMIT'] = {'userid': userid, 'member_id': member_id, 'wallet_id': wallet_id}
        except ConfigParser.NoOptionError:
            pass

        self.con.close()

        ### expire_seconds
        self.expire_seconds_list = list()
        if self.type == 'NORMAL':
            [self.expire_seconds_list.append(2) for _ in range(10)]
            [self.expire_seconds_list.append(60) for _ in range(85)]
        else:
            [self.expire_seconds_list.append(600) for _ in range(1)]

        # 준비 완료
        while True:
            try:
                self.run()
            except Exception as err:
                if DEBUG:
                    self.log.w('red', 'ERROR', traceback.format_exc(err))
                else:
                    self.log.w('red', 'ERROR', repr(traceback.format_exc(err)))
                exit(1)

    def get_sprice(self):
        ''' 모든 price는 integer단위로 다룬다 '''
        s_price = rconfig.getint('%s_%s' % (self.RUN_PRODUCT, self.type), 's_price')
        if s_price > 0:
            print(s_price)
            return s_price
        else:
            print(self.Upbit['price'].value)
            return self.Upbit['price'].value

    def get_size(self):
        min_size_ratio = rconfig.getint('%s_%s' % (self.RUN_PRODUCT, self.type), 'min_size_ratio')
        max_size_ratio = rconfig.getint('%s_%s' % (self.RUN_PRODUCT, self.type), 'max_size_ratio')
        min_size_unit = self.min_size_unit * 2
        print(min_size_unit)

        size = np.random.uniform((min_size_unit * min_size_ratio), (min_size_unit * max_size_ratio) + min_size_unit)
        return int(size - (size % self.min_size_unit))

    def get_price(self, distance):
        min_price_gap_ratio = rconfig.getint('%s_%s' % (self.RUN_PRODUCT, self.type), 'min_price_gap_ratio')
        max_price_gap_ratio = rconfig.getint('%s_%s' % (self.RUN_PRODUCT, self.type), 'max_price_gap_ratio')
        if min_price_gap_ratio == max_price_gap_ratio:
            m = min_price_gap_ratio
        else:
            m = random.choice(range(min_price_gap_ratio, max_price_gap_ratio + 1))
        s_min_price_unit = (float(self.min_price_unit) / self.products[self.RUN_PRODUCT]['qcu']) * m
        price_distance = s_min_price_unit * distance
        price_distance = int(round(price_distance, int(-math.log10(s_min_price_unit))) * self.products[self.RUN_PRODUCT]['qcu'])
        trade_price = self.get_sprice()  # 시세따라가기
        raw_price = trade_price + price_distance
        price = int(round(raw_price, -int(math.log10(self.min_price_unit))))  # 반올림
        return price

    def get_member(self, order_type, side):
        member = self.members[order_type]
        print(member)
        if member is None:
            member = random.choice(self.members['default'])
        return member

    def get_ask(self):
        prices = self.responsedb.zrange('ORDERBOOK:{}:ASK'.format(self.RUN_PRODUCT), 0, -1)
        if prices:
            return min([int(float(t)) for t in prices])
        else:
            return None

    def get_bid(self):
        prices = self.responsedb.zrange('ORDERBOOK:{}:BID'.format(self.RUN_PRODUCT), 0, -1)
        if prices:
            return max([int(float(t)) for t in prices])
        else:
            return None

    def run(self):
        self.min_price_unit = self.products[self.RUN_PRODUCT]['min_price_unit']
        self.min_size_unit = self.products[self.RUN_PRODUCT]['min_size_unit']

        length = rconfig.getint('%s_%s' % (self.RUN_PRODUCT, self.type), 'length')
        proba = rconfig.getfloat('%s_%s' % (self.RUN_PRODUCT, self.type), 'proba')
        max_lookback = rconfig.getfloat('%s_%s' % (self.RUN_PRODUCT, self.type), 'max_lookback')
        directions, series = random_walk(length=length, proba=proba, min_lookback=1, max_lookback=max_lookback, cumprod=False)

        for index, (_side, distance) in enumerate(zip(directions, series), 1):
            price = self.get_price(distance=distance)
            if price <= 0:
                continue
            size = self.get_size()
            side = self.SIDE[_side]
            expire_seconds = random.choice(self.expire_seconds_list)

            # 실제 주문의 order_typ은 아니고, 계정을 구분하기 위한 order_type
            if side == 'BUY':
                ask = self.get_ask()
                if ask is None:
                    order_type = 'LIMIT'
                else:
                    if price >= ask:
                        order_type = 'MARKET'
                    else:
                        order_type = 'LIMIT'

            else:  # SELL
                bid = self.get_bid()
                if bid is None:
                    order_type = 'LIMIT'
                else:
                    if price <= bid:
                        order_type = 'MARKET'
                    else:
                        order_type = 'LIMIT'
            try:
                self.stat
            except AttributeError:
                self.stat = dict()

            try:
                self.stat[order_type]
            except KeyError:
                self.stat[order_type] = dict()

            try:
                self.stat[order_type][side] += 1
            except KeyError:
                self.stat[order_type][side] = 1

            ### member
            member = self.get_member(order_type=order_type, side=side)

            # print member['userid'], self.type, self.stat

            orderset = {'product': self.RUN_PRODUCT,
                        'order_type': 'LIMIT',
                        'member_id': member['member_id'],
                        'wallet_id': member['wallet_id'],
                        'side': side,
                        'policy': 'GTT',
                        'size': size,
                        'price': price,
                        'tfr': 100,
                        'mfr': 100,
                        'expire_seconds': expire_seconds}
            self.log.w('gray', json.dumps(orderset))
            self.requestdb.lpush('ORDERSEND', json.dumps(orderset))
            STAT.send('ORDER')
            self.TimerQueue.get()


class Upbit:
    print("inside upbit")
    CODESET = dict()
    CODESET['BTCUSDT'] = {'code': 'USDT-BTC', 'qcu': 10 ** 4}
    CODESET['ETHUSDT'] = {'code': 'USDT-ETH', 'qcu': 10 ** 4}
    CODESET['XRPUSDT'] = {'code': 'USDT-XRP', 'qcu': 10 ** 4}
    CODESET['DASHUSDT'] = {'code': 'USDT-DASH', 'qcu': 10 ** 4}
    CODESET['XMRUSDT'] = {'code': 'USDT-XMR', 'qcu': 10 ** 4}
    CODESET['ADAUSDT'] = {'code': 'USDT-ADA', 'qcu': 10 ** 4}

    CODESET['LTCUSDT'] = {'code': 'USDT-LTC', 'qcu': 10 ** 4}
    CODESET['BCHUSDT'] = {'code': 'USDT-BCH', 'qcu': 10 ** 4}
    CODESET['ETCUSDT'] = {'code': 'USDT-ETC', 'qcu': 10 ** 4}
    CODESET['TRXUSDT'] = {'code': 'USDT-TRX', 'qcu': 10 ** 4}
    CODESET['ZECUSDT'] = {'code': 'USDT-ZEC', 'qcu': 10 ** 4}
    CODESET['OMGUSDT'] = {'code': 'USDT-OMG', 'qcu': 10 ** 4}
    CODESET['DOGEUSDT'] = {'code': 'USDT-DOGE', 'qcu': 10 ** 4}
    CODESET['TUSDUSDT'] = {'code': 'USDT-TUSD', 'qcu': 10 ** 4}
    CODESET['XVGUSDT'] = {'code': 'USDT-XVG', 'qcu': 10 ** 4}
    CODESET['SCUSDT'] = {'code': 'USDT-SC', 'qcu': 10 ** 4}
    CODESET['PLFUSDT'] = {'code': 'USDT-PLF', 'qcu': 10 ** 4}

    def __init__(self, log, RUN_PRODUCT, Upbit):
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.Upbit = Upbit

        while True:
            self.run()
            self.wait()

    def run(self):
        self.isget = None
        try:
            price = self.get_price()
            self.current_time = int(time.time())
            self.isget = True
        except Exception:
            self.isget = False
            return

        self.Upbit['time'].set(self.current_time)
        self.Upbit['price'].set(price)

    def wait(self):
        if self.isget:
            time.sleep(60)
        else:
            time.sleep(1)

    def get_price(self):
        querys = {'market': self.CODESET[self.RUN_PRODUCT]['code'], 'count': 1, 'to': datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S+09:00')}
        url = 'https://api.upbit.com/v1/candles/minutes/1?{querys}'.format(querys=urllib.urlencode(querys))
        r = requests.get(url)
        print("the price get",r)
        data = json.loads(r.content)
        print(data)
        tick = data[0]
        self.log.w('yellow', data)
        return int(float(tick['trade_price']) * self.CODESET[self.RUN_PRODUCT]['qcu'])


class Main(exchange):
    def multiprocess_starter(self, target, kwargs):
        p = multiprocessing.Process(target=target, kwargs=kwargs)
        p.daemon = True
        p.start()

        return p

    def __init__(self, log, RUN_PRODUCT):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT

        self.TimerQueue = dict()
        self.TimerQueue['NORMAL'] = multiprocessing.Queue()
        self.TimerQueue['EXTERNAL'] = multiprocessing.Queue()

        self.Upbit = dict()
        self.Upbit['time'] = multiprocessing.Manager().Value('i', 0)
        self.Upbit['price'] = multiprocessing.Manager().Value('i', 0)

        ## Process Manager
        self.multiprocess_starter(target=Upbit, kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT,
                                                        'Upbit': self.Upbit})

        # upbit에서 처음 가격 가져올때까지 기다렸다가 오더 데몬 실행하기
        while True:
            if self.Upbit['price'].value != 0:
                break
            time.sleep(0.1)

        self.multiprocess_starter(target=OrdersendProcessor, kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT,
                                                                     'TimerQueue': self.TimerQueue['NORMAL'], '_type': 'NORMAL',
                                                                     'Upbit': self.Upbit})
        self.multiprocess_starter(target=OrdersendProcessor, kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT,
                                                                     'TimerQueue': self.TimerQueue['EXTERNAL'], '_type': 'EXPAND',
                                                                     'Upbit': self.Upbit})

        self.multiprocess_starter(target=TimerProcessor, kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT,
                                                                 'TimerQueue': self.TimerQueue['NORMAL'], '_type': 'NORMAL', })
        self.multiprocess_starter(target=TimerProcessor, kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT,
                                                                 'TimerQueue': self.TimerQueue['EXTERNAL'], '_type': 'EXPAND', })
        while True:
            time.sleep(1)


if __name__ == '__main__':
    # option load
    parser = optparse.OptionParser()
    parser.add_option("--debug", action='store_true', default=False,
                      help="debug",
                      dest="debug")
    parser.add_option("-p", "--product", action='store',
                      dest="product")
    options, args = parser.parse_args()
    DEBUG = options.debug
    if options.product is None:
        parser.print_help()
        exit(1)
    RUN_PRODUCT = options.product.upper()
    assert RUN_PRODUCT in ('BTCUSDT', 'ETHUSDT', 'XRPUSDT', 'DASHUSDT', 'XMRUSDT', 'ADAUSDT',
                           'LTCUSDT',
                           'BCHUSDT',
                           'ETCUSDT',
                           'TRXUSDT',
                           'ZECUSDT',
                           'OMGUSDT',
                           'DOGEUSDT',
                           'TUSDUSDT',
                           'XVGUSDT',
                           'SCUSDT',
                           'PLFUSDT',
                           )

    # log
    LoggingDirectory = os.path.join(
        CONFIG.LOG['directory'],
        RUN_PRODUCT,
        os.path.basename(FILENAME).rsplit('.', 1)[0]
    )
    print(LoggingDirectory)
    log = Log(LoggingDirectory=LoggingDirectory, DEBUG=DEBUG)
    log.f_format = '%Y%m%d_%H.log'
    log.w('gray', 'LoggingDirectory', log.LoggingDirectory)

    # main
    me = singleton.SingleInstance(RUN_PRODUCT)
    main = Main(log=log, RUN_PRODUCT=RUN_PRODUCT)
