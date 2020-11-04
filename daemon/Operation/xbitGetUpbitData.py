# -*- encoding:utf8 -*-
import os, sys, time, json, optparse, traceback, requests, urllib, uuid, copy

import redis
from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.exchange import exchange, exchangeConfigParser
from lib.function import *
from sign import digifinex
coin = digifinex({"appKey":"791fccc3913e1e", "appSecret":"3abeb2da2620db3bc9e6a97134ea801fc25aecc733"})

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
    'WALLETCACHE': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
        #{'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    print CONFIG.errormsg
    exit(1)


class WalletCache:
    rcon = None

    @classmethod
    def inspect(cls):
        if cls.rcon is None:
            #cls.rcon = redis.Redis(host=CONFIG.WALLETCACHE['host'], password=CONFIG.WALLETCACHE['password'], db=CONFIG.WALLETCACHE['db'], port=CONFIG.WALLETCACHE['port'])
             cls.rcon = redis.Redis(host=CONFIG.WALLETCACHE['host'], port=CONFIG.WALLETCACHE['port']) 
    @classmethod
    def SetDateClosingPrice(cls, product_code, ordinal, price):
        cls.inspect()
        pipeline = cls.rcon.pipeline(transaction=True)
        pipeline.hset('PRICE', product_code, price)
        pipeline.hset('PRICE:HASH:{product_code}'.format(product_code=product_code), ordinal, price)
        pipeline.hset('PRICE:HASH:{product_code}'.format(product_code=product_code), ordinal + 1, price)
        pipeline.zadd('PRICE:ZSET:{product_code}'.format(product_code=product_code), ordinal, ordinal)
        pipeline.execute()


class Main(exchange):
    def __init__(self, log):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log
        self.storagedb_init()
        self.responsedb_init()

        self.currency_load()
        self.product_load()

        self.run_markets = (
            ('USDT-BTC', 'BTCUSDT'),
            ('USDT-ETH', 'ETHUSDT'),
            #('USDT-XRP', 'XRPUSDT'),
            #('USDT-LTC', 'LTCUSDT'),
            #('USDT-TRX', 'TRXUSDT'),
            #('USDT-ADA', 'ADAUSDT'),
            #('USDT-BCH', 'BCHUSDT'),
            #('USDT-ETC', 'ETCUSDT'),
            #('USDT-BAT', 'BATUSDT'),
        )

        self.run_products = list()
        self.market_to_product = dict()
        self.previous_ticker = {product_code: {'price': -float('inf'), 'side': 0} for (_, product_code) in self.run_markets}  # side를 결정하기 위한 이전값 저장

        for upbit_market, product_code in self.run_markets:
            self.market_to_product[upbit_market] = product_code
            run_product = {
                'upbit_market': upbit_market,
                'product_code': product_code,
                'bcc': self.products[product_code]['bcc'],
                'bcd': self.products[product_code]['bcd'],
                'bcu': self.products[product_code]['bcu'],
                'qcc': self.products[product_code]['qcc'],
                'qcd': self.products[product_code]['qcd'],
                'qcu': self.products[product_code]['qcu'],
                'min_size_unit': self.products[product_code]['min_size_unit'],
                'min_price_unit': self.products[product_code]['min_price_unit'],
            }
            self.run_products.append(run_product)

        self.periods = [
            {'name': '1M', 'seconds': 60, 'count': 10},
            {'name': '5M', 'seconds': 60 * 5, 'count': 10},
            {'name': '15M', 'seconds': 60 * 15, 'count': 10},
            {'name': '30M', 'seconds': 60 * 30, 'count': 10},
            {'name': '1H', 'seconds': 60 * 60, 'count': 10},
            {'name': '4H', 'seconds': 60 * 60 * 4, 'count': 10},
            {'name': '1D', 'seconds': 60 * 60 * 24, 'count': 10},
            {'name': '1W', 'seconds': 60 * 60 * 24 * 7, 'count': 10},
        ]
        self.SQL = "INSERT INTO `txquote`.`OHLC:{product_code}:{period}` (`time`, `open`, `high`, `low`, `close`, `volume`) VALUES (%(time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s) " \
                   " ON DUPLICATE KEY UPDATE `open`=%(open)s, `high`=%(high)s, `low`=%(low)s, `close`=%(close)s, `volume`=%(volume)s;"

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
        pass

    def after_run(self):
        time.sleep(5)

    def run(self):
        # TICKER 보내기
        querys = {'markets': ','.join([_t['upbit_market'] for _t in self.run_products])}
        url = 'https://api.upbit.com/v1/ticker?{querys}'.format(querys=urllib.urlencode(querys))
        print("get updata url",url)
        r = requests.get(url)
        if r.status_code == 200:
            log.w('gray', url, r.headers['remaining-req'])
            data = json.loads(r.text)
            print("data from api upbit",data)
        #data = coin.do_request("GET","/ticker",{
        #    "symbol":"btc_usdt"
        #    })
        #data2 = coin.do_request("GET","/ticker",{
        #    "symbol":"eth_usdt"
        #    })
        #data = data+data2
        #print("data",data)          
        for ticker in data:
                product_code = self.market_to_product[ticker['market']]
                mpu = self.products[product_code]['min_price_unit'] / float(self.products[product_code]['qcu'])
                print("mpu",mpu)
                # msu = self.products[product_code]['min_size_unit'] / float(self.products[product_code]['bcu'])
                print("ticker",ticker['trade_price'])
                last_price = ticker['trade_price'] - (ticker['trade_price'] % mpu)
                last_price_integer = int(last_price * self.products[product_code]['qcu'])
                self.responsedb.set(self.RESPONSEDBNAME['TRADEPRICE'].format(product_code=product_code), last_price_integer)

                if last_price_integer > self.previous_ticker[product_code]['price']:
                    taker_side = 0  # BUY
                elif last_price_integer < self.previous_ticker[product_code]['price']:
                    taker_side = 1  # SELL
                else:
                    taker_side = self.previous_ticker[product_code]['side']

                data = {'trade_id': str(uuid.uuid4()), 'side': pSIDE[taker_side], 'size': 0,
                        'price': last_price, 'product': product_code,
                        'time': int(time.time() * 10 ** 6), 'sequence': 0, 'bcu': self.products[product_code]['bcu'], 'qcu': self.products[product_code]['qcu']}
                self.responsedb.publish(self.RESPONSEDBNAME['TRADEHISTORY:PUBLISH'].format(product_code=product_code),
                                        json.dumps(data))
                self.previous_ticker[product_code]['side'] = copy.deepcopy(taker_side)
                self.previous_ticker[product_code]['price'] = copy.deepcopy(last_price_integer)

                # walletserver에서 기간수익률 구하기 위한 날짜별 가격 집어 넣기
                WalletCache.SetDateClosingPrice(product_code=product_code, ordinal=datetime.datetime.now().toordinal(), price=last_price_integer)
        else:
            log.w('red', url, r.status_code, r.text)

        # OHLC 채우기
        for run_product in self.run_products:
            for period in self.periods:
                time_from = self.get_last(product_code=run_product['product_code'], period=period['name'])
                ohlcs = self.get_upbit_ohlc(period=period['name'], product_code=run_product['product_code'], market=run_product['upbit_market'], seconds=period['seconds'], time_from=time_from)
                for ohlc in ohlcs:
                    sql = self.SQL.format(product_code=run_product['product_code'], period=period['name'])
                    self.cur.execute(sql, ohlc)
                    #print self.cur._executed
                self.con.commit()
        exit(0)

    def get_last(self, product_code, period):
        sql = "SELECT `time`, NULL FROM `txquote`.`OHLC:{product_code}:{period}` ORDER BY `time` DESC LIMIT 1;".format(product_code=product_code, period=period)
        if self.cur.execute(sql):
            _time, _ = self.cur.fetchone()
            return datetime.datetime.fromtimestamp(_time)
        else:
            return datetime.datetime(2019, 6, 1, 0, 0, 0)

    def get_upbit_ohlc(self, period, product_code, market, seconds, time_from):
        self.COUNT = 200  # max count 200
        mpu = self.products[product_code]['min_price_unit'] / float(self.products[product_code]['qcu'])
        msu = self.products[product_code]['min_size_unit'] / float(self.products[product_code]['bcu'])

        ohlc_dict = dict()
        _break = False
        for step in range(1, 11):
            count = self.COUNT
            to = time_from + datetime.timedelta(seconds=(seconds * (self.COUNT - 1)) * step)  # step 별로 기간 구해서 count 만큼 가져오기

            # 최근 데이터까지 다 왔을때

            if to >= datetime.datetime.now():
                try:
                    last_from = datetime.datetime.fromtimestamp(max(ohlc_dict.keys()))
                except ValueError:
                    last_from = time_from

                sss = int((datetime.datetime.now() - last_from).total_seconds())  # 마지막 데이터부터 현재시간까지의 초
                count = ((sss - sss % seconds) / seconds) + 2
                count=abs(count)  # period의 second로 나누어서 가져올 ohlc의 갯수를 지정
                _break = True  # 현재시간까지 다 왔으니 break
            if period in ('1M', '5M', '15M', '30M', '1H', '4H'):
                # querys = {'market': market, 'count': count, 'to': to.strftime('%Y-%m-%dT%H:%M:%S+09:00')}
                querys = {'market': market, 'count': count, 'to': to.strftime('%Y-%m-%dT%H:%M:%S+00:00')}
                url = 'https://api.upbit.com/v1/candles/minutes/{params}?{querys}'.format(params=seconds / 60, querys=urllib.urlencode(querys))
                print("second url",url)
            elif period == '1D':
                # querys = {'market': market, 'count': count, 'to': to.strftime('%Y-%m-%dT%H:%M:%S+09:00')}
                querys = {'market': market, 'count': count, 'to': to.strftime('%Y-%m-%dT%H:%M:%S+00:00')}
                url = 'https://api.upbit.com/v1/candles/days?{querys}'.format(querys=urllib.urlencode(querys))
                print("third url",url)
            elif period == '1W':
                # querys = {'market': market, 'count': count, 'to': to.strftime('%Y-%m-%dT%H:%M:%S+09:00')}
                querys = {'market': market, 'count': count, 'to': to.strftime('%Y-%m-%dT%H:%M:%S+00:00')}
                url = 'https://api.upbit.com/v1/candles/weeks?{querys}'.format(querys=urllib.urlencode(querys))
                print("fouth url",url)

            try:
                r = requests.get(url)
            except Exception:
                break
            if r.status_code == 200:
                log.w('gray', url, r.headers['remaining-req'])
                data = json.loads(r.text)
                #print("data getting from the json on click",data)
                for upbit_ohlc in data:
                    #print("inside for")
                    _time = int(time.mktime(time.strptime(upbit_ohlc['candle_date_time_kst'], '%Y-%m-%dT%H:%M:%S')))
                    print("time",_time)
                    ohlc_dict[_time] = {
                        'time': _time,
                        'open': upbit_ohlc['opening_price'] - (upbit_ohlc['opening_price'] % mpu),
                        'high': upbit_ohlc['high_price'] - (upbit_ohlc['high_price'] % mpu),
                        'low': upbit_ohlc['low_price'] - (upbit_ohlc['low_price'] % mpu),
                        'close': upbit_ohlc['trade_price'] - (upbit_ohlc['trade_price'] % mpu),
                        'volume': upbit_ohlc['candle_acc_trade_volume'] - (upbit_ohlc['candle_acc_trade_volume'] % msu),
                    }
                    print(upbit_ohlc)    
            else:
                print("there was a error in url",r.status_code,r.text)
                log.w('red', url, r.status_code, r.text)
                break

            if _break:
                print("inside break")
                break
        return sorted(ohlc_dict.values(), key=lambda t: t['time'])


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
