# -*- encoding:utf8 -*-
import os, sys, ConfigParser, time, re, json, uuid, copy, socket, struct, operator

import MySQLdb, redis
from boto.s3.connection import S3Connection
from boto.s3.key import Key

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
LOG_BASE_DIRECTORY = os.path.join('/data', 'logs', os.path.basename(BASE_DIRECTORY), )
CONF_BASE_DIRECTORY = os.path.join(BASE_DIRECTORY, 'conf')

sys.path.append(BASE_DIRECTORY)
from lib.function import *
from lib.define import *


class STORAGEDB:
    __AUTOCOMMIT = False
    STORAGEDBNAME = {
        'ORDER:OPEN': 'order:OPEN',
        'ORDER:DONE': 'order:DONE',
        'ORDER:STOP': 'order:STOP',
        'ORDER:DETAIL': '{product_code}:order:DETAIL',
        'ORDER:HISTORY': '{product_code}:order:HISTORY',
        'NOTICE': 'notice',
        'TRADEHISTORY': 'tradehistory',
    }

    def storagedb_init(self, AUTOCOMMIT=False):
        self.__AUTOCOMMIT = AUTOCOMMIT
        self.con = MySQLdb.connect(**self.CONFIG.STORAGEDB)
        self.cur = self.con.cursor()
        self.cur.execute("SET NAMES UTF8;")
        if self.__AUTOCOMMIT:
            self.cur.execute("SET AUTOCOMMIT=TRUE;")
        else:
            self.cur.execute("SET AUTOCOMMIT=FALSE;")

    def storagedb_check(self):
        try:
            print("checking")
            self.cur.execute("SELECT 1;")
        except MySQLdb.OperationalError as err:
            if err[0] == 2006:  # MySQL server has gone away
                print("error in storagedb")
                self.storagedb_init(AUTOCOMMIT=self.__AUTOCOMMIT)
            elif err[0] == 2013:  # Lost connection to MySQL server during query
                print("lost connection")
                self.storagedb_init(AUTOCOMMIT=self.__AUTOCOMMIT)
            else:
                raise MySQLdb.OperationalError(err)


class REQUESTDB:
    REQUESTDBNAME = {
        'ORDERSEND': 'ORDERSEND',
        'CURRENCYREVISION': 'CURRENCYREVISION',
        'PRODUCTREVISION': 'PRODUCTREVISION',
        'SEQUENCE:ORDERSEND': 'SEQUENCE:ORDERSEND',
        'SEQUENCE:ORDERBOOK': 'SEQUENCE:ORDERBOOK:{product_code}',
        'SEQUENCE:TRADEHISTORY': 'SEQUENCE:TRADEHISTORY:{product_code}',
    }

    def requestdb_init(self):
        self.requestdb = redis.Redis(**self.CONFIG.REQUESTDB)


class RESPONSEDB:
    RESPONSEDBNAME = {
        'RESPONSE': 'RESPONSE',
        'ORDERBOOK:PUBLISH': 'ORDERBOOK:PUBLISH:{product_code}',
        'ORDERBOOK:SEQ': 'ORDERBOOK:{product_code}:SEQ',
        'ORDERBOOK:BID': 'ORDERBOOK:{product_code}:BID',
        'ORDERBOOK:ASK': 'ORDERBOOK:{product_code}:ASK',
        'TRADEHISTORY:PUBLISH': 'TRADEHISTOR:PUBLISH:{product_code}',
        'TRADEHISTORY:SNAPSHOT': 'TRADEHISTORY:SNAPSHOT:{product_code}',
        'TRADEFUNDS': 'TRADEFUNDS:{product_code}',
        'TRADEFUNDS:TIME': 'TRADEFUNDS:{product_code}:TIME',
        'TRADEPRICE': 'TRADEPRICE:{product_code}',
    }

    def responsedb_init(self):
        self.responsedb = redis.Redis(**self.CONFIG.RESPONSEDB)


class PROCESSDB:
    PROCESSDBNAME = {
        'PROCESS_A': 'PROCESS_A:{product_code}',  # Pre -> Match
        'PROCESS_B': 'PROCESS_B',  # Match -> Post
        'PROCESS_C': 'PROCESS_C',  # Post -> FeeDaemon
    }

    def processdb_init(self):
        self.processdb = redis.Redis(**self.CONFIG.PROCESSDB)


class MATCHINGCACHE:
    MATCHINGCACHENAME = {
        'OPEN:BUY': '{product_code}:OPEN:BUY',
        'OPEN:SELL': '{product_code}:OPEN:SELL',
        'OPEN:SEQ': '{product_code}:OPEN:SEQ',
        'OPEN:INFO': '{product_code}:OPEN:INFO',
        'OPEN:EXPIRE': '{product_code}:OPEN:EXPIRE',
        'ACCUMULATE:SIZE': '{product_code}:ACCUMULATE:SIZE',
        'ACCUMULATE:FUNDS': '{product_code}:ACCUMULATE:FUNDS',
        'ACCUMULATE:FEE': '{product_code}:ACCUMULATE:FEE',
        'STOP:BUY': '{product_code}:STOP:BUY',
        'STOP:SELL': '{product_code}:STOP:SELL',
        'STOP:SEQ': '{product_code}:STOP:SEQ',
        'STOP:INFO': '{product_code}:STOP:INFO',
    }

    def matchingcache_init(self):
        self.matchingcache = redis.Redis(**self.CONFIG.MATCHINGCACHE)

    def get_ask(self, product_code):
        _sequence = self.matchingcache.zrange(name=self.MATCHINGCACHENAME['OPEN:SELL'].format(product_code=product_code), start=0, end=0, withscores=True)
        if _sequence:
            sequence, price = _sequence[0]
            return int(price)
        else:
            return None

    def get_bid(self, product_code):
        _sequence = self.matchingcache.zrange(name=self.MATCHINGCACHENAME['OPEN:BUY'].format(product_code=product_code), start=0, end=0, withscores=True)
        if _sequence:
            sequence, price = _sequence[0]
            return abs(int(price))
        else:
            return None


class exchange_Response:
    def ResponseSystem(self, response_code, **kwargs):
        response_msg = {'response_type': 'SYSTEM', 'response_code': RESPONSE_CODE[response_code], 'time': DATETIME_UNIXTIMESTAMP(), }
        response_msg.update(kwargs)
        return response_msg

    def ResponseOrder(self, response_code, order_type, product, member_id, **kwargs):
        response_msg = {'response_type': 'ORDER', 'response_code': RESPONSE_CODE[response_code], 'time': DATETIME_UNIXTIMESTAMP(),
                        'product_code': product, 'order_type': order_type, 'member_id': member_id, }
        response_msg.update(kwargs)
        return response_msg

    def ResponseInfo(self, response_code, member_id, **kwargs):
        response_msg = {'response_type': 'INFO', 'response_code': RESPONSE_CODE[response_code], 'time': DATETIME_UNIXTIMESTAMP(),
                        'member_id': member_id, }
        response_msg.update(kwargs)
        return response_msg


class exchange(STORAGEDB, REQUESTDB, RESPONSEDB, PROCESSDB, MATCHINGCACHE, exchange_Response):
    def config_parser(self, config_file, config_arguments, ):
        if not os.path.exists(config_file):
            return False, 'not exists config.ini'
        else:
            config = ConfigParser.ConfigParser()
            config.read(config_file)
            CONFIG = dict()
            for section, args in config_arguments.items():
                try:
                    CONFIG[section]
                except KeyError:
                    CONFIG[section] = dict()
                for arg in args:
                    if arg['method'] == 'get':
                        method = config.get
                    elif arg['method'] == 'getboolean':
                        method = config.getboolean
                    elif arg['method'] == 'getfloat':
                        method = config.getfloat
                    elif arg['method'] == 'getint':
                        method = config.getint
                    else:
                        raise ValueError('UNKNOWN')
                    try:
                        CONFIG[section][arg['key']] = method(section, arg['key'])
                    except ConfigParser.NoOptionError as err:
                        if arg['defaultvalue'] == None:
                            return False, err
                        else:
                            CONFIG[section][arg['key']] = arg['defaultvalue']
            return True, CONFIG

    def currency_check(self):
        try:
            self.currency_revision
        except AttributeError:
            self.currency_revision = None
        revision = self.requestdb.get(self.REQUESTDBNAME['CURRENCYREVISION'])
        print("inside currency_check",revision)
        if revision == None:
            revision = 0
        if revision != self.currency_revision:
            self.currency_load()
            self.currency_revision = revision

    def product_check(self):
        try:
            self.product_revision
        except AttributeError:
            self.product_revision = None
        revision = self.requestdb.get(self.REQUESTDBNAME['CURRENCYREVISION'])
        print("checking the product",revision)
        if revision == None:
            revision = 0
        if revision != self.product_revision:
            self.product_load()
            self.product_revision = revision

    def currency_load(self):
        try:
            self.cur.execute('SELECT `id`, `code`, `digit` FROM `currency`;')
            currency_list = self.cur.fetchall()
            self.con.commit()
        except AttributeError:
            __con = MySQLdb.connect(**self.CONFIG.STORAGEDB)
            __cur = __con.cursor()
            __cur.execute('SELECT `id`, `code`, `digit` FROM `currency`;')
            currency_list = __cur.fetchall()
            __con.close()
        self.currencys = dict()
        for currency_id, currency_code, currency_digit in currency_list:
            self.currencys[currency_code.upper()] = {'id': currency_id, 'digit': currency_digit}

    def product_load(self):
        try:
            self.cur.execute('SELECT `product_id`, `product_name`, `product_status`, `min_size_unit`, `min_price_unit`, `base_currency_code`, `quote_currency_code` FROM `products`;')
            product_list = self.cur.fetchall()
            self.con.commit()
        except AttributeError:
            __con = MySQLdb.connect(**self.CONFIG.STORAGEDB)
            __cur = __con.cursor()
            __cur.execute('SELECT `product_id`, `product_name`, `product_status`, `min_size_unit`, `min_price_unit`, `base_currency_code`, `quote_currency_code` FROM `products`;')
            product_list = __cur.fetchall()
            __con.close()
        self.products = dict()
        for product_id, product_name, product_status, min_size_unit, min_price_unit, base_currency_code, quote_currency_code in product_list:
            values = {
                'id': product_id,
                'name': product_name,
                'status': product_status,
                'min_size_unit': min_size_unit,
                'min_price_unit': min_price_unit,
                'bcc': base_currency_code,
                'qcc': quote_currency_code,
                'bcd': self.currencys[base_currency_code]['digit'],
                'qcd': self.currencys[quote_currency_code]['digit'],
                'bcu': 10 ** self.currencys[base_currency_code]['digit'],
                'qcu': 10 ** self.currencys[quote_currency_code]['digit'],
            }
            self.products[product_name.upper()] = values


class OrderValidCheck:
    isvalid = None

    def __init__(self, RecvMsg):
        self.RecvMsg = RecvMsg

        if self.__Base():
            if self.RecvMsg['order_type'] == 'MARKET':
                if self.__Market():
                    if self.RecvMsg['side'] == 'BUY':
                        self.__Market_Buy()
                    elif self.RecvMsg['side'] == 'SELL':
                        self.__Market_Sell()
                    else:
                        raise ValueError('UNKNOWN')

            elif self.RecvMsg['order_type'] == 'LIMIT':
                if self.__Limit():
                    if self.RecvMsg['policy'] == 'GTC':
                        self.__Limit_GTC()
                    elif self.RecvMsg['policy'] == 'GTT':
                        self.__Limit_GTT()
                    elif self.RecvMsg['policy'] in ('IOC', 'FOK'):
                        pass
                    else:
                        raise ValueError('UNKNOWN')

            elif self.RecvMsg['order_type'] == 'STOP':
                if self.__Stop():
                    if self.RecvMsg.has_key('price'):  # Limit
                        self.__Stop_Limit()

            elif self.RecvMsg['order_type'] == 'CANCEL':
                self.__Cancel()

            elif self.RecvMsg['order_type'] == 'MODIFY':
                self.__Modify()

            elif self.RecvMsg['order_type'] == 'STOPCANCEL':
                self.__StopCancel()

            else:
                raise ValueError('UNKNOWN')

    def __Base(self):
        validset = [
            {'key': 'product', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': [], 'castings': ['upper']},
            {'key': 'order_type', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['enum'], 'castings': ['upper'], 'enum': ORDER_TYPE.keys()},
            {'key': 'member_id', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['uuid'], 'castings': []},
            {'key': 'wallet_id', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['uuid'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Market(self):
        validset = [
            {'key': 'side', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['enum'], 'castings': ['upper'], 'enum': SIDE.keys()},
            {'key': 'tfr', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': []},
            {'key': 'magicnumber', 'require': False, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': [], 'defaultvalue': 0},
        ]
        return self.__validchecker(validset=validset)

    def __Market_Buy(self):
        validset = [
            {'key': 'funds', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Market_Sell(self):
        validset = [
            {'key': 'size', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Limit(self):
        validset = [
            {'key': 'policy', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['enum'], 'castings': ['upper'], 'enum': POLICY.keys()},
            {'key': 'side', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['enum'], 'castings': ['upper'], 'enum': SIDE.keys()},
            {'key': 'size', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
            {'key': 'price', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
            {'key': 'tfr', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': []},
            {'key': 'magicnumber', 'require': False, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': [], 'defaultvalue': 0},
        ]
        return self.__validchecker(validset=validset)

    def __Limit_GTC(self):
        validset = [
            {'key': 'mfr', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Limit_GTT(self):
        validset = [
            {'key': 'mfr', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': []},
            {'key': 'expire_seconds', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Stop(self):
        validset = [
            {'key': 'stop_price', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
            {'key': 'tfr', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': []},
            {'key': 'side', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['enum'], 'castings': ['upper'], 'enum': SIDE.keys()},
            {'key': 'amount', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
            {'key': 'magicnumber', 'require': False, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': [], 'defaultvalue': 0},
        ]
        return self.__validchecker(validset=validset)

    def __Stop_Limit(self):
        validset = [
            {'key': 'price', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
            {'key': 'mfr', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Cancel(self):
        validset = [
            {'key': 'order_id', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['uuid'], 'castings': []},
            {'key': 'size', 'require': False, 'kind': 'unit', 'datatype': int, 'validtypes': ['zero'], 'castings': [], 'defaultvalue': 0},
        ]
        return self.__validchecker(validset=validset)

    def __StopCancel(self):
        validset = [
            {'key': 'order_id', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['uuid'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __Modify(self):
        validset = [
            {'key': 'order_id', 'require': True, 'kind': 'unit', 'datatype': str, 'validtypes': ['uuid'], 'castings': []},
            {'key': 'price', 'require': True, 'kind': 'unit', 'datatype': int, 'validtypes': ['nonzero'], 'castings': []},
        ]
        return self.__validchecker(validset=validset)

    def __validchecker(self, validset):
        for validset in validset:
            # 필수, 옵션 검사
            if validset['require']:  # 필수
                try:
                    _value = self.RecvMsg[validset['key']]
                except KeyError:
                    self.isvalid = False
                    return self.isvalid
            else:  # 옵션
                try:
                    _value = self.RecvMsg[validset['key']]
                except KeyError:
                    _value = validset['defaultvalue']

            # 데이터타입 검사 및 변환
            try:
                if validset['kind'] == 'unit':
                    value = validset['datatype'](_value)
                elif validset['kind'] == 'array':
                    value = list()
                    for v in _value:
                        value.append(validset['datatype'](v))
                else:
                    raise ValueError('UNKNOWN')
            except Exception as err:
                self.isvalid = False
                return self.isvalid

            # 변환
            _value = copy.deepcopy(value)
            if validset['kind'] == 'unit':
                value = self.__castings(castings=validset['castings'], value=_value)
            elif validset['kind'] == 'array':
                value = list()
                for _v in _value:
                    v = self.__castings(castings=validset['castings'], value=_v)
                    value.append(v)
            else:
                raise ValueError('UNKNOWN')

            # 검사
            if validset['kind'] == 'unit':
                if not self.__validtype(validset=validset, value=value):
                    self.isvalid = False
                    return self.isvalid
            elif validset['kind'] == 'array':
                for v in value:
                    if not self.__validtype(validset=validset, value=v):
                        self.isvalid = False
                        return self.isvalid
            else:
                raise ValueError('UNKNOWN')

            self.RecvMsg[validset['key']] = value
        self.isvalid = True
        return self.isvalid

    def __castings(self, castings, value):
        for cast in castings:
            if cast == 'upper':
                return str(value).upper()
            elif cast == 'lower':
                return str(value).lower()
            else:
                raise ValueError('UNKNOWN')
        else:
            return value

    def __validtype(self, validset, value):
        validtypes = validset['validtypes']
        for validtype in validtypes:
            if validtype == 'boolean':
                r = self.__validchk_boolean(value=value)
            elif validtype == 'zero':
                r = self.__validchk_zero(number=value)
            elif validtype == 'nonzero':
                r = self.__validchk_nonzero(number=value)
            elif validtype == 'timestamp10':
                r = self.__validchk_timestamp(timestamp=value, num=10)
            elif validtype == 'timestamp13':
                r = self.__validchk_timestamp(timestamp=value, num=13)
            elif validtype == 'timestamp16':
                r = self.__validchk_timestamp(timestamp=value, num=16)
            elif validtype == 'uuid':
                r = self.__validchk_uuid(_uuid=value)
            elif validtype == 'enum':
                r = self.__validchk_enum(value=value, enumSet=validset['enum'])
            else:
                raise ValueError('UNKNOWN')

            if not r:
                return False
        return True

    def __validchk_boolean(self, value):
        if int(value) in (0, 1):
            return True
        else:
            return False

    def __validchk_zero(self, number):
        if int(number) >= 0:
            return True
        else:
            return False

    def __validchk_nonzero(self, number):
        if int(number) > 0:
            return True
        else:
            return False

    def __validchk_timestamp(self, timestamp, num):
        try:
            datetime.datetime.fromtimestamp(int(timestamp) / 10 ** (num - 10))
            return True
        except ValueError:
            return False

    def __validchk_uuid(self, _uuid):
        if re.match('^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', str(_uuid)):
            return True
        else:
            return False

    def __validchk_enum(self, value, enumSet):
        if value in enumSet:
            return True
        else:
            return False


class OrderConversion:
    data = None
    Orderinfo = None

    def __init__(self, currencys, products, RecvMsg):
        self.currencys = currencys
        self.products = products
        self.RecvMsg = RecvMsg
        print("inside orderconversion",RecvMsg)

        if self.RecvMsg['order_type'] == 'MARKET':
            if self.RecvMsg['side'] == 'BUY':
                self.__Market_Buy()
            elif self.RecvMsg['side'] == 'SELL':
                print("inside SELL of MArket")
                r=self.__Market_Sell()
                print(r)
            else:
                raise ValueError('UNKNOWN')
            self.data = self.Orderinfo

        elif self.RecvMsg['order_type'] == 'LIMIT':
            if self.RecvMsg['policy'] == 'GTC':
                self.__Limit_GTC()
            elif self.RecvMsg['policy'] == 'GTT':
                self.__Limit_GTT()
            else:
                raise ValueError('UNKNOWN')
            self.data = self.Orderinfo

        elif self.RecvMsg['order_type'] == 'STOP':
            if self.RecvMsg.has_key('price'):
                self.__Stop_Limit_GTC()
            elif self.RecvMsg['side'] == 'BUY':
                self.__Stop_Market_Buy()
            elif self.RecvMsg['side'] == 'SELL':
                self.__Stop_Market_Sell()
            else:
                raise ValueError('UNKNOWN')
            self.data = {
                'product': self.Orderinfo['product'],
                'order_type': ORDER_TYPE['STOP'],
                'side': self.Orderinfo['side'],
                'stop_price': self.Orderinfo['stop_price'],
                'Orderinfo': self.Orderinfo,
            }

        elif self.RecvMsg['order_type'] == 'CANCEL':
            if self.RecvMsg.has_key('expire_time'):
                self.__ExpireCancel()
            else:
                self.__Cancel()
            self.data = self.Orderinfo

        elif self.RecvMsg['order_type'] == 'STOPCANCEL':
            self.__StopCancel()
            self.data = self.Orderinfo

        else:
            raise ValueError('UNKNOWN')

    def __Base(self):
        self.Orderinfo = {
            'sequence': self.RecvMsg['sequence'],
            'trx_id': self.RecvMsg['trx_id'],
            'product': self.RecvMsg['product'],
            'product_id': self.products[self.RecvMsg['product']]['id'],
            'incoming': 'WEB',
            'broker_id': self.RecvMsg['member_id'],
            'member_id': self.RecvMsg['member_id'],
            'wallet_id': self.RecvMsg['wallet_id'],
            'receive_time': DATETIME_UNIXTIMESTAMP(),
            'open_time': None,
            'done_time': None,
            'bcc': self.products[self.RecvMsg['product']]['bcc'],
            'bcd': self.products[self.RecvMsg['product']]['bcd'],
            'bcu': 10 ** self.products[self.RecvMsg['product']]['bcd'],
            'qcc': self.products[self.RecvMsg['product']]['qcc'],
            'qcd': self.products[self.RecvMsg['product']]['qcd'],
            'qcu': 10 ** self.products[self.RecvMsg['product']]['qcd'],
            'min_size_unit': self.products[self.RecvMsg['product']]['min_size_unit'],
            'min_price_unit': self.products[self.RecvMsg['product']]['min_price_unit'],
        }

    def __Market(self):
        self.Orderinfo.update({
            'order_type': ORDER_TYPE[self.RecvMsg['order_type']],
            'side': SIDE[self.RecvMsg['side']],
            'tfr': self.RecvMsg['tfr'],
            'open_price': None,
            'magicnumber': self.RecvMsg['magicnumber'],
            'f_sequence': self.RecvMsg['sequence'],
            'accumulate_size': 0,
            'accumulate_funds': 0,
            'accumulate_cancelsize': 0,
            'accumulate_fee': 0,
            'order_id': str(uuid.uuid4()),
            'stoporder': 0,
        })

    def __Market_Buy(self):
        self.__Base()
        self.__Market()
        self.Orderinfo.update({
            'funds': self.RecvMsg['funds'],
            'remaining_funds': self.RecvMsg['funds'],
            'amount_type': AMOUNT_TYPE['FUNDS'],
            'hold_currency_code': self.products[self.RecvMsg['product']]['qcc'],
            'hold_digit': self.products[self.RecvMsg['product']]['qcd'],
            'hold_amount': self.RecvMsg['funds'],
        })

    def __Market_Sell(self):
        print("inside market sell")
        self.__Base()
        self.__Market()
        self.Orderinfo.update({
            'size': self.RecvMsg['size'],
            'remaining_size': self.RecvMsg['size'],
            'amount_type': AMOUNT_TYPE['SIZE'],
            'hold_currency_code': self.products[self.RecvMsg['product']]['bcc'],
            'hold_digit': self.products[self.RecvMsg['product']]['bcd'],
            'hold_amount': self.RecvMsg['size'],
        })

    def __Limit(self):
        self.Orderinfo.update({
            'order_type': ORDER_TYPE[self.RecvMsg['order_type']],
            'side': SIDE[self.RecvMsg['side']],
            'size': self.RecvMsg['size'],
            'price': self.RecvMsg['price'],
            'tfr': self.RecvMsg['tfr'],
            'mfr': self.RecvMsg['mfr'],
            'policy': POLICY[self.RecvMsg['policy']],
            'open_price': None,
            'magicnumber': self.RecvMsg['magicnumber'],
            'f_sequence': self.RecvMsg['sequence'],
            'remaining_size': self.RecvMsg['size'],
            'accumulate_size': 0,
            'accumulate_funds': 0,
            'accumulate_cancelsize': 0,
            'accumulate_fee': 0,
            'order_id': str(uuid.uuid4()),
            'amount_type': AMOUNT_TYPE['SIZE'],
            'stoporder': 0,
        })
        if self.Orderinfo['side'] == SIDE['BUY']:
            self.Orderinfo.update({
                'hold_currency_code': self.products[self.RecvMsg['product']]['qcc'],
                'hold_digit': self.products[self.RecvMsg['product']]['qcd'],
                'hold_amount': (self.RecvMsg['price'] * self.RecvMsg['size']) / (10 ** self.products[self.RecvMsg['product']]['bcd']),
            })
        elif self.Orderinfo['side'] == SIDE['SELL']:
            self.Orderinfo.update({
                'hold_currency_code': self.products[self.RecvMsg['product']]['bcc'],
                'hold_digit': self.products[self.RecvMsg['product']]['bcd'],
                'hold_amount': self.RecvMsg['size'],
            })
        else:
            raise ValueError('UNKNOWN')

    def __Limit_GTC(self):
        self.__Base()
        self.__Limit()

    def __Limit_GTT(self):
        self.__Base()
        self.__Limit()
        self.Orderinfo.update({
            'expire_time': int(time.time() + self.RecvMsg['expire_seconds'])
        })

    def __Stop(self):
        self.Orderinfo.update({
            'side': SIDE[self.RecvMsg['side']],
            'tfr': self.RecvMsg['tfr'],
            'accumulate_size': 0,
            'accumulate_funds': 0,
            'accumulate_cancelsize': 0,
            'accumulate_fee': 0,
            'open_price': None,
            'magicnumber': self.RecvMsg['magicnumber'],
            'f_sequence': self.RecvMsg['sequence'],
            'order_id': str(uuid.uuid4()),
            'stoporder': 1,
            'stop_price': self.RecvMsg['stop_price'],
        })

    def __Stop_Limit_GTC(self):
        self.__Base()
        self.__Stop()
        self.Orderinfo.update({
            'order_type': ORDER_TYPE['LIMIT'],
            'size': self.RecvMsg['amount'],
            'price': self.RecvMsg['price'],
            'mfr': self.RecvMsg['mfr'],
            'policy': POLICY['GTC'],
            'remaining_size': self.RecvMsg['amount'],
            'amount_type': AMOUNT_TYPE['SIZE'],
        })
        if self.Orderinfo['side'] == SIDE['BUY']:
            self.Orderinfo.update({
                'hold_currency_code': self.products[self.RecvMsg['product']]['qcc'],
                'hold_digit': self.products[self.RecvMsg['product']]['qcd'],
                'hold_amount': (self.RecvMsg['price'] * self.RecvMsg['amount']) / (10 ** self.products[self.RecvMsg['product']]['bcd']),
            })
        elif self.Orderinfo['side'] == SIDE['SELL']:
            self.Orderinfo.update({
                'hold_currency_code': self.products[self.RecvMsg['product']]['bcc'],
                'hold_digit': self.products[self.RecvMsg['product']]['bcd'],
                'hold_amount': self.RecvMsg['amount'],
            })
        else:
            raise ValueError('UNKNOWN')

    def __Stop_Market_Buy(self):
        self.__Base()
        self.__Stop()
        self.Orderinfo.update({
            'order_type': ORDER_TYPE['MARKET'],
            'funds': self.RecvMsg['amount'],
            'remaining_funds': self.RecvMsg['amount'],
            'amount_type': AMOUNT_TYPE['FUNDS'],
            'hold_currency_code': self.products[self.RecvMsg['product']]['qcc'],
            'hold_digit': self.products[self.RecvMsg['product']]['qcd'],
            'hold_amount': self.RecvMsg['amount'],
        })

    def __Stop_Market_Sell(self):
        self.__Base()
        self.__Stop()
        self.Orderinfo.update({
            'order_type': ORDER_TYPE['MARKET'],
            'size': self.RecvMsg['amount'],
            'remaining_size': self.RecvMsg['amount'],
            'amount_type': AMOUNT_TYPE['SIZE'],
            'hold_currency_code': self.products[self.RecvMsg['product']]['bcc'],
            'hold_digit': self.products[self.RecvMsg['product']]['bcd'],
            'hold_amount': self.RecvMsg['amount'],
        })

    def __Cancel(self):
        self.__Base()
        self.Orderinfo.update({
            'order_type': ORDER_TYPE[self.RecvMsg['order_type']],
            'order_id': self.RecvMsg['order_id'],
            'size': self.RecvMsg['size'],
            'cancel_size': None,
            'cancel_time': None,
        })

    def __ExpireCancel(self):
        self.__Cancel()
        self.Orderinfo.update({
            'expire_time': self.RecvMsg['expire_time'],
        })

    def __StopCancel(self):
        self.__Base()
        self.Orderinfo.update({
            'order_type': ORDER_TYPE[self.RecvMsg['order_type']],
            'order_id': self.RecvMsg['order_id'],
        })


class ResponseType:
    ''' response_type 별 구분 '''

    @staticmethod
    def ResponseSystem(response_code, **kwargs):
        response_msg = {'response_type': 'SYSTEM', 'response_code': RESPONSE_CODE[response_code], 'time': DATETIME_UNIXTIMESTAMP()}
        response_msg.update(kwargs)
        return response_msg

    @staticmethod
    def ResponseOrder(response_code, member_id, product_code, order_type, **kwargs):
        response_msg = {'response_type': 'ORDER', 'response_code': RESPONSE_CODE[response_code], 'time': DATETIME_UNIXTIMESTAMP(),
                        'member_id': member_id, 'product_code': product_code, 'order_type': order_type}
        response_msg.update(kwargs)
        return response_msg


class ResponseCode(ResponseType):
    '''
    resposne_code 별 분류
    인자로 받는건 해당 method에서 str로 치환한다. kwargs처럼 인자 key가 분명치 않은건 입력받은 그대로 전달한다.
    '''

    @classmethod
    def JSON_ERROR(cls, recvmsg):
        r = cls.ResponseSystem(response_code='JSON_ERROR', recvmsg=recvmsg)
        return r

    @classmethod
    def INVALID_PARAM(cls, recvmsg):
        r = cls.ResponseSystem(response_code='INVALID_PARAM', recvmsg=recvmsg)
        return r

    @classmethod
    def INTERNAL_ERROR(cls, member_id, product_code, message=''):
        r = cls.ResponseOrder(response_code='INTERNAL_ERROR', member_id=member_id, product_code=product_code, message=message)
        return r

    @classmethod
    def NOT_ENOUGH_BALANCE(cls, member_id, product_code, order_type, order_id, side, hold_amount, hold_currency_code, **kwargs):
        r = cls.ResponseOrder(response_code='NOT_ENOUGH_BALANCE', member_id=member_id, product_code=product_code, order_type=pORDER_TYPE[order_type],
                              order_id=order_id, side=pSIDE[side], hold_amount=hold_amount, hold_currency_code=hold_currency_code,
                              **kwargs)
        return r

    @classmethod
    def NOT_ALLOW_ORDER(cls, member_id, product_code, order_type, **kwargs):
        r = cls.ResponseOrder(response_code='NOT_ALLOW_ORDER', member_id=member_id, product_code=product_code, order_type=pORDER_TYPE[order_type],
                              **kwargs)
        return r

    @classmethod
    def NOT_ACCUMULATE_SIZE(cls, member_id, product_code, order_type, side, **kwargs):
        r = cls.ResponseOrder(response_code='NOT_ACCUMULATE_SIZE', member_id=member_id, product_code=product_code, order_type=pORDER_TYPE[order_type], side=pSIDE[side],
                              **kwargs)
        return r

    @classmethod
    def NOT_ALLOW_STOP_PRICE(cls, member_id, product_code, order_type2, stop_price, side, **kwargs):
        r = cls.ResponseOrder(response_code='NOT_ALLOW_STOP_PRICE', member_id=member_id, product_code=product_code, order_type='STOP',
                              order_type2=pORDER_TYPE[order_type2], stop_price=stop_price, side=pSIDE[side],
                              **kwargs)
        return r

    @classmethod
    def NOT_FOUND_ORDER_ID(cls, member_id, product_code, order_type, order_id):
        r = cls.ResponseOrder(response_code='NOT_FOUND_ORDER_ID', member_id=member_id, product_code=product_code, order_type=pORDER_TYPE[order_type],
                              order_id=order_id)
        return r


class Response(ResponseCode):
    '''
    response_code에서 상황별 분류
    필수 인자는 code로, 추가인자(kwargs)는 str로 입력
    '''

    @classmethod
    def NOT_FOUND_ORDER_ID_CANCEL(cls, member_id, product_code, order_id):
        r = cls.NOT_FOUND_ORDER_ID(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['CANCEL'], order_id=order_id)
        return r

    @classmethod
    def NOT_FOUND_ORDER_ID_STOPCANCEL(cls, member_id, product_code, order_id):
        r = cls.NOT_FOUND_ORDER_ID(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOPCANCEL'], order_id=order_id)
        return r

    @classmethod
    def NOT_ALLOW_STOP_PRICE_LIMIT(cls, member_id, product_code, stop_price, side, price, size):
        r = cls.NOT_ALLOW_STOP_PRICE(member_id=member_id, product_code=product_code, order_type2=ORDER_TYPE['LIMIT'], stop_price=stop_price, side=side,
                                     price=price, size=size)
        return r

    @classmethod
    def NOT_ALLOW_STOP_PRICE_MARKET_BUY(cls, member_id, product_code, stop_price, funds):
        r = cls.NOT_ALLOW_STOP_PRICE(member_id=member_id, product_code=product_code, order_type2=ORDER_TYPE['MARKET'], stop_price=stop_price, side=SIDE['BUY'],
                                     funds=funds)
        return r

    @classmethod
    def NOT_ALLOW_STOP_PRICE_MARKET_SELL(cls, member_id, product_code, stop_price, size):
        r = cls.NOT_ALLOW_STOP_PRICE(member_id=member_id, product_code=product_code, order_type2=ORDER_TYPE['MARKET'], stop_price=stop_price, side=SIDE['SELL'],
                                     size=size)
        return r

    @classmethod
    def NOT_ACCUMULATE_SIZE_MARKET_BUY(cls, member_id, product_code, funds):
        r = cls.NOT_ACCUMULATE_SIZE(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['MARKET'], side=SIDE['BUY'],
                                    funds=funds)
        return r

    @classmethod
    def NOT_ACCUMULATE_SIZE_MARKET_SELL(cls, member_id, product_code, size):
        r = cls.NOT_ACCUMULATE_SIZE(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['MARKET'], side=SIDE['SELL'],
                                    funds=size)
        return r

    @classmethod
    def NOT_ACCUMULATE_SIZE_STOP_MARKET_BUY(cls, member_id, product_code, stop_price, funds):
        r = cls.NOT_ACCUMULATE_SIZE(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'], side=SIDE['BUY'],
                                    order_type2='MARKET', stop_price=stop_price, funds=funds)
        return r

    @classmethod
    def NOT_ACCUMULATE_SIZE_STOP_MARKET_SELL(cls, member_id, product_code, stop_price, size):
        r = cls.NOT_ACCUMULATE_SIZE(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'], side=SIDE['SELL'],
                                    order_type2='MARKET', stop_price=stop_price, size=size)
        return r

    @classmethod
    def NOT_ACCUMULATE_SIZE_STOP_LIMIT(cls, member_id, product_code, side, stop_price, price, size):
        r = cls.NOT_ACCUMULATE_SIZE(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'], side=side,
                                    order_type2='LIMIT', stop_price=stop_price, price=price, size=size)
        return r

    @classmethod
    def NOT_ENOUGH_BALANCE_STOP(cls, member_id, product_code, order_id, side, hold_amount, hold_currency_code, order_type2):
        r = cls.NOT_ENOUGH_BALANCE(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'], order_id=order_id, side=side, hold_amount=hold_amount,
                                   hold_currency_code=hold_currency_code,
                                   order_type2=pORDER_TYPE[order_type2])
        return r

    @classmethod
    def NOT_ALLOW_ORDER_LIMIT(cls, member_id, product_code, side, price, size):
        r = cls.NOT_ALLOW_ORDER(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['LIMIT'],
                                side=pSIDE[side], price=price, size=size)
        return r

    @classmethod
    def NOT_ALLOW_ORDER_MARKET_BUY(cls, member_id, product_code, funds):
        r = cls.NOT_ALLOW_ORDER(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['MARKET'],
                                side='BUY', funds=funds)
        return r

    @classmethod
    def NOT_ALLOW_ORDER_MARKET_SELL(cls, member_id, product_code, size):
        r = cls.NOT_ALLOW_ORDER(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['MARKET'],
                                side='SELL', size=size)
        return r

    @classmethod
    def NOT_ALLOW_ORDER_STOP_LIMIT(cls, member_id, product_code, stop_price, side, price, size):
        r = cls.NOT_ALLOW_ORDER(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'],
                                order_type2='LIMIT', stop_price=stop_price, side=pSIDE[side], price=price, size=size)
        return r

    @classmethod
    def NOT_ALLOW_ORDER_STOP_MARKET_BUY(cls, member_id, product_code, stop_price, funds):
        r = cls.NOT_ALLOW_ORDER(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'],
                                order_type2='MARKET', stop_price=stop_price, side='BUY', funds=funds)
        return r

    @classmethod
    def NOT_ALLOW_ORDER_STOP_MARKET_SELL(cls, member_id, product_code, stop_price, size):
        r = cls.NOT_ALLOW_ORDER(member_id=member_id, product_code=product_code, order_type=ORDER_TYPE['STOP'],
                                order_type2='LIMIT', stop_price=stop_price, side='SELL', size=size)
        return r


class exchangeConfigParser:
    isvalid = None
    errormsg = None

    def __init__(self, config_file, config_arguments):
        self.config_file = config_file
        self.config_arguments = config_arguments

        if not os.path.exists(config_file):
            self.isvalid = False
            self.errormsg = 'not exists `%s`' % (self.config_file,)
            return

        try:
            self.parsing()
            return
        except Exception as err:
            self.isvalid = False
            self.errormsg = err
            return

    def setItem(self, section, key, value):
        try:
            getattr(self, section)
        except AttributeError:
            setattr(self, section, dict())
        getattr(self, section)[key] = value

    def parsing(self):
        config = ConfigParser.ConfigParser()
        config.read(self.config_file)
        for section, args in self.config_arguments.items():
            for arg in args:
                getvalue = getattr(config, arg['method'])
                try:
                    value = getvalue(section, arg['key'])
                except ConfigParser.NoOptionError as err:
                    if arg['defaultvalue'] == None:
                        self.isvalid = False
                        self.errormsg = err
                        return
                    else:
                        value = arg['defaultvalue']
                self.setItem(section=section, key=arg['key'], value=value)
        self.isvalid = True


class S3:
    __con = None

    @classmethod
    def __connect(cls):
        if cls.__con == None:
            cls.__con = S3Connection(cls.aws_access_key_id, cls.aws_secret_access_key)
            cls.__bucket = cls.__con.get_bucket(cls.aws_bucket)

    @classmethod
    def aws_s3_info_set(cls, aws_access_key_id, aws_secret_access_key, aws_bucket):
        cls.aws_access_key_id = aws_access_key_id
        cls.aws_secret_access_key = aws_secret_access_key
        cls.aws_bucket = aws_bucket

    @classmethod
    def set_string(cls, key, content):
        cls.__connect()

        s3key = Key(cls.__bucket)
        s3key.key = key
        s3key.set_contents_from_string(content)
        return s3key

    @classmethod
    def get(cls, key):
        cls.__connect()
        return cls.__bucket.get_key(key)

    @classmethod
    def list(cls, preifx):
        cls.__connect()
        return cls.__bucket.list(preifx)


class WalletSocketClient:
    FCODE = {
        'HEALTHCHECK': 0,
        'HOLDUP': 1,
        'HOLDDN': 2,
        'EXCHANGES': 3,
        'TRXID': 1000,
    }
    disconnecctd_message = 'request disconnected'

    def __init__(self, host, port, log=None):
        self.log = log
        self.setup(host=host, port=port)

    def setup(self, host, port):
        self.host = host
        self.port = port

        # PacketHeader
        self.Head = struct.Struct('!36sHII')  # Unsigned Short(2), Unsigned Int(4), Unsigned Int(4)
        # [ Function Code | Not Used | Data Length ]

        self.request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # socket stream 생성
        self.request.connect((self.host, self.port))

    def close(self):
        self.request.close()
        del self.request

    def SendPacket(self, Fcode, Data, trx_id=None):
        if trx_id == None:
            trx_id = '00000000-0000-0000-0000-000000000000'
        Packet = self.Head.pack(str(trx_id), Fcode, 0, len(Data))
        Packet += struct.pack('!%ds' % (len(Data),), Data)
        self.request.sendall(Packet)
        return trx_id, Fcode, len(Data), Data

    def ReceivePacket(self, MAXBUFFSIZE=1024):
        Packet = self.request.recv(self.Head.size)
        if not Packet:
            raise socket.error(self.disconnecctd_message)
        trx_id, Fcode, _, DataLength = self.Head.unpack(Packet)
        Body = struct.Struct('!%ds' % (DataLength,))
        Packet = ''
        while DataLength > 0:
            receivepacket = self.request.recv(min((MAXBUFFSIZE, DataLength)))
            if receivepacket:
                DataLength -= len(receivepacket)
                Packet += receivepacket
            else:
                raise socket.error(self.disconnecctd_message)
        Data = Body.unpack(Packet)[0]
        return trx_id, Fcode, Body.size, Data

    def HealthCheck(self):
        self.SendPacket(Fcode=self.FCODE['HEALTHCHECK'], Data='ping')
        return True

    def HoldUp(self, wallet_id, currency_code, amount, trx_id=None):
        try:
            self.SendPacket(Fcode=self.FCODE['HOLDUP'], Data=json.dumps({'wallet_id': wallet_id, 'currency_code': currency_code, 'amount': amount}), trx_id=trx_id)
        except socket.error as err:
            if err.errno == 32:  # [Errno 32] Broken pipe <- timeout 발생했을때 들어오는 부분이다.
                del self.request
                self.request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # socket stream 생성
                self.request.connect((self.host, self.port))
                self.SendPacket(Fcode=self.FCODE['HOLDUP'], Data=json.dumps({'wallet_id': wallet_id, 'currency_code': currency_code, 'amount': amount}), trx_id=trx_id)
            else:
                raise socket.error(err)
        trx_id, RecvFcode, RecvDataLength, RecvData = self.ReceivePacket()
        return json.loads(RecvData)

    def HoldDn(self, wallet_id, currency_code, amount, trx_id=None):
        try:
            self.SendPacket(Fcode=self.FCODE['HOLDDN'], Data=json.dumps({'wallet_id': wallet_id, 'currency_code': currency_code, 'amount': amount}), trx_id=trx_id)
        except socket.error as err:
            if err.errno == 32:  # [Errno 32] Broken pipe <- timeout 발생했을때 들어오는 부분이다.
                del self.request
                self.request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # socket stream 생성
                self.request.connect((self.host, self.port))
                self.SendPacket(Fcode=self.FCODE['HOLDDN'], Data=json.dumps({'wallet_id': wallet_id, 'currency_code': currency_code, 'amount': amount}), trx_id=trx_id)
            else:
                raise socket.error(err)
        trx_id, RecvFcode, RecvDataLength, RecvData = self.ReceivePacket()
        return json.loads(RecvData)

    def Exchanges(self, Takerinfo, Makerinfos, Feeinfos, trade_id, trx_id=None):
        try:
            self.SendPacket(Fcode=self.FCODE['EXCHANGES'], Data=json.dumps({'Takerinfo': Takerinfo, 'Makerinfos': Makerinfos, 'Feeinfos': Feeinfos, 'trade_id': trade_id}), trx_id=trx_id)
        except socket.error as err:
            if err.errno == 32:  # [Errno 32] Broken pipe <- timeout 발생했을때 들어오는 부분이다.
                del self.request
                self.request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # socket stream 생성
                self.request.connect((self.host, self.port))
                self.SendPacket(Fcode=self.FCODE['EXCHANGES'], Data=json.dumps({'Takerinfo': Takerinfo, 'Makerinfos': Makerinfos, 'Feeinfos': Feeinfos, 'trade_id': trade_id}), trx_id=trx_id)
            else:
                raise socket.error(err)
        trx_id, RecvFcode, RecvDataLength, RecvData = self.ReceivePacket()
        return json.loads(RecvData)

    def TrxId(self, trx_id):
        try:
            self.SendPacket(Fcode=self.FCODE['TRXID'], Data=json.dumps({'trx_id': trx_id}), trx_id=None)
        except socket.error as err:
            if err.errno == 32:  # [Errno 32] Broken pipe <- timeout 발생했을때 들어오는 부분이다.
                del self.request
                self.request = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # socket stream 생성
                self.request.connect((self.host, self.port))
                self.SendPacket(Fcode=self.FCODE['TRXID'], Data=json.dumps({'trx_id': trx_id}), trx_id=None)
            else:
                raise socket.error(err)
        trx_id, RecvFcode, RecvDataLength, RecvData = self.ReceivePacket()
        return json.loads(RecvData)


class WalletDB:
    def __init__(self, WALLETDB, WALLETCACHE=None):
        self.WALLETDB = WALLETDB
        self.con = MySQLdb.connect(**self.WALLETDB)
        self.cur = self.con.cursor()

        if WALLETCACHE != None:
            self.cache = redis.Redis(**WALLETCACHE)

        self.currencys = dict()
        self.dsp_currencys = dict()

        self.cur.execute("SELECT `id`, `code`, `digit`, `init_fiat_price`, `isdsp` FROM `bmex:wallet:currency`;")
        description = [t[0] for t in self.cur.description]
        for row in self.cur.fetchall():
            currency = {k: v for (k, v) in zip(description, row)}
            self.currencys[currency['code']] = {'id': currency['id'], 'digit': currency['digit'], 'init_fiat_price': currency['init_fiat_price']}
            if currency['isdsp']:
                self.dsp_currencys[currency['code']] = {'id': currency['id'], 'digit': currency['digit'], 'init_fiat_price': currency['init_fiat_price']}

    def healthcheck(self):
        try:
            self.cur.execute("SELECT 1;")
        except MySQLdb.OperationalError as err:
            if err[0] == 2006:  # MySQL server has gone away
                self.con = MySQLdb.connect(**self.WALLETDB)
                self.cur = self.con.cursor()
            elif err[0] == 2013:  # Lost connection to MySQL server during query
                self.con = MySQLdb.connect(**self.WALLETDB)
                self.cur = self.con.cursor()
            else:
                raise MySQLdb.OperationalError(err)

    def holdup(self, wallet_id, currency_code, amount, trx_id=None):
        print("wallet_id",wallet_id,"amount",amount)
        sql = "UPDATE `bmex:wallet:{currency_code}:balance` AS balance JOIN `bmex:wallet:{currency_code}:hold` AS hold ON balance.wallet_id = hold.wallet_id " \
              "SET hold.amount = hold.amount + %(amount)s WHERE balance.amount - hold.amount >= %(amount)s AND balance.`wallet_id`=%(wallet_id)s;".format(currency_code=currency_code)
        kwargs = {'amount': amount, 'wallet_id': wallet_id}
        if self.cur.execute(sql, kwargs):
            self.con.commit()
            return True, None
        else:
            self.con.rollback()
            return False, 'not_enougn_balance'

    def holddn(self, wallet_id, currency_code, amount, trx_id=None):
        sql = 'UPDATE `bmex:wallet:{currency_code}:hold` SET `amount` = `amount` - %(amount)s WHERE `wallet_id` = %(wallet_id)s AND `amount` - %(amount)s >= 0;'.format(currency_code=currency_code)
        kwargs = {'wallet_id': wallet_id, 'amount': amount}
        if self.cur.execute(sql, kwargs):
            self.con.commit()
            return True, None
        else:
            self.con.rollback()
            return False, None

    def GetFinalClosingPrice(self, product_code, product_price=dict()):
        ''' 최종 체결가 가져오기 '''
        try:
            print("product code",product_code)
            print("product_dict",product_price)
            price = product_price[product_code]
            if price == 0:
                price = 93400000
            print("inside the try price",price)
        except KeyError:
            price = self.cache.hget('PRICE', product_code)
            print(" hget price which we get in key error",price)
            price = '1000'
            price = int(price)
            #print("the price getfinal closing one",product_price[product_code])
        return price

    def ValueConversion(self, amount, amount_currency_code, display_currency_code, products=dict()):  # 구 fund_calc
        ''' 가치 환산 '''
        print("product",products)
        if amount_currency_code == display_currency_code:
            #print("the currency code we are getting in value conversion inside first if",display_currency_code)
            return amount, 10 ** self.currencys[amount_currency_code]['digit']
        else:
            #print("inside the else statment",amount_currency_code)
            if amount_currency_code == FIAT:  # ex) krw -> btc
                #print("amount_currency_code",FIAT)
                product_b = '%s%s' % (display_currency_code, FIAT,)
                print("product_code,product_price",product_b,products)
                _price = self.GetFinalClosingPrice(product_code=product_b, product_price=products)  # 실제 price
                #print("product code product_price",product_b,products)
                r2=self.currencys[display_currency_code]['digit']
                print("value converstion price ",_price,"currency_code = ",r2)
                _price = int(_price)
                #i have to remove this code 
            #    if _price == 0:
            #       _price = 234

                _price = int((10 ** self.currencys[display_currency_code]['digit'] * 10 ** self.currencys[amount_currency_code]['digit']) / _price)  # 역수 취한 price
                v1 = (amount * _price) / (10 ** (self.currencys[amount_currency_code]['digit'] - R_DIGIT))
                _funds = int(round(v1, -R_DIGIT)) / (10 ** R_DIGIT)
            elif display_currency_code == FIAT:  # ex) btc -> krw
                product_a = '%s%s' % (amount_currency_code, FIAT,)
                _price = self.GetFinalClosingPrice(product_code=product_a, product_price=products)
                v1 = int((amount * _price) / (10 ** (self.currencys[amount_currency_code]['digit'] - R_DIGIT)))
                _funds = int(round(v1, -R_DIGIT)) / (10 ** R_DIGIT)
            else:  # ex) eth -> btc
                product_a = '%s%s' % (amount_currency_code, FIAT,)
                product_b = '%s%s' % (display_currency_code, FIAT,)
                product_a_fcp = self.GetFinalClosingPrice(product_code=product_a, product_price=products)
                product_a_fcp = int(product_a_fcp)
                product_b_fcp = self.GetFinalClosingPrice(product_code=product_b, product_price=products)
                #product_b_fcp = int(product_b_fcp)
                fml=self.currencys[display_currency_code]['digit']
                #print('the formula val of v1 = ',product_a_fcp+1,'currency code digit = ',fml+1,'R digit = ',R_DIGIT+1)
                #v1 = product_a_fcp * (10 ** (self.currencys[display_currency_code]['digit'] + R_DIGIT)) / product_b_fcp
                v1 = int(product_a_fcp * (10 ** (self.currencys[display_currency_code]['digit'] + R_DIGIT)) / product_b_fcp)
                _price = int(round(v1, -R_DIGIT)) / (10 ** R_DIGIT)
                v1 = (amount * _price) / 10 ** (self.currencys[amount_currency_code]['digit'] - R_DIGIT)
                _funds = int(round(v1, -R_DIGIT)) / (10 ** R_DIGIT)
        return _funds, _price

    def SetDateClosingPrice(self, product_code, price, ordinal=None):
        ''' QuoteCurrencyCode가 Fiat인것만 입력 '''
        if product_code[-len(FIAT):] == FIAT:
            if ordinal == None:
                # ordinal = datetime.datetime.now(tz=UTC()).toordinal() # UTC
                ordinal = datetime.datetime.now().toordinal()  # KST
            price = int(price)
            pipeline = self.cache.pipeline(transaction=True)
            pipeline.hset('PRICE', product_code, price)
            pipeline.hset('PRICE:HASH:{product_name}'.format(product_name=product_code), ordinal, price)
            pipeline.hset('PRICE:HASH:{product_name}'.format(product_name=product_code), ordinal + 1, price)
            pipeline.zadd('PRICE:ZSET:{product_name}'.format(product_name=product_code), ordinal, ordinal)
            pipeline.execute()

    def _exchanges_set_detail(self, wallet_id, ext_id, product, currency_code, digit, inout, side, trade_time, amount, dsp_currency_funds, fee, bcc, qcc):
    
        table = "bmex:wallet:{currency_code}:detail".format(currency_code=currency_code)
        funds_sql = {'funds_fields': '', 'funds_values': '', 'funds_updates': '', 'funds_kwargs': dict()}
        for dsp_currency_code in self.dsp_currencys:  # detail 입력하기
            key = 'funds_{currency_code}'.format(currency_code=dsp_currency_code)
            print("key",key)
            funds_sql['funds_fields'] += '`{key}`,'.format(key=key)
            print("lor",'`{key}`,'.format(key=key))
            funds_sql['funds_values'] += '%({key})s,'.format(key=key)
            funds_sql['funds_updates'] += '`{key}`=`{key}`+%({key})s,'.format(key=key)
            funds_sql['funds_kwargs'][key] = dsp_currency_funds[dsp_currency_code]
        funds_sql['funds_fields'] = funds_sql['funds_fields'].strip(', ')
        print("strip funds fields",funds_sql['funds_fields'].strip(', '))
        funds_sql['funds_values'] = funds_sql['funds_values'].strip(', ')
        funds_sql['funds_updates'] = funds_sql['funds_updates'].strip(', ')
        sql = 'INSERT INTO `{table}` ' \
              '(`ext_id`,`wallet_id`,`product`,`inout`,`side`,`time`,`amount`,`digit`,`fee`,`time_days`, `reg_date`,`bcc`,`qcc`, {funds_fields}) ' \
              'VALUES ' \
              '(%(ext_id)s,%(wallet_id)s,%(product)s,%(inout)s,%(side)s,%(time)s,%(amount)s,%(digit)s,%(fee)s,%(time_days)s, %(reg_date)s, %(bcc)s, %(qcc)s, {funds_values}) ' \
              'ON DUPLICATE KEY UPDATE ' \
              '`time`=%(time)s, `amount`=`amount`+%(amount)s, `fee`=`fee`+%(fee)s, {funds_updates}'.format(
            table=table, funds_fields=funds_sql['funds_fields'], funds_values=funds_sql['funds_values'], funds_updates=funds_sql['funds_updates'])
        kwargs = {'wallet_id': wallet_id, 'ext_id': ext_id, 'product': product, 'inout': inout, 'side': side, 'time': trade_time,
                  'reg_date': DATETIME_UNIXTIMESTAMP(), 'amount': amount, 'digit': digit, 'fee': fee, 'time_days': UNIXTIMESTAMP_DATETIME(trade_time).toordinal(), 'bcc': bcc, 'qcc': qcc}
        kwargs.update(funds_sql['funds_kwargs'])
        self.cur.execute(sql, kwargs)

    def _exchanges_set_fee(self, Feeinfos):
        sql = 'INSERT INTO `bmex:wallet:fee` SET ' \
              '`wallet_id`=%(wallet_id)s, `ext_id`=%(ext_id)s, `inout`=%(inout)s, `side`=%(side)s, `currency_code`=%(currency_code)s, ' \
              '`currency_digit`=%(currency_digit)s, `fee`=%(fee)s, `time`=%(time)s, `time_days`=%(time_days)s;'
        for Feeinfo in Feeinfos:
            kwargs = {'wallet_id': Feeinfo['wallet_id'], 'ext_id': Feeinfo['trade_id'],
                      'inout': WALLET_DETAIL_INOUT['IN'], 'side': WALLET_DETAIL_SIDE[Feeinfo['side']],
                      'currency_code': Feeinfo['currency_code'], 'currency_digit': Feeinfo['currency_digit'],
                      'fee': Feeinfo['fee'], 'time': Feeinfo['trade_time'], 'time_days': UNIXTIMESTAMP_DATETIME(Feeinfo['trade_time']).toordinal(), }
            self.cur.execute(sql, kwargs)

    def _exchanges_set_balance_in(self, wallet_id, currency_code, amount):
        sql = 'UPDATE `bmex:wallet:{currency_code}:balance` SET `amount`=ROUND(`amount`+%(amount)s, %(digit)s) WHERE `wallet_id`=%(wallet_id)s;'.format(currency_code=currency_code)
        kwargs = {'wallet_id': wallet_id, 'amount': amount, 'digit': self.currencys[currency_code]['digit']}
        self.cur.execute(sql, kwargs)

    def _exchanges_set_balance_out(self, wallet_id, currency_code, amount, hold_amount):
        sql = 'UPDATE `bmex:wallet:{currency_code}:balance` AS balance JOIN `bmex:wallet:{currency_code}:hold` AS hold ' \
              'ON balance.wallet_id = hold.wallet_id ' \
              'SET balance.amount=ROUND(balance.amount-%(amount)s, %(digit)s), hold.amount=ROUND(hold.amount-%(hold_amount)s, %(digit)s) ' \
              'WHERE ROUND(balance.amount-%(amount)s, %(digit)s) >= 0 AND ROUND(hold.amount-%(hold_amount)s, %(digit)s) >= 0 AND balance.wallet_id=%(wallet_id)s;'.format(currency_code=currency_code)
        kwargs = {'wallet_id': wallet_id, 'amount': amount, 'hold_amount': hold_amount, 'digit': self.currencys[currency_code]['digit']}
        self.cur.execute(sql, kwargs)

    def _execute_summary_averageprice(self, wallet_id, amount, amount_currency_code, dsp_currency_price, inout):
        ''' summary avgp 구하는 공식은 sql문으로 처리하는데 bigint의 허용 범위를 벗어나 error가 발생하는 문제로 인하여
        해당 부분만 double형태로 입력하고 계산처리하도록 한다.
        detail_summary api에서 요청할때는 10**digit으로 계산하여 integer형태로 return을 주기는 한다.'''
        print("checking sql the values",wallet_id,amount,amount_currency_code,dsp_currency_price,inout)
        amount_digit = self.currencys[amount_currency_code]['digit']
        print("getting the amount digit",amount_digit)
        kwargs = {'wallet_id': wallet_id, 'amount': amount, 'digit': amount_digit}
        sql = 'UPDATE `bmex:wallet:{currency_code}:summary` SET '.format(currency_code=amount_currency_code)
        if inout == WALLET_DETAIL_INOUT['IN']:
            print("inside inout working fine",inout)
            for dsp_currency_code in self.dsp_currencys:
                print("dsp_currency_code",dsp_currency_code)
                sql += '`avgp_{dsp_currency_code}` = ((`netposition` / POWER(10, %(digit)s)) * `avgp_{dsp_currency_code}` + %(amount)s * %({dsp_currency_code}_price)s) / ((`netposition` / POWER(10, %(digit)s)) + %(amount)s), '.format(
                    dsp_currency_code=dsp_currency_code)
                kwargs['{currency}_price'.format(currency=dsp_currency_code)] = dsp_currency_price[dsp_currency_code]
            sql += '`netposition` = `netposition` + ROUND(%(amount)s * POWER(10, %(digit)s), 0), '
        else:  # OUT
            print("inside the out part")
            sql += '`netposition` = `netposition` - ROUND(%(amount)s * POWER(10, %(digit)s), 0), '
        sql = sql.strip(', ') + ' WHERE `wallet_id`=%(wallet_id)s;'
        if not self.cur.execute(sql, kwargs):
            print("querry not working right now")
            print("checking the values going there",amount_currency_code,)
            #sql = 'INSERT INTO `bmex:wallet:{currency_code}:summary` SET `wallet_id`=%(wallet_id)s, `netposition`='0', '.format(
            #    currency_code=amount_currency_code)
            sql = 'INSERT INTO `bmex:wallet:{currency_code}:summary` SET `wallet_id`=%(wallet_id)s,'.format(
                currency_code=amount_currency_code)
            if inout == WALLET_DETAIL_INOUT['IN']:
                print("inside the part of in where querry of above is not working ")
                for dsp_currency_code in self.dsp_currencys:
                    sql += '`avgp_{cc}`=%({cc}_price)s, '.format(cc=dsp_currency_code)
            sql = sql.strip(', ') + ';'
            print("the querry",sql)
            r=self.cur.execute(sql, kwargs)
            print("checking the querry response",r)
        return self.cur._executed

    def SetTradeAmountHour(self, wallet_id, amount, trade_time):
        '''시간당 누적거래대금 입력하기'''
        _unixtime = trade_time / 10 ** 6
        timestamp = _unixtime - (_unixtime % 3600)
        self._execute_TradeAmountHour_insert(wallet_id=wallet_id, amount=amount, timestamp=timestamp)

    def _execute_TradeAmountHour_insert(self, wallet_id, amount, timestamp):
        sql = "INSERT INTO `bmex:wallet:trade_amount_hour` (`wallet_id`,`amount`,`timestamp`) VALUES (%(wallet_id)s, %(amount)s, %(timestamp)s) " \
              "ON DUPLICATE key update `amount`=`amount`+%(amount)s;"
        kwargs = {'wallet_id': wallet_id, 'amount': amount, 'timestamp': timestamp}
        self.cur.execute(sql, kwargs)
        return self.cur._executed

    def exchanges(self, Takerinfo, Makerinfos, Feeinfos, trade_id):
        Takerinfo['trade_size_converted_value'] = dict()
        Takerinfo['trade_funds_converted_value'] = dict()
        _products = {Takerinfo['product']: Takerinfo['trade_price']}
        print("products of ex = ",_products)

        for dsp_currency_code in self.dsp_currencys:
            #print("values going for the trade size",Takerinfo['trade_size'] - Takerinfo['fee'][Takerinfo['tbmq']],Takerinfo['bcc'])
            tscv, _price = self.ValueConversion(amount=Takerinfo['trade_size'] - Takerinfo['fee'][Takerinfo['tbmq']], amount_currency_code=Takerinfo['bcc'], display_currency_code=dsp_currency_code,
                                                products=_products) 
            #print("value going for the trade funds",Takerinfo['trade_funds'] - Takerinfo['fee'][Takerinfo['tqmb']])                                                                       
            tfcv, _price = self.ValueConversion(amount=Takerinfo['trade_funds'] - Takerinfo['fee'][Takerinfo['tqmb']], amount_currency_code=Takerinfo['qcc'], display_currency_code=dsp_currency_code,
                                                products=_products)
            #print("line 13702",tfcv,_price)                                    
            Takerinfo['trade_size_converted_value'][dsp_currency_code] = tscv
            Takerinfo['trade_funds_converted_value'][dsp_currency_code] = tfcv

        for Makerinfo in Makerinfos:
            self.SetDateClosingPrice(product_code=Takerinfo['product'], price=Makerinfo['trade_price'], ordinal=UNIXTIMESTAMP_DATETIME(Makerinfo['trade_time']).toordinal())
            _products = {Takerinfo['product']: Makerinfo['trade_price']}
            Makerinfo['trade_size_converted_value'] = dict()
            Makerinfo['trade_funds_converted_value'] = dict()
            for dsp_currency_code in self.dsp_currencys:
                tscv, _price = self.ValueConversion(amount=Makerinfo['trade_size'] - Makerinfo['fee'][Takerinfo['tqmb']], amount_currency_code=Takerinfo['bcc'],
                                                    display_currency_code=dsp_currency_code, products=_products)
                print("value conversion loop",tscv,_price)                                    
                tfcv, _price = self.ValueConversion(amount=Makerinfo['trade_funds'] - Makerinfo['fee'][Takerinfo['tbmq']], amount_currency_code=Takerinfo['qcc'],
                                                    display_currency_code=dsp_currency_code, products=_products)
                print("value conversion loop2",tfcv,_price)                                    
                Makerinfo['trade_size_converted_value'][dsp_currency_code] = tscv
                Makerinfo['trade_funds_converted_value'][dsp_currency_code] = tfcv

        ### Detail
        ## Taker Quote
        d_calc = lambda amount, inout: abs(amount) if inout == 'IN' else -abs(amount)
        print(d_calc)

        self._exchanges_set_detail(wallet_id=Takerinfo['wallet_id'], ext_id=Takerinfo['order_id'], product=Takerinfo['product'], currency_code=Takerinfo['qcc'],
                                   digit=self.currencys[Takerinfo['qcc']]['digit'],
                                   inout=WALLET_DETAIL_INOUT[Takerinfo['tqmb']], side=Takerinfo['side'], trade_time=Takerinfo['trade_time'],
                                   amount=d_calc(Takerinfo['trade_funds'], Takerinfo['tqmb']) - Takerinfo['fee'][Takerinfo['tqmb']],
                                   dsp_currency_funds=Takerinfo['trade_funds_converted_value'], fee=Takerinfo['fee'][Takerinfo['tqmb']],
                                   bcc=Takerinfo['bcc'], qcc=Takerinfo['qcc'], )
        ## Maker Quote
        for Makerinfo in Makerinfos:
            self._exchanges_set_detail(wallet_id=Makerinfo['wallet_id'], ext_id=Makerinfo['order_id'], product=Takerinfo['product'], currency_code=Takerinfo['qcc'],
                                       digit=self.currencys[Takerinfo['qcc']]['digit'],
                                       inout=WALLET_DETAIL_INOUT[Takerinfo['tbmq']], side=Makerinfo['side'], trade_time=Makerinfo['trade_time'],
                                       amount=d_calc(Makerinfo['trade_funds'], Takerinfo['tbmq']) - Makerinfo['fee'][Takerinfo['tbmq']],
                                       dsp_currency_funds=Makerinfo['trade_funds_converted_value'], fee=Makerinfo['fee'][Takerinfo['tbmq']],
                                       bcc=Takerinfo['bcc'], qcc=Takerinfo['qcc'], )
        ## Taker Base
        self._exchanges_set_detail(wallet_id=Takerinfo['wallet_id'], ext_id=Takerinfo['order_id'], product=Takerinfo['product'], currency_code=Takerinfo['bcc'],
                                   digit=self.currencys[Takerinfo['bcc']]['digit'],
                                   inout=WALLET_DETAIL_INOUT[Takerinfo['tbmq']], side=Takerinfo['side'], trade_time=Takerinfo['trade_time'],
                                   amount=d_calc(Takerinfo['trade_size'], Takerinfo['tbmq']) - Takerinfo['fee'][Takerinfo['tbmq']],
                                   dsp_currency_funds=Takerinfo['trade_size_converted_value'], fee=Takerinfo['fee'][Takerinfo['tbmq']],
                                   bcc=Takerinfo['bcc'], qcc=Takerinfo['qcc'], )
        ## Maker Base
        for Makerinfo in Makerinfos:
            self._exchanges_set_detail(wallet_id=Makerinfo['wallet_id'], ext_id=Makerinfo['order_id'], product=Takerinfo['product'], currency_code=Takerinfo['bcc'],
                                       digit=self.currencys[Takerinfo['bcc']]['digit'],
                                       inout=WALLET_DETAIL_INOUT[Takerinfo['tqmb']], side=Makerinfo['side'], trade_time=Makerinfo['trade_time'],
                                       amount=d_calc(Makerinfo['trade_size'], Takerinfo['tqmb']) - Makerinfo['fee'][Takerinfo['tqmb']],
                                       dsp_currency_funds=Makerinfo['trade_size_converted_value'], fee=Makerinfo['fee'][Takerinfo['tqmb']],
                                       bcc=Takerinfo['bcc'], qcc=Takerinfo['qcc'], )

        ### Fee
        self._exchanges_set_fee(Feeinfos=Feeinfos)

        ### Balance & Hold / Summary
        # 데이터 담는 부분
        balance_data = list()
        summary_data = list()
        wallet_summary_data = list()
        trade_amount_data = list()  # FIAT 거래대금

        if Takerinfo['tqmb'] == 'IN':  # Taker Sell, Maker Buy]
            # Taker Quote In
            # balance
            balance_data.append({'flag': 'IN', 'wallet_id': Takerinfo['wallet_id'], 'currency_code': Takerinfo['qcc'], 'amount': Takerinfo['trade_funds'] - Takerinfo['fee']['IN']})
            # summary
            _trade_funds = Takerinfo['trade_funds'] - Takerinfo['fee']['IN']
            if _trade_funds == 0:
                _trade_funds = 1
            print("trade funds====",_trade_funds)
            _amount = float(_trade_funds) / 10 ** self.currencys[Takerinfo['qcc']]['digit']
            print("trade amount====",_amount)
            _dsp_currency_price = {k: float((v * 10 ** self.currencys[Takerinfo['qcc']]['digit']) / _trade_funds) / 10 ** self.currencys[k]['digit'] for (k, v) in
                                   Takerinfo['trade_funds_converted_value'].items()}
            summary_data.append(
                {'wallet_id': Takerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['qcc'], 'inout': WALLET_DETAIL_INOUT['IN'], 'dsp_currency_price': _dsp_currency_price})

            # Taker Base Out
            # balance & hold
            balance_data.append({'flag': 'OUT', 'wallet_id': Takerinfo['wallet_id'], 'currency_code': Takerinfo['bcc'], 'amount': Takerinfo['trade_size'], 'hold_amount': Takerinfo['hold_amount']})
            # summary
            _amount = float(Takerinfo['trade_size']) / 10 ** self.currencys[Takerinfo['bcc']]['digit']
            summary_data.append({'wallet_id': Takerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['bcc'], 'inout': WALLET_DETAIL_INOUT['OUT'], 'dsp_currency_price': None})
            # FIAT 거래대금
            trade_amount_data.append({'wallet_id': Takerinfo['wallet_id'], 'trade_amount_fiat': Takerinfo['trade_size_converted_value'][FIAT], 'trade_time': Takerinfo['trade_time']})

            # Maker Quote Out
            for Makerinfo in Makerinfos:
                # balance & hold
                balance_data.append(
                    {'flag': 'OUT', 'wallet_id': Makerinfo['wallet_id'], 'currency_code': Takerinfo['qcc'], 'amount': Makerinfo['trade_funds'], 'hold_amount': Makerinfo['hold_amount']})
                # summary
                _amount = float(Makerinfo['trade_funds']) / 10 ** self.currencys[Takerinfo['qcc']]['digit']
                summary_data.append({'wallet_id': Makerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['qcc'], 'inout': WALLET_DETAIL_INOUT['OUT'], 'dsp_currency_price': None})
                # FIAT 거래대금
                trade_amount_data.append({'wallet_id': Makerinfo['wallet_id'], 'trade_amount_fiat': Makerinfo['trade_funds_converted_value'][FIAT], 'trade_time': Makerinfo['trade_time']})

            # Maker Base In
            for Makerinfo in Makerinfos:
                # balance
                balance_data.append({'flag': 'IN', 'wallet_id': Makerinfo['wallet_id'], 'currency_code': Takerinfo['bcc'], 'amount': Makerinfo['trade_size'] - Makerinfo['fee']['IN']})
                # summary
                _trade_size = Makerinfo['trade_size'] - Makerinfo['fee']['IN']
                _amount = float(_trade_size) / 10 ** self.currencys[Takerinfo['bcc']]['digit']
                _dsp_currency_price = {k: float((v * 10 ** self.currencys[Takerinfo['bcc']]['digit']) / _trade_size) / 10 ** self.currencys[k]['digit'] for (k, v) in
                                       Makerinfo['trade_size_converted_value'].items()}
                summary_data.append(
                    {'wallet_id': Makerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['bcc'], 'inout': WALLET_DETAIL_INOUT['IN'], 'dsp_currency_price': _dsp_currency_price})
        else:  # Taker Buy, Maker Sell
            # Taker Quote Out
            # balance & hold
            balance_data.append({'flag': 'OUT', 'wallet_id': Takerinfo['wallet_id'], 'currency_code': Takerinfo['qcc'], 'amount': Takerinfo['trade_funds'], 'hold_amount': Takerinfo['hold_amount']})
            # summary
            _amount = float(Takerinfo['trade_funds']) / 10 ** self.currencys[Takerinfo['qcc']]['digit']
            summary_data.append({'wallet_id': Takerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['qcc'], 'inout': WALLET_DETAIL_INOUT['OUT'], 'dsp_currency_price': None})
            # FIAT 거래대금
            trade_amount_data.append({'wallet_id': Takerinfo['wallet_id'], 'trade_amount_fiat': Takerinfo['trade_funds_converted_value'][FIAT], 'trade_time': Takerinfo['trade_time']})

            # Taker Base In
            # balance
            balance_data.append({'flag': 'IN', 'wallet_id': Takerinfo['wallet_id'], 'currency_code': Takerinfo['bcc'], 'amount': Takerinfo['trade_size'] - Takerinfo['fee']['IN']})
            # summary
            _trade_size = Takerinfo['trade_size'] - Takerinfo['fee']['IN']
            _amount = float(_trade_size) / 10 ** self.currencys[Takerinfo['bcc']]['digit']
            _dsp_currency_price = {k: float((v * 10 ** self.currencys[Takerinfo['bcc']]['digit']) / _trade_size) / 10 ** self.currencys[k]['digit'] for (k, v) in
                                   Takerinfo['trade_size_converted_value'].items()}
            summary_data.append(
                {'wallet_id': Takerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['bcc'], 'inout': WALLET_DETAIL_INOUT['IN'], 'dsp_currency_price': _dsp_currency_price})

            # Maker Quote In
            for Makerinfo in Makerinfos:
                # balance
                balance_data.append({'flag': 'IN', 'wallet_id': Makerinfo['wallet_id'], 'currency_code': Takerinfo['qcc'], 'amount': Makerinfo['trade_funds'] - Makerinfo['fee']['IN']})
                # summary
                if Makerinfo['trade_funds'] == 0:
                   Makerinfo['trade_funds'] = 1   
                print("makerinfo =",Makerinfo['trade_funds'])
                print("makerinfo fee = ",Makerinfo['fee']['IN'])
                _trade_funds = Makerinfo['trade_funds'] - Makerinfo['fee']['IN']
                forml=self.currencys[Takerinfo['qcc']]['digit']
                print("checking the formula of dsp currency price",forml)
                print("checking the value of trade fund for line 1523 error",_trade_funds)
                _amount = float(_trade_funds) / 10 ** self.currencys[Takerinfo['qcc']]['digit']
                print("checking the value of amount line 1523 amount",_amount)

                _dsp_currency_price = {k: float((v * 10 ** self.currencys[Takerinfo['qcc']]['digit']) / _trade_funds) / 10 ** self.currencys[k]['digit'] for (k, v) in
                                       Makerinfo['trade_funds_converted_value'].items()}
                summary_data.append(
                    {'wallet_id': Makerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['qcc'], 'inout': WALLET_DETAIL_INOUT['IN'], 'dsp_currency_price': _dsp_currency_price})

            # Maker Base Out
            for Makerinfo in Makerinfos:
                # balance & hold
                balance_data.append({'flag': 'OUT', 'wallet_id': Makerinfo['wallet_id'], 'currency_code': Takerinfo['bcc'], 'amount': Makerinfo['trade_size'], 'hold_amount': Makerinfo['hold_amount']})
                # summary
                _amount = float(Makerinfo['trade_size']) / 10 ** self.currencys[Takerinfo['bcc']]['digit']
                summary_data.append({'wallet_id': Makerinfo['wallet_id'], 'amount': _amount, 'amount_currency_code': Takerinfo['bcc'], 'inout': WALLET_DETAIL_INOUT['OUT'], 'dsp_currency_price': None})
                # FIAT 거래대금
                trade_amount_data.append({'wallet_id': Makerinfo['wallet_id'], 'trade_amount_fiat': Makerinfo['trade_size_converted_value'][FIAT], 'trade_time': Makerinfo['trade_time']})

        # 실행하는 부분
        # balance & hold
        for value in sorted(balance_data, key=operator.itemgetter('wallet_id', 'currency_code')):
            if value['flag'] == 'IN':
                self._exchanges_set_balance_in(wallet_id=value['wallet_id'], currency_code=value['currency_code'], amount=value['amount'])  # taker in
            else:  # OUT
                self._exchanges_set_balance_out(wallet_id=value['wallet_id'], currency_code=value['currency_code'], amount=value['amount'], hold_amount=value['hold_amount'])
        # currency summary
        #for value in sorted(summary_data, key=operator.itemgetter('wallet_id', 'amount_currency_code')):
        #    self._execute_summary_averageprice(wallet_id=value['wallet_id'], amount=value['amount'], amount_currency_code=value['amount_currency_code'], inout=value['inout'],
        #                                       dsp_currency_price=value['dsp_currency_price'])

        # 시간당 누적 거래 대금
        for value in sorted(trade_amount_data, key=operator.itemgetter('wallet_id')):
            self.SetTradeAmountHour(wallet_id=value['wallet_id'], amount=value['trade_amount_fiat'], trade_time=value['trade_time'])

        self.con.commit()
        return