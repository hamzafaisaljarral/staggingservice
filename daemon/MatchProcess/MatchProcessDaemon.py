# -*- encoding:utf8 -*-
import os, sys, time, multiprocessing, optparse, uuid, traceback, json, Queue

import redis
from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.function import *
from lib.define import *
from lib.exchange import exchange, OrderConversion, exchangeConfigParser

### CONFIG PARSERING ###
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
    'PROCESSDB': [
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
    'MATCHINGCACHE': [
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


class STAT:
    con = None

    @classmethod
    def send(cls, name):
        if cls.con is None:
             cls.con = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'],db=0)
            #cls.con = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'], password=CONFIG.REQUESTDB['password'], db=0)
        msg = json.dumps({'name': name, 'time': int(time.time())})
        cls.con.publish('STAT', msg)


class MatchingProcessor(exchange):
    def __init__(self, log, RUN_PRODUCT, currencys, products, StopQueue, OrderbookQueue, ResultQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.currencys = currencys
        self.products = products
        self.StopQueue = StopQueue
        self.OrderbookQueue = OrderbookQueue
        self.ResultQueue = ResultQueue

        self.processdb_init()
        self.matchingcache_init()
        self.requestdb_init()

        while True:
            try:
                Recv = self.processdb.brpop(self.PROCESSDBNAME['PROCESS_A'].format(product_code=self.RUN_PRODUCT), timeout=1)
                print(Recv)
                if Recv:
                    _RecvName, _RecvMsg = Recv
                    self.trx_id, _RecvMsg = eval(_RecvMsg)
                    self.log.w('purple', 'RECV', self.trx_id, _RecvName, _RecvMsg)
                    self.run(_RecvName=_RecvName, _RecvMsg=_RecvMsg)
                    STAT.send('MatchProcess')

                pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            except Exception as err:
                self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                # exit(1)

    def run(self, _RecvName, _RecvMsg):
        RecvMsg = json.loads(_RecvMsg)
        print("inside run",RecvMsg)

        RecvMsg['open_time'] = DATETIME_UNIXTIMESTAMP()
        if RecvMsg['order_type'] == ORDER_TYPE['LIMIT'] and RecvMsg['policy'] in (POLICY['GTC'], POLICY['GTT']):
            self.LimitMatchProcess(TakerOrder=RecvMsg)
        elif RecvMsg['order_type'] == ORDER_TYPE['MARKET'] and RecvMsg['side'] == SIDE['BUY']:
            print("inside BUY")
            self.MarketBuyMatchProcess(TakerOrder=RecvMsg)
        elif RecvMsg['order_type'] == ORDER_TYPE['MARKET'] and RecvMsg['side'] == SIDE['SELL']:
            self.MarketSellMatchProcess(TakerOrder=RecvMsg)
        elif RecvMsg['order_type'] == ORDER_TYPE['CANCEL']:
            self.CancelProcess(Cancelinfo=RecvMsg)
        # elif RecvMsg['order_type'] == ORDER_TYPE['MODIFY']:
        #     self.ModifyProcess(Modifyinfo=RecvMsg)
        elif RecvMsg['order_type'] == ORDER_TYPE['STOP']:
            self.StopQueue.put(('STOP', RecvMsg))
        elif RecvMsg['order_type'] == ORDER_TYPE['STOPCANCEL']:
            self.StopQueue.put(('STOPCANCEL', RecvMsg))
        else:
            raise ValueError('UNKNOWN')

    def MarketBuyMatchProcess(self, TakerOrder):
        print("inside BUY MATCH")
        result = dict()
        product = TakerOrder['product']
        Matchs = list()
        w_index = 0
        while True:
            _sequence = self.matchingcache.zrange(name=self.MATCHINGCACHENAME['OPEN:SELL'].format(product_code=product), start=w_index, end=w_index)
            if _sequence != []:
                w_index += 1
                sequence = int(_sequence[0])
                order_id = self.matchingcache.hget(name=self.MATCHINGCACHENAME['OPEN:SEQ'].format(product_code=product), key=sequence)
                MakerOrder = json.loads(self.matchingcache.hget(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=product), order_id))

                taker_size = (TakerOrder['remaining_funds'] * TakerOrder['bcu']) / MakerOrder['price']
                taker_size = taker_size - (taker_size % TakerOrder['min_size_unit'])
                if taker_size <= 0:
                    break
                trade_size = min((taker_size, MakerOrder['remaining_size']))
                trade_funds = (trade_size * MakerOrder['price']) / TakerOrder['bcu']
                if trade_funds <= 0:
                    break

                TakerOrder['remaining_funds'] -= trade_funds
                MakerOrder['remaining_size'] -= trade_size
                Matchs.append((TakerOrder.copy(), MakerOrder.copy(), trade_size, trade_funds))

                if MakerOrder['remaining_size'] > 0:
                    break
            else:
                break
        if Matchs:
            MatchResults = list()
            for TakerOrder, MakerOrder, trade_size, trade_funds in Matchs:
                self.StopQueue.put(('TRIGGER', {'trade_price': MakerOrder['price'], 'trx_id': self.trx_id}))
                Tradeinfo = self.create_Tradeinfo(product_code=product, TakerOrder=TakerOrder, MakerOrder=MakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                TakerOrder['match_side'] = 'taker'
                MakerOrder['match_side'] = 'maker'

                taker_fee_std = trade_size
                maker_fee_std = trade_funds
                taker_currency_digit = self.products[product]['bcd']
                maker_currency_digit = self.products[product]['qcd']

                # Taker 평단
                taker_standard_price, taker_accumulate_size, taker_accumulate_funds = self.get_standard_price(Orderinfo=TakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                TakerOrder['open_price'] = taker_standard_price
                TakerOrder['accumulate_size'] = taker_accumulate_size
                TakerOrder['accumulate_funds'] = taker_accumulate_funds
                _taker_fee = (taker_fee_std * TakerOrder['tfr']) / 10 ** (6 - R_DIGIT)
                taker_fee = int(round(_taker_fee, -R_DIGIT)) / (10 ** R_DIGIT)
                TakerOrder['accumulate_fee'] = self.get_accumulate_fee(Orderinfo=TakerOrder, fee=taker_fee, fee_digit=taker_currency_digit)
                TakerOrder['fee'] = taker_fee

                # Maker 평단
                maker_standard_price, maker_accumulate_size, maker_accumulate_funds = self.get_standard_price(Orderinfo=MakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                MakerOrder['open_price'] = maker_standard_price
                MakerOrder['accumulate_size'] = maker_accumulate_size
                MakerOrder['accumulate_funds'] = maker_accumulate_funds
                _maker_fee = (maker_fee_std * MakerOrder['mfr']) / 10 ** (6 - R_DIGIT)
                maker_fee = int(round(_maker_fee, -R_DIGIT)) / (10 ** R_DIGIT)
                MakerOrder['accumulate_fee'] = self.get_accumulate_fee(Orderinfo=MakerOrder, fee=maker_fee, fee_digit=maker_currency_digit)
                MakerOrder['fee'] = maker_fee
                MakerOrder['f_sequence'] = TakerOrder['sequence']

                self.OrderbookQueue.put({'flag': '-', 'side': MakerOrder['side'], 'price': MakerOrder['price'], 'size': Tradeinfo['trade_size'], 'product': product, 'trx_id': self.trx_id})
                # Maker
                if MakerOrder['remaining_size'] <= 0:  # filled
                    self.del_orderinfo(Orderinfo=MakerOrder)
                    self.del_accumulate_values(Orderinfo=MakerOrder)
                    MakerOrder['done_time'] = Tradeinfo['trade_time']
                else:  # remaining
                    self.set_orderinfo(Orderinfo=MakerOrder)
                MatchResults.append({'TakerOrder': TakerOrder.copy(), 'MakerOrder': MakerOrder.copy(), 'Tradeinfo': Tradeinfo.copy()})
            if MatchResults:
                result['Match'] = MatchResults

            # Taker
            self.del_accumulate_values(Orderinfo=TakerOrder)
            TakerOrder['done_time'] = Tradeinfo['trade_time']
            result['Done'] = TakerOrder

            _result = json.dumps(result)
            self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
            self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _result)
            return
        else:
            # self.del_accumulate_values(Orderinfo=TakerOrder)
            if w_index == 0:
                result['Fail'] = {'response_code': 'NOT_ACCUMULATE_SIZE', 'data': TakerOrder}
                _result = json.dumps(result)
                self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
                self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
                return
            else:
                result['Fail'] = {'response_code': 'NOT_ALLOW_ORDER', 'data': TakerOrder}
                _result = json.dumps(result)
                self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
                self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
                return

    def MarketSellMatchProcess(self, TakerOrder):
        result = dict()
        product = TakerOrder['product']
        Matchs = list()
        w_index = 0
        while True:
            _sequence = self.matchingcache.zrange(name=self.MATCHINGCACHENAME['OPEN:BUY'].format(product_code=product), start=w_index, end=w_index)
            if _sequence != []:
                w_index += 1
                sequence = int(_sequence[0])
                order_id = self.matchingcache.hget(name=self.MATCHINGCACHENAME['OPEN:SEQ'].format(product_code=TakerOrder['product']), key=sequence)
                _MakerOrder = self.matchingcache.hget(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=TakerOrder['product']), order_id)
                MakerOrder = json.loads(_MakerOrder)

                ## 2019-03-14; BTCBXB에서 최소거래사이즈(0.001)로 주문 날렸을때, 아래 조건에 의해 NOT_ALLOW_ORDER가 발생함
                ## 조건식 가장 끝의 qcu는 계산착오에 의한 문제로 판단하여, quote currency의 최소단위를 의미하는 1로 수정
                # if (MakerOrder['price'] * TakerOrder['remaining_size']) / TakerOrder['bcu'] < TakerOrder['qcu']:
                #     break
                print(MakerOrder['price'])
                print(TakerOrder['remaining_size'])
                print(TakerOrder['bcu'])
                
                if (MakerOrder['price'] * TakerOrder['remaining_size']) / TakerOrder['bcu'] < 1:
                    break

                trade_size = min((TakerOrder['remaining_size'], MakerOrder['remaining_size']))
                trade_funds = (trade_size * MakerOrder['price']) / MakerOrder['bcu']
                MakerOrder['remaining_size'] -= trade_size
                TakerOrder['remaining_size'] -= trade_size
                Matchs.append((TakerOrder.copy(), MakerOrder.copy(), trade_size, trade_funds))
                if TakerOrder['remaining_size'] <= 0:
                    break
            else:
                break
        if Matchs:
            MatchResults = list()
            for TakerOrder, MakerOrder, trade_size, trade_funds in Matchs:
                self.StopQueue.put(('TRIGGER', {'trade_price': MakerOrder['price'], 'trx_id': self.trx_id}))
                Tradeinfo = self.create_Tradeinfo(product_code=product, TakerOrder=TakerOrder, MakerOrder=MakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                MakerOrder['match_side'] = 'maker'
                TakerOrder['match_side'] = 'taker'

                maker_fee_std = trade_size
                taker_fee_std = trade_funds
                maker_currency_digit = self.products[product]['bcd']
                taker_currency_digit = self.products[product]['qcd']

                # Taker 평단
                taker_standard_price, taker_accumulate_size, taker_accumulate_funds = self.get_standard_price(Orderinfo=TakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                TakerOrder['open_price'] = taker_standard_price
                TakerOrder['accumulate_size'] = taker_accumulate_size
                TakerOrder['accumulate_funds'] = taker_accumulate_funds
                _taker_fee = (taker_fee_std * TakerOrder['tfr']) / 10 ** (6 - R_DIGIT)
                taker_fee = int(round(_taker_fee, -R_DIGIT)) / (10 ** R_DIGIT)
                TakerOrder['accumulate_fee'] = self.get_accumulate_fee(Orderinfo=TakerOrder, fee=taker_fee, fee_digit=taker_currency_digit)
                TakerOrder['fee'] = taker_fee

                # Maker 평단
                maker_standard_price, maker_accumulate_size, maker_accumulate_funds = self.get_standard_price(Orderinfo=MakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                MakerOrder['open_price'] = maker_standard_price
                MakerOrder['accumulate_size'] = maker_accumulate_size
                MakerOrder['accumulate_funds'] = maker_accumulate_funds
                _maker_fee = (maker_fee_std * MakerOrder['mfr']) / 10 ** (6 - R_DIGIT)
                maker_fee = int(round(_maker_fee, -R_DIGIT)) / (10 ** R_DIGIT)
                MakerOrder['accumulate_fee'] = self.get_accumulate_fee(Orderinfo=MakerOrder, fee=maker_fee, fee_digit=maker_currency_digit)
                MakerOrder['fee'] = maker_fee

                self.OrderbookQueue.put({'flag': '-', 'side': MakerOrder['side'], 'price': MakerOrder['price'], 'size': Tradeinfo['trade_size'], 'product': product, 'trx_id': self.trx_id})
                # Maker
                if MakerOrder['remaining_size'] <= 0:  # filled
                    self.del_orderinfo(Orderinfo=MakerOrder)
                    self.del_accumulate_values(Orderinfo=MakerOrder)
                    MakerOrder['done_time'] = Tradeinfo['trade_time']
                else:  # remaining
                    self.set_orderinfo(Orderinfo=MakerOrder)
                MatchResults.append({'TakerOrder': TakerOrder.copy(), 'MakerOrder': MakerOrder.copy(), 'Tradeinfo': Tradeinfo.copy()})
            if MatchResults:
                result['Match'] = MatchResults
            # Taker
            self.del_accumulate_values(Orderinfo=TakerOrder)
            TakerOrder['done_time'] = Tradeinfo['trade_time']
            result['Done'] = TakerOrder
        else:
            if w_index == 0:
                result['Fail'] = {'response_code': 'NOT_ACCUMULATE_SIZE', 'data': TakerOrder}
                _result = json.dumps(result)
                self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
                self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
                return
            else:
                result['Fail'] = {'response_code': 'NOT_ALLOW_ORDER', 'data': TakerOrder}
                _result = json.dumps(result)
                self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
                self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
                return
        _result = json.dumps(result)
        self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
        self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _result)
        return

    def LimitMatchProcess(self, TakerOrder):
        result = dict()
        product = TakerOrder['product']
        taker_filled, Matchs = self.GetMatching(TakerOrder=TakerOrder)
        if Matchs:
            MatchResults = list()
            for TakerOrder, MakerOrder, trade_size, trade_funds in Matchs:
                self.StopQueue.put(('TRIGGER', {'trade_price': MakerOrder['price'], 'trx_id': self.trx_id}))
                Tradeinfo = self.create_Tradeinfo(product_code=product, TakerOrder=TakerOrder, MakerOrder=MakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                MakerOrder['match_side'] = 'maker'
                TakerOrder['match_side'] = 'taker'

                if TakerOrder['side'] == SIDE['BUY']:
                    taker_fee_std = trade_size
                    maker_fee_std = trade_funds
                    taker_currency_digit = self.products[product]['bcd']
                    maker_currency_digit = self.products[product]['qcd']
                else:  # MakerOrder['side'] == SIDE['BUY']:
                    maker_fee_std = trade_size
                    taker_fee_std = trade_funds
                    maker_currency_digit = self.products[product]['bcd']
                    taker_currency_digit = self.products[product]['qcd']

                # Taker 평단
                taker_standard_price, taker_accumulate_size, taker_accumulate_funds = self.get_standard_price(Orderinfo=TakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                TakerOrder['open_price'] = taker_standard_price
                TakerOrder['accumulate_size'] = taker_accumulate_size
                TakerOrder['accumulate_funds'] = taker_accumulate_funds
                _taker_fee = (taker_fee_std * TakerOrder['tfr']) / 10 ** (6 - R_DIGIT)
                taker_fee = int(round(_taker_fee, -R_DIGIT)) / (10 ** R_DIGIT)
                TakerOrder['accumulate_fee'] = self.get_accumulate_fee(Orderinfo=TakerOrder, fee=taker_fee, fee_digit=taker_currency_digit)
                TakerOrder['fee'] = taker_fee

                # Maker 평단
                maker_standard_price, maker_accumulate_size, maker_accumulate_funds = self.get_standard_price(Orderinfo=MakerOrder, trade_size=trade_size, trade_funds=trade_funds)
                MakerOrder['open_price'] = maker_standard_price
                MakerOrder['accumulate_size'] = maker_accumulate_size
                MakerOrder['accumulate_funds'] = maker_accumulate_funds
                _maker_fee = (maker_fee_std * MakerOrder['mfr']) / 10 ** (6 - R_DIGIT)
                maker_fee = int(round(_maker_fee, -R_DIGIT)) / (10 ** R_DIGIT)
                MakerOrder['accumulate_fee'] = self.get_accumulate_fee(Orderinfo=MakerOrder, fee=maker_fee, fee_digit=maker_currency_digit)
                MakerOrder['fee'] = maker_fee
                MakerOrder['f_sequence'] = TakerOrder['sequence']

                self.OrderbookQueue.put({'flag': '-', 'side': MakerOrder['side'], 'price': MakerOrder['price'], 'size': Tradeinfo['trade_size'], 'product': product, 'trx_id': self.trx_id})
                # Maker
                if MakerOrder['remaining_size'] <= 0:  # filled
                    self.del_orderinfo(Orderinfo=MakerOrder)
                    self.del_accumulate_values(Orderinfo=MakerOrder)
                    MakerOrder['done_time'] = Tradeinfo['trade_time']
                else:  # remaining
                    self.set_orderinfo(Orderinfo=MakerOrder)
                MatchResults.append({'TakerOrder': TakerOrder.copy(), 'MakerOrder': MakerOrder.copy(), 'Tradeinfo': Tradeinfo.copy()})
            if MatchResults:
                result['Match'] = MatchResults
            # Taker
            if TakerOrder['remaining_size'] <= 0:
                self.del_accumulate_values(Orderinfo=TakerOrder)
                TakerOrder['done_time'] = Tradeinfo['trade_time']
                result['Done'] = TakerOrder
            else:
                self.OrderbookQueue.put({'flag': '+', 'side': TakerOrder['side'], 'price': TakerOrder['price'], 'size': TakerOrder['remaining_size'], 'product': product, 'trx_id': self.trx_id})
                self.set_orderinfo(Orderinfo=TakerOrder)
                result['Open'] = TakerOrder
        else:
            self.OrderbookQueue.put({'flag': '+', 'side': TakerOrder['side'], 'price': TakerOrder['price'], 'size': TakerOrder['remaining_size'], 'product': product, 'trx_id': self.trx_id})
            self.set_orderinfo(Orderinfo=TakerOrder)
            result['Open'] = TakerOrder

        if 'expire_time' in TakerOrder:
            self.set_order_expire(product_code=product, order_id=TakerOrder['order_id'], expire_time=TakerOrder['expire_time'])
        _result = json.dumps(result)
        self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
        self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _result)
        return

    def CancelProcess(self, Cancelinfo):
        result = dict()
        product = Cancelinfo['product']

        _Orderinfo = self.matchingcache.hget(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=self.RUN_PRODUCT), Cancelinfo['order_id'])
        if _Orderinfo is None:
            if 'expire_time' in Cancelinfo:
                return
            else:
                result['Fail'] = {'response_code': 'NOT_FOUND_ORDER_ID', 'data': Cancelinfo}
                _result = json.dumps(result)
                self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
                self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
                return

        Orderinfo = json.loads(_Orderinfo)
        Orderinfo['match_side'] = 'cancel'
        if Cancelinfo['size'] <= 0:
            Cancelinfo['cancel_size'] = Orderinfo['remaining_size']
        else:
            Cancelinfo['cancel_size'] = min((Cancelinfo['size'], Orderinfo['remaining_size']))

        Cancelinfo['cancel_time'] = DATETIME_UNIXTIMESTAMP()
        Orderinfo['remaining_size'] -= Cancelinfo['cancel_size']
        Orderinfo['accumulate_cancelsize'] += Cancelinfo['cancel_size']
        Orderinfo['f_sequence'] = Cancelinfo['sequence']

        self.OrderbookQueue.put({'flag': '-', 'side': Orderinfo['side'], 'price': Orderinfo['price'], 'size': Cancelinfo['cancel_size'], 'product': product, 'trx_id': self.trx_id})
        if Orderinfo['remaining_size'] <= 0:  # done
            self.del_orderinfo(Orderinfo=Orderinfo)
            self.del_accumulate_values(Orderinfo=Orderinfo)
            Orderinfo['done_time'] = Cancelinfo['cancel_time']
        else:  # update
            self.set_orderinfo(Orderinfo=Orderinfo)

        result['MakerCancel'] = {'Orderinfo': Orderinfo, 'Cancelinfo': Cancelinfo}
        _result = json.dumps(result)
        self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
        self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _result)
        return

    def GetMatching(self, TakerOrder):
        ''' Limit(GTC, IOC, FOK) Buy, Sell 모두 사용 가능'''
        if TakerOrder['side'] == SIDE['BUY']:
            MatchOrderData = {'name': self.MATCHINGCACHENAME['OPEN:SELL'], 'operator': lambda x, y: x >= y}
        elif TakerOrder['side'] == SIDE['SELL']:
            MatchOrderData = {'name': self.MATCHINGCACHENAME['OPEN:BUY'], 'operator': lambda x, y: x <= y}
        taker_filled = False
        Matchs = list()
        w_index = 0
        while True:
            _sequence = self.matchingcache.zrange(name=MatchOrderData['name'].format(product_code=TakerOrder['product']), start=w_index, end=w_index)
            if _sequence != []:
                w_index += 1
                sequence = int(_sequence[0])
                order_id = self.matchingcache.hget(name=self.MATCHINGCACHENAME['OPEN:SEQ'].format(product_code=TakerOrder['product']), key=sequence)
                _MakerOrder = self.matchingcache.hget(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=TakerOrder['product']), order_id)
                MakerOrder = json.loads(_MakerOrder)
                if MatchOrderData['operator'](TakerOrder['price'], MakerOrder['price']):
                    trade_size = min((TakerOrder['remaining_size'], MakerOrder['remaining_size']))
                    trade_funds = (trade_size * MakerOrder['price']) / MakerOrder['bcu']
                    MakerOrder['remaining_size'] -= trade_size
                    TakerOrder['remaining_size'] -= trade_size
                    Matchs.append((TakerOrder.copy(), MakerOrder.copy(), trade_size, trade_funds))
                    if TakerOrder['remaining_size'] <= 0:
                        taker_filled = True
                        break
                else:
                    break
            else:
                break
        return taker_filled, Matchs

    def set_orderinfo(self, Orderinfo):
        _sequence = str(Orderinfo['sequence']).zfill(20)
        pipeline = self.matchingcache.pipeline(transaction=True)
        pipeline.hset(self.MATCHINGCACHENAME['OPEN:SEQ'].format(product_code=Orderinfo['product']), Orderinfo['sequence'], Orderinfo['order_id'])
        pipeline.hset(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=Orderinfo['product']), Orderinfo['order_id'], json.dumps(Orderinfo))
        if Orderinfo['side'] == SIDE['BUY']:
            pipeline.zadd(self.MATCHINGCACHENAME['OPEN:BUY'].format(product_code=Orderinfo['product']), _sequence, -Orderinfo['price'])
        else:
            pipeline.zadd(self.MATCHINGCACHENAME['OPEN:SELL'].format(product_code=Orderinfo['product']), _sequence, Orderinfo['price'])
        pipeline.execute()

    def del_orderinfo(self, Orderinfo):
        _sequence = str(Orderinfo['sequence']).zfill(20)
        pipeline = self.matchingcache.pipeline(transaction=True)
        pipeline.hdel(self.MATCHINGCACHENAME['OPEN:SEQ'].format(product_code=Orderinfo['product']), Orderinfo['sequence'])  # key: sequence, value: order_id
        pipeline.hdel(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])  # key: order_id, value: Orderinfo
        pipeline.zrem(self.MATCHINGCACHENAME['OPEN:BUY'].format(product_code=Orderinfo['product']), _sequence)  # key: sequence, value: price
        pipeline.zrem(self.MATCHINGCACHENAME['OPEN:SELL'].format(product_code=Orderinfo['product']), _sequence)  # key: sequence, value: price
        pipeline.execute()

    def del_accumulate_values(self, Orderinfo):
        pipeline = self.matchingcache.pipeline(transaction=True)
        pipeline.hdel(self.MATCHINGCACHENAME['ACCUMULATE:SIZE'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])
        pipeline.hdel(self.MATCHINGCACHENAME['ACCUMULATE:FUNDS'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])
        pipeline.hdel(self.MATCHINGCACHENAME['ACCUMULATE:FEE'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])
        pipeline.execute()

    def get_standard_price(self, Orderinfo, trade_size, trade_funds):
        accumulate_size = self.get_accumulate_size(Orderinfo=Orderinfo, size=trade_size)
        accumulate_funds = self.get_accumulate_funds(Orderinfo=Orderinfo, funds=trade_funds)
        standard_price = (accumulate_funds * 10 ** Orderinfo['bcd']) / accumulate_size
        return standard_price, accumulate_size, accumulate_funds

    def get_accumulate_size(self, Orderinfo, size):
        _accumulate_size = self.matchingcache.hget(self.MATCHINGCACHENAME['ACCUMULATE:SIZE'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])
        if _accumulate_size is not None:
            accumulate_size = int(float(_accumulate_size)) + size
        else:
            accumulate_size = size
        self.matchingcache.hset(self.MATCHINGCACHENAME['ACCUMULATE:SIZE'].format(product_code=Orderinfo['product']), Orderinfo['order_id'], accumulate_size)
        return accumulate_size

    def get_accumulate_funds(self, Orderinfo, funds):
        _accumulate_funds = self.matchingcache.hget(self.MATCHINGCACHENAME['ACCUMULATE:FUNDS'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])
        if _accumulate_funds is not None:
            accumulate_funds = int(float(_accumulate_funds)) + funds
        else:
            accumulate_funds = funds
        self.matchingcache.hset(self.MATCHINGCACHENAME['ACCUMULATE:FUNDS'].format(product_code=Orderinfo['product']), Orderinfo['order_id'], accumulate_funds)
        return accumulate_funds

    def get_accumulate_fee(self, Orderinfo, fee, fee_digit):
        _accumulate_fee = self.matchingcache.hget(self.MATCHINGCACHENAME['ACCUMULATE:FEE'].format(product_code=Orderinfo['product']), Orderinfo['order_id'])
        if _accumulate_fee is not None:
            accumulate_fee = int(float(_accumulate_fee)) + fee
        else:
            accumulate_fee = fee
        self.matchingcache.hset(self.MATCHINGCACHENAME['ACCUMULATE:FEE'].format(product_code=Orderinfo['product']), Orderinfo['order_id'], accumulate_fee)
        return accumulate_fee

    def set_order_expire(self, product_code, order_id, expire_time):
        self.matchingcache.zadd(self.MATCHINGCACHENAME['OPEN:EXPIRE'].format(product_code=product_code), order_id, expire_time)

    def create_Tradeinfo(self, product_code, TakerOrder, MakerOrder, trade_size, trade_funds):
        Tradeinfo = {'trade_id': str(uuid.uuid4()), 'trade_price': MakerOrder['price'], 'trade_size': trade_size, 'trade_funds': trade_funds, 'trade_time': DATETIME_UNIXTIMESTAMP(), 'product': TakerOrder['product'],
                     'bcc': TakerOrder['bcc'], 'bcd': TakerOrder['bcd'], 'bcu': TakerOrder['bcu'], 'qcc': TakerOrder['qcc'], 'qcd': TakerOrder['qcd'], 'qcu': TakerOrder['qcu'], 'taker_side': TakerOrder['side'],
                     'sequence': self.requestdb.incr(self.REQUESTDBNAME['SEQUENCE:TRADEHISTORY'].format(product_code=product_code))}
        return Tradeinfo
    # def ModifyProcess(self, Modifyinfo):
    #     result = dict()
    #     product = Modifyinfo['product']
    #     _Orderinfo = self.matchingcache.hget(self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=product), Modifyinfo['order_id'])
    #     if _Orderinfo == None: # not found order_id
    #         result['Fail'] = {'Modifyinfo': Modifyinfo, 'response_code': 'NOT_FOUND_ORDER_ID'}
    #         _result = json.dumps(result)
    #         self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
    #         self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
    #         return
    #     Orderinfo = json.loads(_Orderinfo)
    #     if Orderinfo['price'] == Modifyinfo['price']:
    #         Modifyinfo['old_price'] = Orderinfo['price']
    #         result['Modify'] = {'Orderinfo': Orderinfo, 'Modifyinfo': Modifyinfo}
    #     else:
    #         if Orderinfo['side'] == SIDE['BUY']: # ASK
    #             ASK = self.get_ask(product_code=product)
    #             if ASK == None or ASK > Modifyinfo['price']:
    #                 Modifyinfo['old_price'] = Orderinfo['price']
    #                 Orderinfo['price'] = Modifyinfo['price']
    #                 Orderinfo['f_sequence'] = Modifyinfo['sequence']
    #                 self.OrderbookQueue.put({'flag': '-', 'side': Orderinfo['side'], 'price': Modifyinfo['old_price'], 'size': Orderinfo['remaining_size'], 'product': product, 'trx_id':self.trx_id})
    #                 self.OrderbookQueue.put({'flag': '+', 'side': Orderinfo['side'], 'price': Orderinfo['price'], 'size': Orderinfo['remaining_size'], 'product': product, 'trx_id':self.trx_id})
    #                 self.set_orderinfo(Orderinfo=Orderinfo)
    #                 result['Modify'] = {'Orderinfo': Orderinfo, 'Modifyinfo': Modifyinfo}
    #             else:
    #                 result['Fail'] = {'Modifyinfo': Modifyinfo, 'response_code': 'NOT_ALLOW_PRICE'}
    #         else: # Orderinfo['side'] == SIDE['BUY']: # ASK
    #             BID = self.get_bid(product_code=product)
    #             if BID == None or BID < Modifyinfo['price']:
    #                 Modifyinfo['old_price'] = Orderinfo['price']
    #                 Orderinfo['price'] = Modifyinfo['price']
    #                 Orderinfo['f_sequence'] = Modifyinfo['sequence']
    #                 self.OrderbookQueue.put({'flag': '-', 'side': Orderinfo['side'], 'price': Modifyinfo['old_price'], 'size': Orderinfo['remaining_size'], 'product': product, 'trx_id':self.trx_id})
    #                 self.OrderbookQueue.put({'flag': '+', 'side': Orderinfo['side'], 'price': Orderinfo['price'], 'size': Orderinfo['remaining_size'], 'product': product, 'trx_id':self.trx_id})
    #                 self.set_orderinfo(Orderinfo=Orderinfo)
    #                 result['Modify'] = {'Orderinfo': Orderinfo, 'Modifyinfo': Modifyinfo}
    #             else:
    #                 result['Fail'] = {'Modifyinfo': Modifyinfo, 'response_code': 'NOT_ALLOW_PRICE'}
    #
    #
    #     _result = json.dumps(result)
    #     self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
    #     if result.has_key('Fail'):
    #         self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
    #     else:
    #         self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _result)
    #
    #     return


class StopProcessor(exchange):
    def __init__(self, log, RUN_PRODUCT, currencys, products, StopQueue, ResultQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.currencys = currencys
        self.products = products
        self.StopQueue = StopQueue
        self.ResultQueue = ResultQueue

        self.matchingcache_init()
        self.processdb_init()
        while True:
            try:
                msg = self.StopQueue.get(timeout=1)
            except Queue.Empty:
                msg = None
                pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            if msg:
                kind, data = msg
                if kind == 'STOP':
                    self.StopRegistProcess(Stopinfo=data)
                elif kind == 'STOPCANCEL':
                    self.StopCancelProcess(Cancelinfo=data)
                elif kind == 'TRIGGER':
                    self.StopTriggerProcess(data=data)
                else:
                    raise ValueError('unknown')

    def StopRegistProcess(self, Stopinfo):
        self.trx_id = Stopinfo['Orderinfo']['trx_id']
        result = dict()
        NOT_ACCUMULATE_SIZE = False
        NOT_ALLOW_STOP_PRICE = False
        # stop_price 검증
        if Stopinfo['side'] == SIDE['BUY']:
            ASK = self.get_ask(product_code=Stopinfo['product'])
            print(ASK)
            if ASK is None:
                NOT_ACCUMULATE_SIZE = True
            elif not (Stopinfo['stop_price'] >= ASK):
                NOT_ALLOW_STOP_PRICE = True
            else:
                pass  # STOP 등록

        elif Stopinfo['side'] == SIDE['SELL']:
            BID = self.get_bid(product_code=Stopinfo['product'])
            if BID is None:
                NOT_ACCUMULATE_SIZE = True
            elif not (Stopinfo['stop_price'] <= BID):
                NOT_ALLOW_STOP_PRICE = True
            else:
                pass  # STOP 등록
        else:
            raise ValueError('UNKNOWN')

        if NOT_ACCUMULATE_SIZE:
            _result = json.dumps({'Fail': {'response_code': 'NOT_ACCUMULATE_SIZE', 'data': Stopinfo}})
            self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
            self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
            return
        elif NOT_ALLOW_STOP_PRICE:
            _result = json.dumps({'Fail': {'response_code': 'NOT_ALLOW_STOP_PRICE', 'data': Stopinfo}})
            self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
            self.log.w('yellow', 'FAIL', self.trx_id, 'PROCESS_B', _result)
            return
        else:
            pipeline = self.matchingcache.pipeline(transaction=True)
            pipeline.hset(self.MATCHINGCACHENAME['STOP:INFO'].format(product_code=Stopinfo['product']), Stopinfo['Orderinfo']['order_id'], json.dumps(Stopinfo['Orderinfo']))
            pipeline.hset(self.MATCHINGCACHENAME['STOP:SEQ'].format(product_code=Stopinfo['product']), Stopinfo['Orderinfo']['sequence'], Stopinfo['Orderinfo']['order_id'])
            c_sequence = str(Stopinfo['Orderinfo']['sequence']).zfill(20)
            if Stopinfo['side'] == SIDE['BUY']:
                pipeline.zadd(self.MATCHINGCACHENAME['STOP:BUY'].format(product_code=Stopinfo['product']), c_sequence, Stopinfo['stop_price'])
            else:  # Orderinfo['side'] == SIDE['SELL']
                pipeline.zadd(self.MATCHINGCACHENAME['STOP:SELL'].format(product_code=Stopinfo['product']), c_sequence, -Stopinfo['stop_price'])
            pipeline.execute()
            result['StopRegist'] = Stopinfo['Orderinfo']

            _result = json.dumps(result)
            self.ResultQueue.put({'type': 'process_b', '_result': _result, 'trx_id': self.trx_id})
            self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _result)
            return

    def StopCancelProcess(self, Cancelinfo):
        result = dict()
        self.trx_id = Cancelinfo['trx_id']
        _Orderinfo = self.matchingcache.hget(self.MATCHINGCACHENAME['STOP:INFO'].format(product_code=self.RUN_PRODUCT), Cancelinfo['order_id'])
        if _Orderinfo is None:
            result['Fail'] = {'response_code': 'NOT_FOUND_ORDER_ID', 'data': Cancelinfo}
            self.ResultQueue.put({'type': 'process_b', '_result': json.dumps(result), 'trx_id': self.trx_id})
            self.log.w('yellow', 'FAIL', self.trx_id, Cancelinfo)
        else:
            result['StopCancel'] = {'Cancelinfo': Cancelinfo, 'Orderinfo': json.loads(_Orderinfo)}
            self.ResultQueue.put({'type': 'process_b', '_result': json.dumps(result), 'trx_id': self.trx_id})
            self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_B', _Orderinfo)
        _sequence = str(Cancelinfo['sequence']).zfill(20)
        pipeline = self.matchingcache.pipeline(transaction=True)
        pipeline.hdel(self.MATCHINGCACHENAME['STOP:INFO'].format(product_code=self.RUN_PRODUCT), Cancelinfo['order_id'])
        pipeline.hdel(self.MATCHINGCACHENAME['STOP:SEQ'].format(product_code=self.RUN_PRODUCT), Cancelinfo['sequence'])
        pipeline.zrem(self.MATCHINGCACHENAME['STOP:BUY'].format(product_code=self.RUN_PRODUCT), _sequence)
        pipeline.zrem(self.MATCHINGCACHENAME['STOP:SELL'].format(product_code=self.RUN_PRODUCT), _sequence)
        pipeline.execute()

    def StopTriggerProcess(self, data):
        self.trx_id = data['trx_id']
        trade_price = data['trade_price']
        StopOrders = list()
        StopOrders.extend(self.matchingcache.zrangebyscore(name=self.MATCHINGCACHENAME['STOP:BUY'].format(product_code=self.RUN_PRODUCT), min=0, max=trade_price, withscores=True))
        StopOrders.extend(self.matchingcache.zrangebyscore(name=self.MATCHINGCACHENAME['STOP:SELL'].format(product_code=self.RUN_PRODUCT), min=-float('inf'), max=-trade_price, withscores=True))
        if StopOrders:
            for _sequence, _stop_price in StopOrders:
                sequence = int(_sequence)
                # stop_price = abs(_stop_price)
                order_id = self.matchingcache.hget(self.MATCHINGCACHENAME['STOP:SEQ'].format(product_code=self.RUN_PRODUCT), sequence)
                _Orderinfo = self.matchingcache.hget(self.MATCHINGCACHENAME['STOP:INFO'].format(product_code=self.RUN_PRODUCT), order_id)
                if _Orderinfo is not None:
                    self.processdb.lpush(self.PROCESSDBNAME['PROCESS_A'].format(product_code=self.RUN_PRODUCT), str((self.trx_id, _Orderinfo)))
                    self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_A', _Orderinfo)
                pipeline = self.matchingcache.pipeline(transaction=True)
                pipeline.hdel(self.MATCHINGCACHENAME['STOP:INFO'].format(product_code=self.RUN_PRODUCT), order_id)
                pipeline.hdel(self.MATCHINGCACHENAME['STOP:SEQ'].format(product_code=self.RUN_PRODUCT), sequence)
                pipeline.zrem(self.MATCHINGCACHENAME['STOP:BUY'].format(product_code=self.RUN_PRODUCT), _sequence)
                pipeline.zrem(self.MATCHINGCACHENAME['STOP:SELL'].format(product_code=self.RUN_PRODUCT), _sequence)
                pipeline.execute()


class OrderbookProcessor(exchange):
    def __init__(self, log, RUN_PRODUCT, currencys, products, OrderbookQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.currencys = currencys
        self.products = products
        self.OrderbookQueue = OrderbookQueue

        self.requestdb_init()
        self.responsedb_init()

        while True:
            try:
                msg = self.OrderbookQueue.get(timeout=1)
            except Queue.Empty:
                msg = None
                pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            if msg:
                msg['price'] = float(msg['price'])  # key값이 일괄되게 적용되게 하기 위하여 float으로 casting
                if msg['flag'] == '-':
                    msg['size'] = -abs(msg['size'])
                sequence = self.requestdb.incr(self.REQUESTDBNAME['SEQUENCE:ORDERBOOK'].format(product_code=msg['product']))
                pipeline = self.responsedb.pipeline(transaction=True)
                pipeline.set(self.RESPONSEDBNAME['ORDERBOOK:SEQ'].format(product_code=self.RUN_PRODUCT), sequence)
                if msg['side'] == SIDE['BUY']:
                    _side = 'BID'
                    pipeline.zincrby(self.RESPONSEDBNAME['ORDERBOOK:BID'].format(product_code=self.RUN_PRODUCT), msg['price'], msg['size'])
                elif msg['side'] == SIDE['SELL']:
                    _side = 'ASK'
                    pipeline.zincrby(self.RESPONSEDBNAME['ORDERBOOK:ASK'].format(product_code=self.RUN_PRODUCT), msg['price'], msg['size'])
                else:  # SELL
                    raise ValueError('UNKNOWN')
                _, new_volume = pipeline.execute()

                if new_volume == 0 and msg['side'] == SIDE['BUY']:
                    self.responsedb.zrem(self.RESPONSEDBNAME['ORDERBOOK:BID'].format(product_code=self.RUN_PRODUCT), msg['price'])
                elif new_volume == 0 and msg['side'] == SIDE['SELL']:
                    self.responsedb.zrem(self.RESPONSEDBNAME['ORDERBOOK:ASK'].format(product_code=self.RUN_PRODUCT), msg['price'])

                data = json.dumps({'side': _side, 'price': str(msg['price'] / self.products[self.RUN_PRODUCT]['qcu']), 'volume': new_volume / self.products[self.RUN_PRODUCT]['bcu'], 'sequence': sequence, 'product': self.RUN_PRODUCT})
                self.responsedb.publish(self.RESPONSEDBNAME['ORDERBOOK:PUBLISH'].format(product_code=self.RUN_PRODUCT), data)
                logmsg = '{sequence} {side} {price} {flag}{size} {new_volume}'.format(sequence=sequence, side=_side, price=msg['price'], flag=msg['flag'], size=msg['size'], new_volume=new_volume)
                print("orderbook",logmsg)
                self.log.w('green', 'SUCC', msg['trx_id'], logmsg)


class ExpireTimerProcessor(exchange):
    def __init__(self, log, RUN_PRODUCT, currencys, products, ExpireTimerQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.currencys = currencys
        self.products = products
        self.ExpireTimerQueue = ExpireTimerQueue

        while True:
            pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
            if os.path.exists(pidkill):
                os.remove(pidkill)
                exit(0)

            self.ExpireTimerQueue.put(int(time.time()))
            time.sleep(1)


class ExpireProcessor(exchange):
    def __init__(self, log, RUN_PRODUCT, currencys, products, ExpireTimerQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.currencys = currencys
        self.products = products
        self.ExpireTimerQueue = ExpireTimerQueue

        self.matchingcache_init()
        self.requestdb_init()
        self.processdb_init()
        MATCHINGCACHENAME_OPEN_EXPIRE = self.MATCHINGCACHENAME['OPEN:EXPIRE'].format(product_code=self.RUN_PRODUCT)
        MATCHINGCACHENAME_OPEN_INFO = self.MATCHINGCACHENAME['OPEN:INFO'].format(product_code=self.RUN_PRODUCT)
        while True:
            try:
                msg = self.ExpireTimerQueue.get(timeout=1)
            except Queue.Empty:
                msg = None
                pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            if msg:
                currenct_time = msg
                expire_order_ids = self.matchingcache.zrangebyscore(name=MATCHINGCACHENAME_OPEN_EXPIRE, min=-float('inf'), max=currenct_time, start=0, num=-1, withscores=False)
                for order_id in expire_order_ids:
                    _Orderinfo = self.matchingcache.hget(MATCHINGCACHENAME_OPEN_INFO, order_id)
                    if _Orderinfo is not None:
                        Orderinfo = json.loads(_Orderinfo)
                        TempRecvMsg = self.create_expire_cancel_order(member_id=Orderinfo['member_id'], wallet_id=Orderinfo['wallet_id'], product_code=Orderinfo['product'], order_id=Orderinfo['order_id'],
                                                                      expire_time=Orderinfo['expire_time'])
                        conversion = OrderConversion(currencys=self.currencys,
                                                     products=self.products,
                                                     RecvMsg=TempRecvMsg)
                        self.processdb.lpush(self.PROCESSDBNAME['PROCESS_A'].format(product_code=self.RUN_PRODUCT), str((conversion.data['trx_id'], json.dumps(conversion.data))))
                    self.matchingcache.zrem(MATCHINGCACHENAME_OPEN_EXPIRE, order_id)

    def create_expire_cancel_order(self, member_id, wallet_id, product_code, order_id, expire_time):
        return {'sequence': self.requestdb.incr(self.REQUESTDBNAME['SEQUENCE:ORDERSEND']),
                'trx_id': str(uuid.uuid4()),
                'order_type': 'CANCEL',
                'member_id': member_id,
                'wallet_id': wallet_id,
                'product': product_code,
                'order_id': order_id,
                'size': 0,
                'expire_time': expire_time}


class ResultProcessor(exchange):
    def __init__(self, log, RUN_PRODUCT, currencys, products, ResultQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT
        self.currencys = currencys
        self.products = products
        self.ResultQueue = ResultQueue

        self.processdb_init()

        while True:
            try:
                try:
                    msg = self.ResultQueue.get(timeout=1)
                except Queue.Empty:
                    msg = None
                    pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                    if os.path.exists(pidkill):
                        os.remove(pidkill)
                        exit(0)

                if msg:
                    self.trx_id = msg['trx_id']
                    self.run(msg=msg)

            except Exception as err:
                self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                exit(1)

    def run(self, msg):
        if msg['type'] == 'process_b':
            self.processdb.lpush(self.PROCESSDBNAME['PROCESS_B'], str((msg['trx_id'], msg['_result'])))
        else:
            raise ValueError('UNKNOWN')


class Main(exchange):
    process_list = dict()

    def multiprocess_starter(self, name, target, kwargs):
        p = multiprocessing.Process(target=target, kwargs=kwargs)
        p.daemon = True
        p.start()
        self.process_list[name] = {'name': name, 'process': p, 'pid': p.pid}
        self.log.w('gray', 'INIT', 'Running', name, p.pid)

    def __init__(self, log, RUN_PRODUCT):
        self.CONFIG = CONFIG
        self.log = log
        self.RUN_PRODUCT = RUN_PRODUCT

        # 실행가능한 product인지 확인하기
        self.currency_load()
        self.product_load()
        if not (self.RUN_PRODUCT in self.products):
            self.log.DEBUG = True
            logmsg = 'Can not execute because it is not registered : %s' % (self.RUN_PRODUCT,)
            self.log.w('red', 'INIT', 'Running', logmsg)
            exit(1)
        else:
            self.log.w('gray', 'INIT', 'Running', 'Main', os.getpid())

        # Queue 생성
        StopQueue = multiprocessing.Queue()
        ExpireTimerQueue = multiprocessing.Queue()
        OrderbookQueue = multiprocessing.Queue()
        ResultQueue = multiprocessing.Queue()

        # Processor 실행
        self.multiprocess_starter(name='MatchingProcessor', target=MatchingProcessor,
                                  kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT, 'currencys': self.currencys, 'products': self.products,
                                          'StopQueue': StopQueue, 'OrderbookQueue': OrderbookQueue, 'ResultQueue': ResultQueue})
        self.multiprocess_starter(name='StopProcessor', target=StopProcessor,
                                  kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT, 'currencys': self.currencys, 'products': self.products,
                                          'StopQueue': StopQueue, 'ResultQueue': ResultQueue})
        self.multiprocess_starter(name='OrderbookProcessor', target=OrderbookProcessor,
                                  kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT, 'currencys': self.currencys, 'products': self.products,
                                          'OrderbookQueue': OrderbookQueue})
        self.multiprocess_starter(name='ExpireTimerProcessor', target=ExpireTimerProcessor,
                                  kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT, 'currencys': self.currencys, 'products': self.products,
                                          'ExpireTimerQueue': ExpireTimerQueue})
        self.multiprocess_starter(name='ExpireProcessor', target=ExpireProcessor,
                                  kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT, 'currencys': self.currencys, 'products': self.products,
                                          'ExpireTimerQueue': ExpireTimerQueue})
        self.multiprocess_starter(name='ResultProcessor', target=ResultProcessor,
                                  kwargs={'log': self.log, 'RUN_PRODUCT': self.RUN_PRODUCT, 'currencys': self.currencys, 'products': self.products,
                                          'ResultQueue': ResultQueue})

        self.process_manager()

    def process_manager(self):
        self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
        if not os.path.exists(self.RUN_DIRECTORY):
            os.makedirs(self.RUN_DIRECTORY)

        self.kill_format = '{pid}.kill'
        pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))

        while True:
            #  PID 관련
            with open(os.path.join(self.RUN_DIRECTORY, 'xbit-match-{product_code}.pid'.format(product_code=self.RUN_PRODUCT.lower())), 'w') as f:
                f.write(str(os.getpid()))

            if os.path.exists(pidkill):
                # MatchProcessDaemon 안전 종료 순서
                # 1. MatchingProcessor
                self.killer(name='MatchingProcessor')
                # 2. ExpireTimerProcessor
                self.killer(name='ExpireTimerProcessor')
                # 3. ExpireProcessor
                self.killer(name='ExpireProcessor')
                # 4. StopProcessor
                self.killer(name='StopProcessor')
                # 5. OrderbookProcessor
                self.killer(name='OrderbookProcessor')
                # 6. ResultProcessor
                self.killer(name='ResultProcessor')
                os.remove(pidkill)

            # subprocess가 모두 죽으면 master로 죽자
            for process in self.process_list.values():
                if process['process'].is_alive():
                    break
            else:
                exit(0)
            time.sleep(0.1)

    def killer(self, name):
        process = self.process_list[name]
        with open(os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=process['pid'])), 'w') as f:
            pass

        do = True
        while do:
            process = self.process_list[name]
            if not process['process'].is_alive():
                do = False
            time.sleep(0.1)


if __name__ == '__main__':
    # option load
    parser = optparse.OptionParser()
    parser.add_option("--debug", action='store_true', default=False,
                      help="debug",
                      dest="debug")
    parser.add_option("-p", "--product", action='store',
                      dest="product")
    options, args = parser.parse_args()
    if options.product is None:
        parser.print_help(sys.stdout)
        exit(1)
    DEBUG = options.debug
    RUN_PRODUCT = options.product.upper()

    # log
    LoggingDirectory = os.path.join(
        CONFIG.LOG['directory'],
        RUN_PRODUCT,
        os.path.basename(FILENAME).rsplit('.', 1)[0]
    )
    log = Log(LoggingDirectory=LoggingDirectory, DEBUG=DEBUG)
    log.f_format = '%Y%m%d_%H.log'
    log.w('gray', 'LoggingDirectory', log.LoggingDirectory)

    # main
    me = singleton.SingleInstance(RUN_PRODUCT)
    main = Main(log=log, RUN_PRODUCT=RUN_PRODUCT)
