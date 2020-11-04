# -*- encoding:utf8 -*-
import os, sys, time, multiprocessing, optparse, traceback, json, copy, Queue

import MySQLdb, redis
from tendo import singleton

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.function import *
from lib.define import *
from lib.exchange import exchange, Response, exchangeConfigParser, WalletDB

### CONFIG PARSERING ###
config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'}
    ],
    'POSTPROCESS': [
        {'key': 'processor', 'method': 'getint', 'defaultvalue': 1},
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
        #{'key': 'password', 'method': 'get', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
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


class STAT:
    con = None

    @classmethod
    def send(cls, name):
        if cls.con is None:
            #cls.con = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'], password=CONFIG.REQUESTDB['password'], db=0)
            cls.con = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'], db=0)
        msg = json.dumps({'name': name, 'time': int(time.time())})
        cls.con.publish('STAT', msg)


class DistributeProcessor(exchange):
    ''' 분산용 Process '''

    def __init__(self, log, DistributeQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.DistributeQueue = DistributeQueue

        self.processdb_init()

        while True:
            try:
                while not self.DistributeQueue.empty():
                    pass
                Recv = self.processdb.brpop(self.PROCESSDBNAME['PROCESS_B'], timeout=1)  # timeout발생시 None이 return되기때문에 Recv로 None를 받아서 처리
                #print(Recv,"message getting in procesdb")
                if Recv:
                    _RecvName, _RecvMsg = Recv
                    #print("rec name",_RecvName,"rec message",_RecvMsg)
                    self.trx_id, _RecvMsg = eval(_RecvMsg)
                    self.run(_RecvName=_RecvName, _RecvMsg=_RecvMsg)

                pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    print("inside kill")
                    os.remove(pidkill)
                    exit(0)

            except Exception as err:
                self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                exit(1)

    def run(self, _RecvName, _RecvMsg):
        print("inside run")
        self.DistributeQueue.put({'_RecvMsg': _RecvMsg, 'trx_id': self.trx_id})


class PostProcessor(exchange):
    print("inside postPRocess")
    def __init__(self, log, DistributeQueue, OhlcQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.DistributeQueue = DistributeQueue
        self.OhlcQueue = OhlcQueue
        self.wdb = WalletDB(WALLETDB=self.CONFIG.WALLETDB, WALLETCACHE=self.CONFIG.WALLETCACHE)

        self.processdb_init()
        self.responsedb_init()
        self.storagedb_init()
        while True:
            try:
                try:
                    msg = self.DistributeQueue.get(timeout=1)
                except Queue.Empty:
                    msg = None
                    pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                    if os.path.exists(pidkill):
                        os.remove(pidkill)
                        exit(0)

                if msg:
                    self.trx_id = msg['trx_id']
                    self.run(_RecvMsg=msg['_RecvMsg'])
                    STAT.send('PostProcess')
            except Exception as err:
                if DEBUG:
                    self.log.w('red', 'ERROR', self.trx_id, traceback.format_exc(err))
                else:
                    self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                exit(1)

    def run(self, _RecvMsg):
        print("runing")
        self.storagedb_check()
        self.wdb.healthcheck()
        RecvMsg = json.loads(_RecvMsg)
        print("DATA FROM REDIS ======",RecvMsg)
        if set(RecvMsg.keys()) == set(['Open']):
            response = self.Open(RecvMsg=RecvMsg)
            #print(response)
            self.con.commit()
            self.OpenResponse(response=response)
        elif set(RecvMsg.keys()) == set(['Open', 'Match']):
            Takerinfo, Makerinfos, Feeinfos, trade_id = self.__make_wallet_exchanges(Matchs=RecvMsg['Match'])
            response = self.Open(RecvMsg=RecvMsg)
            MatchTaker_notice = self.MatchTaker(RecvMsg=RecvMsg, Takerinfo=Takerinfo, Makerinfos=Makerinfos, Feeinfos=Feeinfos)  # Taker Order 관련 처리
            response_list = self.MatchMaker(RecvMsg=RecvMsg)  # Maker Order 관련 처리
            kwargs = {'trx_id': Takerinfo['trx_id'], 'value': json.dumps({'Takerinfo': Takerinfo, 'Makerinfos': Makerinfos, 'Feeinfos': Feeinfos, 'trade_id': trade_id}), }
            self.cur.execute("INSERT INTO `trx_id` SET `trx_id`=%(trx_id)s, `value`=%(value)s, `insertDate`=UNIX_TIMESTAMP();", kwargs)
            self.con.commit()
            #print("this is order open for walletexchange let start printing values",Takerinfo,Markerinfos,Feeinfos,trade_id)
            self.MatchWalletExchanges(Takerinfo=Takerinfo, Makerinfos=Makerinfos, Feeinfos=Feeinfos, trade_id=trade_id)  # wallet 처리
            self.cur.execute("DELETE FROM `trx_id` WHERE `trx_id`=%(trx_id)s;", kwargs)
            self.con.commit()
            self.OpenResponse(response=response)  # Open Response
            self.MatchTakerResponse(Takerinfo=Takerinfo, trade_id=trade_id, notice=MatchTaker_notice)  # Taker Match Response
            self.MatchMakerResponse(response_list=response_list)  # Maker Response
            self.MatchTradeDataProcess(RecvMsg=RecvMsg)  # trade_price관련 데이터 처리(trade_history, stop, ohlc)

        elif set(RecvMsg.keys()) == set(['Done', 'Match']):
            Takerinfo, Makerinfos, Feeinfos, trade_id = self.__make_wallet_exchanges(Matchs=RecvMsg['Match'])
            print("after the match the taker info creating problem = ",Takerinfo)
            response = self.Done(RecvMsg=RecvMsg)
            MatchTaker_notice = self.MatchTaker(RecvMsg=RecvMsg, Takerinfo=Takerinfo, Makerinfos=Makerinfos, Feeinfos=Feeinfos)  # Taker Order 관련 처리
            response_list = self.MatchMaker(RecvMsg=RecvMsg)  # Maker Order 관련 처리
            kwargs = {'trx_id': Takerinfo['trx_id'], 'value': json.dumps({'Takerinfo': Takerinfo, 'Makerinfos': Makerinfos, 'Feeinfos': Feeinfos, 'trade_id': trade_id}), }
            self.cur.execute("INSERT INTO `trx_id` SET `trx_id`=%(trx_id)s, `value`=%(value)s, `insertDate`=UNIX_TIMESTAMP();", kwargs)
            self.con.commit()
            print("this is done order before going to matchawalletexchange function lets start printing",Takerinfo,Makerinfos,Feeinfos,trade_id)
            self.MatchWalletExchanges(Takerinfo=Takerinfo, Makerinfos=Makerinfos, Feeinfos=Feeinfos, trade_id=trade_id)  # wallet 처리
            self.cur.execute("DELETE FROM `trx_id` WHERE `trx_id`=%(trx_id)s;", kwargs)
            self.con.commit()
            self.DoneResponse(response=response)
            self.MatchTakerResponse(Takerinfo=Takerinfo, trade_id=trade_id, notice=MatchTaker_notice)  # Taker Match Response
            self.MatchMakerResponse(response_list=response_list)  # Maker Response
            self.MatchTradeDataProcess(RecvMsg=RecvMsg)  # trade_price관련 데이터 처리(trade_history, stop, ohlc)

        elif set(RecvMsg.keys()) == set(['MakerCancel']):
            response = self.MakerCancel(RecvMsg=RecvMsg)
            self.con.commit()
            self.MakerCancelResponse(response=response)

        elif set(RecvMsg.keys()) == set(['Modify']):
            response = self.Modify(RecvMsg=RecvMsg)
            self.con.commit()
            self.ModifyResponse(response=response)
        elif set(RecvMsg.keys()) == set(['Fail']):
            self.Fail(RecvMsg=RecvMsg)
        elif set(RecvMsg.keys()) == set(['StopRegist']):
            self.StopRegist(RecvMsg=RecvMsg)
        elif set(RecvMsg.keys()) == set(['StopCancel']):
            self.StopCancel(RecvMsg=RecvMsg)
        else:
            raise ValueError('UNKNOWN')

    def Open(self, RecvMsg):
        Orderinfo = RecvMsg['Open']
        #print(Orderinfo)
        additional_data = {'order_id': Orderinfo['order_id'], 'side': pSIDE[Orderinfo['side']], 'price': Orderinfo['price'], 'size': Orderinfo['size']}
        if 'stop_price' in Orderinfo:
            additional_data['stop_price'] = Orderinfo['stop_price']
            self.sdb_detail_stop(Orderinfo=Orderinfo)
            self.sdb_history_stop(Orderinfo=Orderinfo)
            self.sdb_order_stop_delete(Orderinfo=Orderinfo)
        self.sdb_history_open(Orderinfo=Orderinfo)
        self.sdb_order_open(Orderinfo=Orderinfo)
        notice = self.sdb_notice_reg_limit(Orderinfo=Orderinfo)
        additional_data['notice'] = notice
        response = self.ResponseOrder(response_code='LIMIT_REGIST', order_type=pORDER_TYPE[Orderinfo['order_type']], product=Orderinfo['product'], member_id=Orderinfo['member_id'],
                                      **additional_data)
        return response

    def Done(self, RecvMsg):
        Orderinfo = RecvMsg['Done']
        additional_data = dict()
        if 'stop_price' in Orderinfo:
            additional_data['stop_price'] = Orderinfo['stop_price']
            self.sdb_detail_stop(Orderinfo=Orderinfo)
            self.sdb_history_stop(Orderinfo=Orderinfo)
            self.sdb_order_stop_delete(Orderinfo=Orderinfo)
        self.sdb_history_open(Orderinfo=Orderinfo)
        self.sdb_order_done(Orderinfo=Orderinfo)
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            notice = self.sdb_notice_reg_limit(Orderinfo=Orderinfo)
            additional_data.update({'order_id': Orderinfo['order_id'], 'side': pSIDE[Orderinfo['side']], 'price': Orderinfo['price'], 'size': Orderinfo['size'], 'notice': notice})
            response = self.ResponseOrder(response_code='LIMIT_REGIST', order_type=pORDER_TYPE[Orderinfo['order_type']], product=Orderinfo['product'], member_id=Orderinfo['member_id'],
                                          **additional_data)
            return response
        else:
            return None

    def MatchTaker(self, RecvMsg, Takerinfo, Makerinfos, Feeinfos):
        for match in RecvMsg['Match']:
            TakerOrder = match['TakerOrder']
            Tradeinfo = match['Tradeinfo']
            self.sdb_detail_match(Orderinfo=TakerOrder, Tradeinfo=Tradeinfo)

        # notice
        notice = self.sdb_notice_match(member_id=Takerinfo['member_id'], product=Takerinfo['product'], order_type=Takerinfo['order_type'],
                                       side=Takerinfo['side'], fee=Takerinfo['fee']['IN'], trade_size=Takerinfo['trade_size'], trade_funds=Takerinfo['trade_funds'],
                                       bcc=Takerinfo['bcc'], qcc=Takerinfo['qcc'], bcd=TakerOrder['bcd'], qcd=TakerOrder['qcd'],
                                       stoporder=Takerinfo['stoporder'], stop_price=Takerinfo['stop_price'], price=Takerinfo['price'], )
        return notice

    def MatchMaker(self, RecvMsg):
        response_list = list()
        for match in RecvMsg['Match']:
            TakerOrder = match['TakerOrder']
            MakerOrder = match['MakerOrder']
            Tradeinfo = match['Tradeinfo']

            self.sdb_detail_match(Orderinfo=MakerOrder, Tradeinfo=Tradeinfo)

            if MakerOrder['remaining_size'] <= 0:
                self.sdb_order_done(Orderinfo=MakerOrder)
            else:
                self.sdb_order_update(Orderinfo=MakerOrder)

            notice = self.sdb_notice_match(member_id=MakerOrder['member_id'], product=MakerOrder['product'], order_type=MakerOrder['order_type'],
                                           side=MakerOrder['side'], fee=MakerOrder['fee'], trade_size=Tradeinfo['trade_size'], trade_funds=Tradeinfo['trade_funds'],
                                           bcc=MakerOrder['bcc'], qcc=MakerOrder['qcc'], bcd=MakerOrder['bcd'], qcd=MakerOrder['qcd'], price=MakerOrder['price'])
            additional_data = dict()
            if 'stop_price' in MakerOrder:
                additional_data['stop_price'] = MakerOrder['stop_price']
            additional_data.update({'order_id': MakerOrder['order_id'], 'price': Tradeinfo['trade_price'], 'size': Tradeinfo['trade_size'], 'side': pSIDE[MakerOrder['side']], 'fee': MakerOrder['fee'],
                                    'match_side': 'maker', 'trade_id': Tradeinfo['trade_id'], 'notice': notice, 'remaining_size': MakerOrder['remaining_size']})
            response = self.ResponseOrder(response_code='MATCH', order_type=pORDER_TYPE[MakerOrder['order_type']], product=Tradeinfo['product'], member_id=MakerOrder['member_id'],
                                          **additional_data)
            response_list.append(response)

            # trade history
            table = self.STORAGEDBNAME['TRADEHISTORY']
            sql = 'INSERT INTO {table} SET `product`=%(product)s, `sequence`=%(sequence)s, `price`=%(price)s, `trade_id`=%(trade_id)s, `time`=%(time)s, `side`=%(side)s, `size`=%(size)s, `trade_funds`=%(trade_funds)s, `taker_order_id`=%(taker_order_id)s, `maker_order_id`=%(maker_order_id)s, `bcd`=%(bcd)s, `qcd`=%(qcd)s;'.format(
                table=table)
            kwargs = {
                'product': Tradeinfo['product'],
                'sequence': Tradeinfo['sequence'],
                'price': Tradeinfo['trade_price'],
                'trade_id': Tradeinfo['trade_id'],
                'time': Tradeinfo['trade_time'],
                'side': TakerOrder['side'],
                'size': Tradeinfo['trade_size'],
                'trade_funds': Tradeinfo['trade_funds'],
                'taker_order_id': TakerOrder['order_id'],
                'maker_order_id': MakerOrder['order_id'],
                'bcd': TakerOrder['bcd'],
                'qcd': TakerOrder['qcd'],
            }
            self.cur.execute(sql, kwargs)

        return response_list

    def MakerCancel(self, RecvMsg):
        Orderinfo = RecvMsg['MakerCancel']['Orderinfo']
        Cancelinfo = RecvMsg['MakerCancel']['Cancelinfo']
        if Orderinfo['side'] == SIDE['BUY']:
            hold_amount = (Cancelinfo['cancel_size'] * Orderinfo['price']) / 10 ** Orderinfo['bcd']
        else:
            hold_amount = Cancelinfo['cancel_size']

        self.wdb.holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=hold_amount, trx_id=self.trx_id)

        self.sdb_detail_cancel(Orderinfo=Orderinfo, Cancelinfo=Cancelinfo)
        if Orderinfo['remaining_size'] <= 0:
            self.sdb_order_done(Orderinfo=Orderinfo)
        else:
            self.sdb_order_update(Orderinfo=Orderinfo)

        notice = self.sdb_notice_cancel_limit(member_id=Orderinfo['member_id'], product=Orderinfo['product'], side=Orderinfo['side'], order_type=Orderinfo['order_type'],
                                              price=Orderinfo['price'], qcc=Orderinfo['qcc'], cancel_size=Cancelinfo['cancel_size'], bcc=Orderinfo['bcc'],
                                              bcd=Orderinfo['bcd'], qcd=Orderinfo['qcd'])

        response = self.ResponseOrder(response_code='CANCEL', order_type=pORDER_TYPE[Orderinfo['order_type']], product=Orderinfo['product'], member_id=Orderinfo['member_id'],
                                      order_id=Orderinfo['order_id'], cancel_size=Cancelinfo['cancel_size'], remaining_size=Orderinfo['remaining_size'], side=pSIDE[Orderinfo['side']],
                                      notice=notice)
        return response

    # def Modify(self, RecvMsg):
    #     Orderinfo = RecvMsg['Modify']['Orderinfo']
    #     Modifyinfo = RecvMsg['Modify']['Modifyinfo']
    #
    #     if Modifyinfo['old_price'] != Modifyinfo['price']:
    #         if Orderinfo['side'] == SIDE['BUY']:
    #             old_hold = (Orderinfo['remaining_size'] * Modifyinfo['old_price']) / (10 ** Orderinfo['bcd'])
    #             new_hold = (Orderinfo['remaining_size'] * Modifyinfo['price']) / (10 ** Orderinfo['bcd'])
    #             amount = new_hold - old_hold
    #             if amount > 0:
    #                 # self.wallet_holdup(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=amount)
    #                 self.wsc.HoldUp(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=amount, trx_id=self.trx_id)
    #             elif amount < 0:
    #                 # self.wallet_holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=abs(amount))
    #                 self.wsc.HoldDn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=abs(amount), trx_id=self.trx_id)
    #         self.sdb_history_modify(Orderinfo=Orderinfo, Modifyinfo=Modifyinfo)
    #         self.sdb_order_update(Orderinfo=Orderinfo)
    #
    #     notice = self.sdb_notice_modify_limit(member_id=Orderinfo['member_id'], product=Orderinfo['product'], side=Orderinfo['side'],
    #                                              order_type=Orderinfo['order_type'], old_price=Modifyinfo['old_price'], price=Modifyinfo['price'],
    #                                              qcc=Orderinfo['qcc'], qcd=Orderinfo['qcd'], bcc=Orderinfo['bcc'])
    #     response = self.ResponseOrder(response_code='MODIFY', order_type=pORDER_TYPE[Orderinfo['order_type']], product=Orderinfo['product'], member_id=Orderinfo['member_id'],
    #                                   order_id=Orderinfo['order_id'], old_price=Modifyinfo['old_price'], new_price=Modifyinfo['price'],
    #                                   notice=notice)
    #     return response
    def Fail(self, RecvMsg):
        response_code = RecvMsg['Fail']['response_code']
        if response_code == 'NOT_ACCUMULATE_SIZE':
            if RecvMsg['Fail']['data']['order_type'] == ORDER_TYPE['MARKET']:
                Orderinfo = RecvMsg['Fail']['data']
                self.wdb.holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
                # r = self.wsc.HoldDn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
                # if r['response_code'] == 0:
                #     if Orderinfo['side'] == SIDE['BUY']:
                #         response = Response.NOT_ACCUMULATE_SIZE_MARKET_BUY(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], funds=Orderinfo['funds'])
                #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
                #
                #     elif Orderinfo['side'] == SIDE['SELL']:
                #         response = Response.NOT_ACCUMULATE_SIZE_MARKET_SELL(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], size=Orderinfo['size'])
                #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
                #
                #     else:
                #         raise ValueError('UNKNOWN')
                #
                # else:
                #     response = Response.INTERNAL_ERROR_(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], message=json.dumps(r))
                #     self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #     self.log.w('red', 'FAIL', self.trx_id, 'INTERNAL_ERROR', response)

            elif RecvMsg['Fail']['data']['order_type'] == ORDER_TYPE['STOP']:
                Stopinfo = RecvMsg['Fail']['data']
                Orderinfo = Stopinfo['Orderinfo']
                self.wdb.holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
                # r = self.wsc.HoldDn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
                # if r['response_code'] == 0:
                #     if Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
                #         response = Response.NOT_ACCUMULATE_SIZE_STOP_MARKET_BUY(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], funds=Orderinfo['funds'])
                #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
                #
                #     elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
                #         response = Response.NOT_ACCUMULATE_SIZE_STOP_MARKET_SELL(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], size=Orderinfo['size'])
                #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
                #
                #     elif Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
                #         response = Response.NOT_ACCUMULATE_SIZE_STOP_LIMIT(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], side=Orderinfo['side'], stop_price=Orderinfo['stop_price'], price=Orderinfo['price'],
                #                                                            size=Orderinfo['size'])
                #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
                #
                #     else:
                #         raise ValueError('UNKNOWN')
                #
                # else:
                #     response = Response.INTERNAL_ERROR_(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], message=json.dumps(r))
                #     self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                #     self.log.w('red', 'FAIL', self.trx_id, 'INTERNAL_ERROR', response)

            else:
                raise ValueError('UNKNOWN')

        elif response_code == 'NOT_ALLOW_ORDER':
            Orderinfo = RecvMsg['Fail']['data']
            self.wdb.holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
            # r = self.wsc.HoldDn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
            # if r['response_code'] == 0:
            #     if Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
            #         response = Response.NOT_ALLOW_ORDER_MARKET_BUY(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], funds=Orderinfo['funds'])
            #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ALLOW_ORDER', response)
            #
            #     elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
            #         response = Response.NOT_ALLOW_ORDER_MARKET_SELL(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], size=Orderinfo['size'])
            #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ALLOW_ORDER', response)
            #
            #     else:
            #         raise ValueError('UNKNOWN')
            #
            # else:
            #     response = Response.INTERNAL_ERROR_(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], message=json.dumps(r))
            #     self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #     self.log.w('red', 'FAIL', self.trx_id, 'INTERNAL_ERROR', response)

        elif response_code == 'NOT_ALLOW_STOP_PRICE':
            Stopinfo = RecvMsg['Fail']['data']
            Orderinfo = Stopinfo['Orderinfo']
            self.wdb.holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
            # r = self.wsc.HoldDn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
            # if r['response_code'] == 0:
            #     if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            #         response = Response.NOT_ALLOW_STOP_PRICE_LIMIT(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], side=Orderinfo['side'], price=Orderinfo['price'],
            #                                                        size=Orderinfo['size'])
            #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
            #
            #     elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
            #         response = Response.NOT_ALLOW_STOP_PRICE_MARKET_BUY(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], funds=Orderinfo['funds'])
            #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
            #
            #     elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
            #         response = Response.NOT_ALLOW_STOP_PRICE_MARKET_SELL(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], size=Orderinfo['size'])
            #         self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #         self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ACCUMULATE_SIZE', response)
            #
            #     else:
            #         raise ValueError('UNKNOWN')
            #
            # else:
            #     response = Response.INTERNAL_ERROR_(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], message=json.dumps(r))
            #     self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            #     self.log.w('red', 'FAIL', self.trx_id, 'INTERNAL_ERROR', response)

        elif response_code == 'NOT_FOUND_ORDER_ID':
            Cancelinfo = RecvMsg['Fail']['data']
            if Cancelinfo['order_type'] == ORDER_TYPE['CANCEL']:
                response = Response.NOT_FOUND_ORDER_ID_CANCEL(member_id=Cancelinfo['member_id'], product_code=Cancelinfo['product'], order_id=Cancelinfo['order_id'])
                self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_FOUND_ORDER_ID', response)
            elif Cancelinfo['order_type'] == ORDER_TYPE['STOPCANCEL']:
                response = Response.NOT_FOUND_ORDER_ID_STOPCANCEL(member_id=Cancelinfo['member_id'], product_code=Cancelinfo['product'], order_id=Cancelinfo['order_id'])
                self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
                self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_FOUND_ORDER_ID', response)
            else:
                raise ValueError('UNKNOWN')

        else:
            raise ValueError('unknown')

    def StopRegist(self, RecvMsg):
        Orderinfo = RecvMsg['StopRegist']
        self.sdb_order_stop(Orderinfo)
        notice = self.sdb_notice_stop(Orderinfo=Orderinfo)
        self.con.commit()
        response = self.ResponseOrder(response_code='STOP_REGIST', order_type=pORDER_TYPE[Orderinfo['order_type']], product=Orderinfo['product'], member_id=Orderinfo['member_id'],
                                      order_id=Orderinfo['order_id'], side=pSIDE[Orderinfo['side']], stop_price=Orderinfo['stop_price'], notice=notice)
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            response.update({'price': Orderinfo['price'], 'size': Orderinfo['size']})
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
            response.update({'funds': Orderinfo['funds']})
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
            response.update({'size': Orderinfo['size']})
        else:
            raise ValueError('unknown')
        self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
        self.log.w('green', 'SUCC', self.trx_id, 'STOP_REGIST', response)

    def StopCancel(self, RecvMsg):
        # Cancelinfo = RecvMsg['StopCancel']['Cancelinfo']
        Orderinfo = RecvMsg['StopCancel']['Orderinfo']
        if 'stop_price' in Orderinfo:
            if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
                kwargs = {'price': Orderinfo['price'], 'size': Orderinfo['size']}
            elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
                kwargs = {'funds': Orderinfo['funds']}
            elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
                kwargs = {'size': Orderinfo['size']}
            else:
                raise ValueError('unknown')

            self.wdb.holddn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
            # self.wsc.HoldDn(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'], trx_id=self.trx_id)
            self.sdb_order_stop_delete(Orderinfo=Orderinfo)
            notice = self.sdb_notice_cancel_stop(Orderinfo=Orderinfo)

            self.con.commit()
            response = self.ResponseOrder(response_code='STOP_CANCEL', order_type=pORDER_TYPE[Orderinfo['order_type']], product=Orderinfo['product'], member_id=Orderinfo['member_id'],
                                          order_id=Orderinfo['order_id'], side=pSIDE[Orderinfo['side']], stop_price=Orderinfo['stop_price'], notice=notice, **kwargs)
            self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            self.log.w('green', 'SUCC', self.trx_id, 'STOP_CANCEL', response)

    def sdb_order_open(self, Orderinfo):
        '''미체결내역'''
        #print("inside insertion of order open",Orderinfo)
        table = self.STORAGEDBNAME['ORDER:OPEN']
        sql = "INSERT INTO `{table}` SET " \
              "`order_id`=%(order_id)s, " \
              "`member_id`=%(member_id)s, " \
              "`broker_id`='00000000-0000-0000-0000-000000000000', " \
              "`order_type`=%(order_type)s, " \
              "`product`=%(product)s, " \
              "`product_id`=%(product_id)s, " \
              "`sequence`=%(sequence)s, " \
              "`side`=%(side)s, " \
              "`amount_type`=%(amount_type)s, " \
              "`size`=%(size)s, " \
              "`funds`=%(funds)s, " \
              "`price`=%(price)s, " \
              "`remaining_size`=%(remaining_size)s, " \
              "`remaining_funds`=%(remaining_funds)s, " \
              "`open_price`=%(open_price)s, " \
              "`incoming`=1, " \
              "`magicnumber`=%(magicnumber)s, " \
              "`stoporder`=%(stoporder)s, " \
              "`policy`=%(policy)s, " \
              "`accumulate_size`=%(accumulate_size)s, " \
              "`accumulate_funds`=%(accumulate_funds)s, " \
              "`accumulate_cancelsize`=%(accumulate_cancelsize)s, " \
              "`accumulate_fee`=%(accumulate_fee)s, " \
              "`hold_amount`=%(hold_amount)s, " \
              "`hold_currency_code`=%(hold_currency_code)s, " \
              "`hold_digit`=%(hold_digit)s, " \
              "`tfr`=%(tfr)s, " \
              "`mfr`=%(mfr)s, " \
              "`receive_time`=%(receive_time)s, " \
              "`open_time`=%(open_time)s, " \
              "`done_time`=%(done_time)s, " \
              "`f_sequence`=%(f_sequence)s, " \
              "`bcc`=%(bcc)s, " \
              "`qcc`=%(qcc)s, " \
              "`bcd`=%(bcd)s, " \
              "`qcd`=%(qcd)s, " \
              "`expire_time`=%(expire_time)s;".format(table=table)
        kwargs = copy.deepcopy(Orderinfo)
        kwargs['funds'] = None
        kwargs['remaining_funds'] = None
        if 'expire_time' not in kwargs:
            kwargs['expire_time'] = None
        self.cur.execute(sql, kwargs)

        # self.responsedb.lpush('OPEN:%s' % (Orderinfo['order_id'],), Orderinfo['order_id'])

    def sdb_order_update(self, Orderinfo):
        '''미체결내역'''
        #print("inside sdb_order_update",Orderinfo)
        table = self.STORAGEDBNAME['ORDER:OPEN']
        sql = "UPDATE `{table}` SET " \
              "`price`=%(price)s, " \
              "`open_price`=%(open_price)s, " \
              "`remaining_size`=%(remaining_size)s, " \
              "`accumulate_size`=%(accumulate_size)s, " \
              "`accumulate_funds`=%(accumulate_funds)s, " \
              "`accumulate_cancelsize`=%(accumulate_cancelsize)s, " \
              "`accumulate_fee`=%(accumulate_fee)s, " \
              "`done_time`=%(done_time)s, " \
              "`f_sequence`=%(f_sequence)s " \
              "WHERE `order_id`=%(order_id)s AND `f_sequence` <= %(f_sequence)s;".format(table=table)
        kwargs = copy.deepcopy(Orderinfo)
        self.cur.execute(sql, kwargs)

    def sdb_order_done(self, Orderinfo):
        '''거래내역'''
        #print("inside order done",Orderinfo)
        table = self.STORAGEDBNAME['ORDER:DONE']
        sql = "INSERT INTO `{table}` SET " \
              "`order_id`=%(order_id)s, " \
              "`member_id`=%(member_id)s, " \
              "`broker_id`='00000000-0000-0000-0000-000000000000', " \
              "`order_type`=%(order_type)s, " \
              "`product`=%(product)s, " \
              "`product_id`=%(product_id)s, " \
              "`sequence`=%(sequence)s, " \
              "`side`=%(side)s, " \
              "`amount_type`=%(amount_type)s, " \
              "`size`=%(size)s, " \
              "`funds`=%(funds)s, " \
              "`price`=%(price)s, " \
              "`remaining_size`=%(remaining_size)s, " \
              "`remaining_funds`=%(remaining_funds)s, " \
              "`open_price`=%(open_price)s, " \
              "`incoming`=1, " \
              "`magicnumber`=%(magicnumber)s, " \
              "`stoporder`=%(stoporder)s, " \
              "`policy`=%(policy)s, " \
              "`accumulate_size`=%(accumulate_size)s, " \
              "`accumulate_funds`=%(accumulate_funds)s, " \
              "`accumulate_cancelsize`=%(accumulate_cancelsize)s, " \
              "`accumulate_fee`=%(accumulate_fee)s, " \
              "`hold_amount`=%(hold_amount)s, " \
              "`hold_currency_code`=%(hold_currency_code)s, " \
              "`hold_digit`=%(hold_digit)s, " \
              "`tfr`=%(tfr)s, " \
              "`mfr`=%(mfr)s, " \
              "`receive_time`=%(receive_time)s, " \
              "`open_time`=%(open_time)s, " \
              "`done_time`=%(done_time)s, " \
              "`f_sequence`=%(f_sequence)s, " \
              "`bcc`=%(bcc)s, " \
              "`qcc`=%(qcc)s, " \
              "`bcd`=%(bcd)s, " \
              "`qcd`=%(qcd)s;".format(table=table)
        kwargs = copy.deepcopy(Orderinfo)
        if Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
            kwargs['price'] = None
            kwargs['policy'] = None
            kwargs['mfr'] = None
            kwargs['size'] = None
            kwargs['remaining_size'] = None
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
            kwargs['price'] = None
            kwargs['policy'] = None
            kwargs['mfr'] = None
            kwargs['funds'] = None
            kwargs['remaining_funds'] = None
        elif Orderinfo['order_type'] == ORDER_TYPE['LIMIT'] and Orderinfo['policy'] in (POLICY['GTC'], POLICY['GTT']):
            kwargs['funds'] = None
            kwargs['remaining_funds'] = None
        elif Orderinfo['order_type'] == ORDER_TYPE['LIMIT'] and Orderinfo['policy'] in (POLICY['IOC'], POLICY['FOK']):
            kwargs['mfr'] = None
            kwargs['funds'] = None
            kwargs['remaining_funds'] = None

        # if Orderinfo['match_side'] == 'maker':
        #     self.responsedb.brpop('OPEN:%s' % (Orderinfo['order_id'],))
        # elif Orderinfo['match_side'] == 'cancel':
        #     r = self.responsedb.rpop('OPEN:%s' % (Orderinfo['order_id'],))
        #     # if r == None:
        #     #     print 'cancel', Orderinfo['order_id'], r
        # else: # taker
        #     r = self.responsedb.rpop('OPEN:%s' % (Orderinfo['order_id'],))
        #     # print 'taker', r

        self.cur.execute(sql, kwargs)

    def sdb_order_stop(self, Orderinfo):
        ''' 예약내역'''
        table = self.STORAGEDBNAME['ORDER:STOP']
        sql = "INSERT INTO `{table}` SET " \
              "`order_id`=%(order_id)s, " \
              "`member_id`=%(member_id)s, " \
              "`broker_id`='00000000-0000-0000-0000-000000000000', " \
              "`time`=%(receive_time)s, " \
              "`product`=%(product_code)s, " \
              "`product_id`=%(product_id)s, " \
              "`side`=%(side)s, " \
              "`order_type`=%(order_type)s, " \
              "`stop_price`=%(stop_price)s, " \
              "`price`=%(price)s, " \
              "`size`=%(size)s, " \
              "`funds`=%(funds)s, " \
              "`bcc`=%(bcc)s, " \
              "`qcc`=%(qcc)s, " \
              "`bcd`=%(bcd)s, " \
              "`qcd`=%(qcd)s, " \
              "`Orderinfo`=%(Orderinfo)s;".format(table=table)
        kwargs = {
            'order_id': Orderinfo['order_id'],
            'member_id': Orderinfo['member_id'],
            'receive_time': Orderinfo['receive_time'],
            'product_code': Orderinfo['product'],
            'product_id': Orderinfo['product_id'],
            'side': Orderinfo['side'],
            'order_type': Orderinfo['order_type'],
            'stop_price': Orderinfo['stop_price'],
            'price': None,
            'size': None,
            'funds': None,
            'bcc': Orderinfo['bcc'],
            'qcc': Orderinfo['qcc'],
            'bcd': Orderinfo['bcd'],
            'qcd': Orderinfo['qcd'],
            'Orderinfo': json.dumps(Orderinfo)}
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            kwargs['price'] = Orderinfo['price']
            kwargs['size'] = Orderinfo['size']
            kwargs['funds'] = (Orderinfo['size'] * Orderinfo['price']) / (10 ** Orderinfo['bcd'])
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['amount_type'] == AMOUNT_TYPE['FUNDS']:  # Market BUY
            kwargs['funds'] = Orderinfo['funds']
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['amount_type'] == AMOUNT_TYPE['SIZE']:  # Market SELL
            kwargs['size'] = Orderinfo['size']
        self.cur.execute(sql, kwargs)

    def sdb_order_stop_delete(self, Orderinfo):
        table = self.STORAGEDBNAME['ORDER:STOP']
        sql = "DELETE FROM `{table}` WHERE `order_id`=%(order_id)s;".format(table=table)
        self.cur.execute(sql, {'order_id': Orderinfo['order_id']})

    def __sdb_history_insert(self, order_id, sequence, member_id, time, product_code, product_id, side, order_type, price, amount, amount_type, history_type, stop_price, bcc, qcc, bcd, qcd):
        table = self.STORAGEDBNAME['ORDER:HISTORY'].format(product_code=product_code)
        sql = "INSERT INTO `{table}` SET " \
              "`order_id`=%(order_id)s, " \
              "`sequence`=%(sequence)s, " \
              "`member_id`=%(member_id)s, " \
              "`time`=%(time)s, " \
              "`product`=%(product_code)s, " \
              "`product_id`=%(product_id)s, " \
              "`side`=%(side)s, " \
              "`order_type`=%(order_type)s, " \
              "`price`=%(price)s, " \
              "`amount`=%(amount)s," \
              "`amount_type`=%(amount_type)s, " \
              "`history_type`=%(history_type)s, " \
              "`stop_price`=%(stop_price)s, " \
              "`bcc`=%(bcc)s, " \
              "`qcc`=%(qcc)s, " \
              "`bcd`=%(bcd)s, " \
              "`qcd`=%(qcd)s;".format(table=table)
        kwargs = {'order_id': order_id, 'sequence': sequence, 'member_id': member_id, 'time': time, 'product_code': product_code,
                  'product_id': product_id, 'side': side, 'order_type': order_type, 'price': price, 'amount': amount, 'amount_type': amount_type,
                  'history_type': history_type, 'stop_price': stop_price, 'bcc': bcc, 'qcc': qcc, 'bcd': bcd, 'qcd': qcd}
        self.cur.execute(sql, kwargs)

    def sdb_history_open(self, Orderinfo):
        '''주문내역 OPEN'''
        kwargs = {
            'order_id': Orderinfo['order_id'],
            'sequence': Orderinfo['sequence'],
            'member_id': Orderinfo['member_id'],
            'time': Orderinfo['open_time'],
            'product_code': Orderinfo['product'],
            'product_id': Orderinfo['product_id'],
            'side': Orderinfo['side'],
            'order_type': Orderinfo['order_type'],
            'price': None,
            'amount': None,
            'amount_type': Orderinfo['amount_type'],
            'history_type': HISTORY_TYPE['OPEN'],
            'stop_price': None,
            'bcc': Orderinfo['bcc'],
            'qcc': Orderinfo['qcc'],
            'bcd': Orderinfo['bcd'],
            'qcd': Orderinfo['qcd'],
        }
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            kwargs['price'] = Orderinfo['price']
            kwargs['amount'] = Orderinfo['size']
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['amount_type'] == AMOUNT_TYPE['SIZE']:
            kwargs['amount'] = Orderinfo['size']
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['amount_type'] == AMOUNT_TYPE['FUNDS']:
            kwargs['amount'] = Orderinfo['funds']
        else:
            raise ValueError('UNKNOWN')
        self.__sdb_history_insert(**kwargs)

    def sdb_history_stop(self, Orderinfo):
        kwargs = {
            'order_id': Orderinfo['order_id'],
            'sequence': Orderinfo['sequence'],
            'member_id': Orderinfo['member_id'],
            'time': Orderinfo['open_time'],
            'product_code': Orderinfo['product'],
            'product_id': Orderinfo['product_id'],
            'side': Orderinfo['side'],
            'order_type': Orderinfo['order_type'],
            'price': None,
            'amount': None,
            'amount_type': Orderinfo['amount_type'],
            'history_type': HISTORY_TYPE['STOP'],
            'stop_price': Orderinfo['stop_price'],
            'bcc': Orderinfo['bcc'],
            'qcc': Orderinfo['qcc'],
            'bcd': Orderinfo['bcd'],
            'qcd': Orderinfo['qcd'],
        }
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            kwargs['price'] = Orderinfo['price']
            kwargs['amount'] = Orderinfo['size']
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['amount_type'] == AMOUNT_TYPE['SIZE']:
            kwargs['amount'] = Orderinfo['size']
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['amount_type'] == AMOUNT_TYPE['FUNDS']:
            kwargs['amount'] = Orderinfo['funds']
        else:
            raise ValueError('UNKNOWN')
        self.__sdb_history_insert(**kwargs)

    # def sdb_history_modify(self, Orderinfo, Modifyinfo):
    #     kwargs = {
    #         'order_id': Orderinfo['order_id'],
    #         'sequence': Orderinfo['sequence'],
    #         'member_id': Orderinfo['member_id'],
    #         'time': Orderinfo['open_time'],
    #         'product_code': Orderinfo['product'],
    #         'product_id': Orderinfo['product_id'],
    #         'side': Orderinfo['side'],
    #         'order_type': Orderinfo['order_type'],
    #         'price': Orderinfo['price'],
    #         'amount': Orderinfo['remaining_size'],
    #         'amount_type': Orderinfo['amount_type'],
    #         'history_type': HISTORY_TYPE['MODIFY'],
    #         'stop_price': None,
    #         'bcc':Orderinfo['bcc'],
    #         'qcc': Orderinfo['qcc'],
    #         'bcd': Orderinfo['bcd'],
    #         'qcd': Orderinfo['qcd'],
    #     }
    #     self.__sdb_history_insert(**kwargs)
    def __sdb_detail_insert(self, detail_type, order_id, ext_id, time, product_code, product_id, side, order_type, price, size, fee, funds, bcc, qcc, bcd, qcd):
        table = self.STORAGEDBNAME['ORDER:DETAIL'].format(product_code=product_code)
        sql = "INSERT INTO `{table}` SET " \
              "`detail_type`=%(detail_type)s, " \
              "`order_id`=%(order_id)s, " \
              "`ext_id`=%(ext_id)s, " \
              "`time`=%(time)s, " \
              "`product`=%(product_code)s, " \
              "`product_id`=%(product_id)s, " \
              "`side`=%(side)s, " \
              "`order_type`=%(order_type)s, " \
              "`price`=%(price)s, " \
              "`size`=%(size)s, " \
              "`fee`=%(fee)s, " \
              "`funds`=%(funds)s, " \
              "`bcc`=%(bcc)s, " \
              "`qcc`=%(qcc)s, " \
              "`bcd`=%(bcd)s, " \
              "`qcd`=%(qcd)s;".format(table=table)
        kwargs = {'detail_type': detail_type, 'order_id': order_id, 'ext_id': ext_id, 'time': time, 'product_code': product_code,
                  'product_id': product_id, 'side': side, 'order_type': order_type, 'price': price, 'size': size, 'fee': fee,
                  'funds': funds, 'bcc': bcc, 'qcc': qcc, 'bcd': bcd, 'qcd': qcd, }
        self.cur.execute(sql, kwargs)

    def sdb_detail_match(self, Orderinfo, Tradeinfo):
        kwargs = {
            'detail_type': DETAIL_TYPE['MATCH'],
            'order_id': Orderinfo['order_id'],
            'ext_id': Tradeinfo['trade_id'],
            'time': Tradeinfo['trade_time'],
            'product_code': Orderinfo['product'],
            'product_id': Orderinfo['product_id'],
            'side': Orderinfo['side'],
            'order_type': Orderinfo['order_type'],
            'price': Tradeinfo['trade_price'],
            'size': Tradeinfo['trade_size'],
            'fee': Orderinfo['fee'],
            'funds': Tradeinfo['trade_funds'],
            'bcc': Orderinfo['bcc'],
            'qcc': Orderinfo['qcc'],
            'bcd': Orderinfo['bcd'],
            'qcd': Orderinfo['qcd'],
        }
        self.__sdb_detail_insert(**kwargs)

    def sdb_detail_cancel(self, Orderinfo, Cancelinfo):
        kwargs = {
            'detail_type': DETAIL_TYPE['CANCEL'],
            'order_id': Orderinfo['order_id'],
            'ext_id': Cancelinfo['sequence'],
            'time': Cancelinfo['cancel_time'],
            'product_code': Orderinfo['product'],
            'product_id': Orderinfo['product_id'],
            'side': Orderinfo['side'],
            'order_type': Cancelinfo['order_type'],
            'price': None,
            'size': Cancelinfo['cancel_size'],
            'fee': None,
            'funds': None,
            'bcc': Orderinfo['bcc'],
            'qcc': Orderinfo['qcc'],
            'bcd': Orderinfo['bcd'],
            'qcd': Orderinfo['qcd'],
        }
        self.__sdb_detail_insert(**kwargs)

    def sdb_detail_stop(self, Orderinfo):
        kwargs = {
            'detail_type': DETAIL_TYPE['STOP'],
            'order_id': Orderinfo['order_id'],
            'ext_id': Orderinfo['order_id'],
            'time': Orderinfo['receive_time'],
            'product_code': Orderinfo['product'],
            'product_id': Orderinfo['product_id'],
            'side': Orderinfo['side'],
            'order_type': Orderinfo['order_type'],
            'price': Orderinfo['stop_price'],
            'size': None,
            'fee': None,
            'funds': None,
            'bcc': Orderinfo['bcc'],
            'qcc': Orderinfo['qcc'],
            'bcd': Orderinfo['bcd'],
            'qcd': Orderinfo['qcd'],
        }
        self.__sdb_detail_insert(**kwargs)

    def __sdb_notice_insert(self, member_id, notice_type, values):
        table = self.STORAGEDBNAME['NOTICE']
        sql = "INSERT INTO `{table}` SET `member_id`=%(member_id)s, `notice_type`=%(notice_type)s, `notice_time`=%(notice_time)s, `values`=%(values)s;".format(table=table)
        kwargs = {
            'member_id': member_id,
            'notice_type': notice_type,
            'notice_time': DATETIME_UNIXTIMESTAMP(),
            'values': json.dumps(values),
        }
        self.cur.execute(sql, kwargs)
        return self.cur.lastrowid

    def sdb_notice_reg_limit(self, Orderinfo):
        bcc, qcc = Orderinfo['bcc'], Orderinfo['qcc']
        bcd, qcd = Orderinfo['bcd'], Orderinfo['qcd']
        bcu, qcu = float(10 ** bcd), float(10 ** qcd)
        values = {'product_name': '%s/%s' % (bcc, qcc),
                  'bcc': bcc, 'qcc': qcc,
                  'product': Orderinfo['product'],
                  'side': Orderinfo['side'],
                  'order_type': Orderinfo['order_type'],
                  'price': Orderinfo['price'] / qcu,
                  'size': Orderinfo['size'] / bcu,
                  'stoporder': Orderinfo['stoporder']}
        if Orderinfo['stoporder'] == 1:
            values['stop_price'] = Orderinfo['stop_price'] / qcu
            notice_type = NOTICE_TYPE['REG_LIMIT_STOP']
        else:
            notice_type = NOTICE_TYPE['REG_LIMIT']
        notice_id = self.__sdb_notice_insert(member_id=Orderinfo['member_id'], notice_type=notice_type, values=values)
        notice = values.copy()
        notice['notice_id'] = notice_id
        notice['notice_type'] = notice_type
        notice['notice_time'] = DATETIME_UNIXTIMESTAMP()
        return notice

    def sdb_notice_modify_limit(self, member_id, product, side, order_type, old_price, price, qcc, qcd, bcc):
        qcu = float(10 ** qcd)
        notice_type = NOTICE_TYPE['MODIFY_LIMIT']
        values = {'product_name': '%s/%s' % (bcc, qcc), 'product': product, 'side': side, 'order_type': order_type, 'old_price': old_price / qcu, 'price': price / qcu, 'qcc': qcc, 'bcc': bcc}
        notice_id = self.__sdb_notice_insert(member_id=member_id, notice_type=notice_type, values=values)
        notice = values.copy()
        notice['notice_id'] = notice_id
        notice['notice_type'] = notice_type
        notice['notice_time'] = DATETIME_UNIXTIMESTAMP()
        return notice

    def sdb_notice_match(self, member_id, product, order_type, side, fee, trade_size, trade_funds, bcc, qcc, bcd, qcd, stoporder=None, stop_price=None, price=None):
        bcu, qcu = float(10 ** bcd), float(10 ** qcd)
        values = {
            'product_name': '%s/%s' % (bcc, qcc),
            'bcc': bcc, 'qcc': qcc,
            'product': product,
            'side': side,
            'order_type': order_type, }
        if side == SIDE['BUY']:
            values.update({'fee': fee / bcu, 'in_amount': trade_size / bcu, 'in_amount_currency': bcc, 'out_amount': trade_funds / qcu, 'out_amount_currency': qcc, })
        else:
            values.update({'fee': fee / qcu, 'in_amount': trade_funds / qcu, 'in_amount_currency': qcc, 'out_amount': trade_size / bcu, 'out_amount_currency': bcc, })

        if order_type == ORDER_TYPE['LIMIT']:
            notice_type = NOTICE_TYPE['MATCH_LIMIT']
            values.update({'price': price / qcu})
            # print '{product} {side} {order_type} 주문이 체결되었습니다. / IN : {in_amount}({fee}) {in_amount_currency} / OUT : {out_amount} {out_amount_currency}'.format(**values)
        elif order_type == ORDER_TYPE['MARKET'] and stoporder == 1:
            values.update({'stoporder': stoporder, 'stop_price': stop_price / qcu, })
            notice_type = NOTICE_TYPE['MATCH_MARKET_STOP']
            # print '{product} {side} 예약({stop_price})에 의한 {order_type} 주문이 체결되었습니다. / IN : {in_amount}({fee}) {in_amount_currency} / OUT : {out_amount} {out_amount_currency}'.format(**values)
        else:
            notice_type = NOTICE_TYPE['MATCH_MARKET']
            # print '{product} {side} {order_type} 주문이 체결되었습니다. / IN : {in_amount}({fee}) {in_amount_currency} / OUT : {out_amount} {out_amount_currency}'.format(**values)
        notice_id = self.__sdb_notice_insert(member_id=member_id, notice_type=notice_type, values=values)
        notice = values.copy()
        notice['notice_id'] = notice_id
        notice['notice_type'] = notice_type
        notice['notice_time'] = DATETIME_UNIXTIMESTAMP()
        return notice

    def sdb_notice_cancel_limit(self, member_id, product, side, order_type, price, qcc, cancel_size, bcc, bcd, qcd):
        bcu, qcu = float(10 ** bcd), float(10 ** qcd)
        notice_type = NOTICE_TYPE['CANCEL_LIMIT']
        values = {'product_name': '%s/%s' % (bcc, qcc),
                  'bcc': bcc, 'qcc': qcc,
                  'product': product, 'side': side, 'order_type': order_type, 'price': price / qcu, 'cancel_size': cancel_size / bcu, }
        notice_id = self.__sdb_notice_insert(member_id=member_id, notice_type=notice_type, values=values)
        notice = values.copy()
        notice['notice_id'] = notice_id
        notice['notice_type'] = notice_type
        notice['notice_time'] = DATETIME_UNIXTIMESTAMP()
        return notice

    def sdb_notice_stop(self, Orderinfo):
        bcc, qcc = Orderinfo['bcc'], Orderinfo['qcc']
        bcd, qcd = Orderinfo['bcd'], Orderinfo['qcd']
        bcu, qcu = float(10 ** bcd), float(10 ** qcd)
        values = {'product_name': '%s/%s' % (bcc, qcc),
                  'bcc': bcc, 'qcc': qcc,
                  'product': Orderinfo['product'],
                  'side': Orderinfo['side'],
                  'order_type': Orderinfo['order_type'],
                  'stop_price': Orderinfo['stop_price'] / qcu,
                  }
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            notice_type = NOTICE_TYPE['REG_STOP_LIMIT']
            values.update({'price': Orderinfo['price'] / qcu, 'size': Orderinfo['size'] / bcu, 'bcc': bcc})
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET']:
            notice_type = NOTICE_TYPE['REG_STOP_MARKET']
            if Orderinfo['side'] == SIDE['BUY']:
                values.update({'amount': Orderinfo['funds'] / qcu, 'acc': qcc})
            else:  # SELL
                values.update({'amount': Orderinfo['size'] / bcu, 'acc': bcc})
        else:
            return
        notice_id = self.__sdb_notice_insert(member_id=Orderinfo['member_id'], notice_type=notice_type, values=values)
        notice = values.copy()
        notice['notice_id'] = notice_id
        notice['notice_type'] = notice_type
        notice['notice_time'] = DATETIME_UNIXTIMESTAMP()
        return notice

    def sdb_notice_cancel_stop(self, Orderinfo):
        bcc, qcc = Orderinfo['bcc'], Orderinfo['qcc']
        bcd, qcd = Orderinfo['bcd'], Orderinfo['qcd']
        bcu, qcu = float(10 ** bcd), float(10 ** qcd)
        values = {'product_name': '%s/%s' % (bcc, qcc),
                  'bcc': bcc, 'qcc': qcc,
                  'product': Orderinfo['product'], 'side': Orderinfo['side'],
                  'order_type': Orderinfo['order_type'],
                  'stop_price': Orderinfo['stop_price'] / qcu,
                  }
        if Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
            notice_type = NOTICE_TYPE['CANCEL_STOP_LIMIT']
            values.update({'price': Orderinfo['price'] / qcu, 'size': Orderinfo['size'] / bcu})
        elif Orderinfo['order_type'] == ORDER_TYPE['MARKET']:
            notice_type = NOTICE_TYPE['CANCEL_STOP_MARKET']
            if Orderinfo['side'] == SIDE['BUY']:
                values.update({'amount': Orderinfo['funds'] / qcu, 'acc': qcc})
            else:  # SELL
                values.update({'amount': Orderinfo['size'] / bcu, 'acc': bcc})
        else:
            return
        notice_id = self.__sdb_notice_insert(member_id=Orderinfo['member_id'], notice_type=notice_type, values=values)
        notice = values.copy()
        notice['notice_id'] = notice_id
        notice['notice_type'] = notice_type
        notice['notice_time'] = DATETIME_UNIXTIMESTAMP()
        return notice

    def OpenResponse(self, response):
        # self.cur.execute("SELECT order_id FROM `order:OPEN`;")
        self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
        self.log.w('green', 'SUCC', self.trx_id, 'LIMIT_REGIST', response)

    def DoneResponse(self, response):
        if response is not None:
            self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            self.log.w('green', 'SUCC', self.trx_id, 'LIMIT_REGIST', response)

    def MatchTakerResponse(self, Takerinfo, trade_id, notice):
        additional_data = {}
        if 'stop_price' in Takerinfo and Takerinfo['stop_price'] is not None:
            additional_data['stop_price'] = Takerinfo['stop_price']
        additional_data.update({'order_id': Takerinfo['order_id'], 'price': Takerinfo['trade_price'], 'size': Takerinfo['trade_size'], 'side': pSIDE[Takerinfo['side']], 'fee': Takerinfo['fee']['IN'],
                                'match_side': 'taker', 'trade_id': trade_id, 'notice': notice})
        if Takerinfo['order_type'] == ORDER_TYPE['MARKET'] and Takerinfo['side'] == SIDE['BUY']:
            additional_data['remaining_funds'] = Takerinfo['remaining_funds']
        else:
            additional_data['remaining_size'] = Takerinfo['remaining_size']
        response = self.ResponseOrder(response_code='MATCH', order_type=pORDER_TYPE[Takerinfo['order_type']], product=Takerinfo['product'], member_id=Takerinfo['member_id'],
                                      **additional_data)
        self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
        self.log.w('green', 'SUCC', self.trx_id, 'MATCH', response)

    def MatchMakerResponse(self, response_list):
        for response in response_list:
            self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
            self.log.w('green', 'SUCC', self.trx_id, 'MATCH', response)

    def MakerCancelResponse(self, response):
        self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], json.dumps(response))
        self.log.w('green', 'SUCC', self.trx_id, 'CANCEL', response)

    # def ModifyResponse(self, response):
    #     self.ResultQueue.put({'type': 'response', '_response': json.dumps(response), 'trx_id':self.trx_id})
    #     self.log.w('green', 'SUCC', self.trx_id, 'MODIFY', response)

    def __make_wallet_exchanges(self, Matchs):
        print("data from the redis send before calling walletcache =",Matchs)
        Takerinfo = {'trade_size': 0, 'trade_funds': 0, 'hold_amount': 0, 'fee': {'IN': 0, 'OUT': 0}, 'price': None}
        Makerinfos = list()
        Feeinfos = list()
        for match in Matchs:
            MakerOrder = match['MakerOrder']
            TakerOrder = match['TakerOrder']
            Tradeinfo = match['Tradeinfo']
            ## taker
            Takerinfo['trade_size'] += Tradeinfo['trade_size']
            Takerinfo['trade_funds'] += Tradeinfo['trade_funds']
            Takerinfo['fee']['IN'] += TakerOrder['fee']
            print("adding the Takerinfo[tradesize]",Takerinfo['trade_size'])
            print("adding the Takerinfo[tradesize]",Takerinfo['trade_funds'])
            print("adding the Takerinfo[tradesize]",Takerinfo['fee']['IN'])
            if TakerOrder['order_type'] == ORDER_TYPE['LIMIT']:
                Takerinfo['price'] = TakerOrder['price']
                if TakerOrder['side'] == SIDE['BUY']:
                    _hold_amount = (Tradeinfo['trade_size'] * TakerOrder['price']) / (10 ** (TakerOrder['bcd'] - R_DIGIT))
                    Takerinfo['hold_amount'] += int(round(_hold_amount, -R_DIGIT)) / (10 ** R_DIGIT)
                else:
                    Takerinfo['hold_amount'] += Tradeinfo['trade_size']
            else:
                Takerinfo['hold_amount'] = TakerOrder['hold_amount']

            ## maker
            Makerinfo = {'member_id': MakerOrder['member_id'],
                         'trade_id': Tradeinfo['trade_id'],
                         'order_id': MakerOrder['order_id'],
                         'trade_funds': Tradeinfo['trade_funds'],
                         'wallet_id': MakerOrder['wallet_id'],
                         'trade_size': Tradeinfo['trade_size'],
                         'side': MakerOrder['side'],
                         'trade_price': Tradeinfo['trade_price'],
                         'trade_time': Tradeinfo['trade_time'],
                         'fee': {'IN': MakerOrder['fee'], 'OUT': 0},
                         }
            if Makerinfo['side'] == SIDE['BUY']:
                _hold_amount = (Tradeinfo['trade_size'] * MakerOrder['price']) / (10 ** (MakerOrder['bcd'] - R_DIGIT))
                Makerinfo['hold_amount'] = int(round(_hold_amount, -R_DIGIT)) / (10 ** R_DIGIT)
            else:
                Makerinfo['hold_amount'] = Tradeinfo['trade_size']

            Makerinfos.append(Makerinfo)
            ## fee
            for Orderinfo in (TakerOrder, MakerOrder):
                if Orderinfo['fee'] > 0:
                    Feeinfo = {'order_id': Orderinfo['order_id'], 'trade_id': Tradeinfo['trade_id'], 'wallet_id': Orderinfo['wallet_id'],
                               'side': pSIDE[Orderinfo['side']], 'trade_time': Tradeinfo['trade_time'], 'fee': Orderinfo['fee']}
                    if Orderinfo['side'] == SIDE['BUY']:
                        Feeinfo.update({'currency_code': Orderinfo['bcc'], 'real_amount': Tradeinfo['trade_size'], 'currency_digit': Orderinfo['bcd'], })
                    else:
                        Feeinfo.update({'currency_code': Orderinfo['qcc'], 'real_amount': Tradeinfo['trade_funds'], 'currency_digit': Orderinfo['qcd'], })
                    Feeinfos.append(Feeinfo)

        _trade_price = (Takerinfo['trade_funds'] * 10 ** (TakerOrder['bcd'] + R_DIGIT)) / Takerinfo['trade_size']
        Takerinfo['trade_price'] = int(round(_trade_price, -R_DIGIT)) / (10 ** R_DIGIT)
        Takerinfo.update({'member_id': TakerOrder['member_id'],
                          'wallet_id': TakerOrder['wallet_id'],
                          'order_id': TakerOrder['order_id'],
                          'side': TakerOrder['side'],
                          'order_type': TakerOrder['order_type'],
                          'product': Tradeinfo['product'], 'bcc': TakerOrder['bcc'], 'qcc': TakerOrder['qcc'],
                          'trade_time': Tradeinfo['trade_time'],
                          'stoporder': TakerOrder['stoporder'],
                          'trx_id': TakerOrder['trx_id'],
                          })

        if TakerOrder['order_type'] == ORDER_TYPE['MARKET'] and TakerOrder['side'] == SIDE['BUY']:
            Takerinfo['remaining_funds'] = TakerOrder['remaining_funds']
        else:
            Takerinfo['remaining_size'] = TakerOrder['remaining_size']

        if TakerOrder['stoporder'] == 1:
            Takerinfo['stop_price'] = TakerOrder['stop_price']
        else:
            Takerinfo['stop_price'] = None
        if Takerinfo['side'] == SIDE['BUY']:
            Takerinfo['tbmq'] = 'IN'  # taker base / maker quote
            Takerinfo['tqmb'] = 'OUT'  # taker quote / maker base
        else:
            Takerinfo['tbmq'] = 'OUT'  # taker base / maker quote
            Takerinfo['tqmb'] = 'IN'  # taker quote / maker base
        return Takerinfo, Makerinfos, Feeinfos, Tradeinfo['trade_id']

    def MatchWalletExchanges(self, Takerinfo, Makerinfos, Feeinfos, trade_id):
        self.wdb.exchanges(Takerinfo=Takerinfo, Makerinfos=Makerinfos, Feeinfos=Feeinfos, trade_id=trade_id)
        self.processdb.lpush(self.PROCESSDBNAME['PROCESS_C'], json.dumps(Feeinfos))

    def MatchTradeDataProcess(self, RecvMsg):
        for match in RecvMsg['Match']:
            # TakerOrder = match['TakerOrder']
            # MakerOrder = match['MakerOrder']
            Tradeinfo = match['Tradeinfo']

            self.MatchingData_tradehistory(Tradeinfo=Tradeinfo)
            self.MatchingData_tradefunds(Tradeinfo=Tradeinfo)
            self.MatchingData_tradeprice(Tradeinfo=Tradeinfo)
            self.MatchingData_ohlc(Tradeinfo=Tradeinfo)

    def MatchingData_tradehistory(self, Tradeinfo):
        TRADEHISTORY_LIMIT = 50
        _RESPONSEDBNAME = {
            'TRADEHISTORY:PUBLISH': self.RESPONSEDBNAME['TRADEHISTORY:PUBLISH'].format(product_code=Tradeinfo['product']),
            'TRADEHISTORY:SNAPSHOT': self.RESPONSEDBNAME['TRADEHISTORY:SNAPSHOT'].format(product_code=Tradeinfo['product']),
        }
        data = {'trade_id': Tradeinfo['trade_id'], 'side': pSIDE[Tradeinfo['taker_side']], 'size': float(Tradeinfo['trade_size']) / Tradeinfo['bcu'],
                'price': float(Tradeinfo['trade_price']) / Tradeinfo['qcu'], 'product': Tradeinfo['product'],
                'time': Tradeinfo['trade_time'], 'sequence': Tradeinfo['sequence'], 'bcu': Tradeinfo['bcu'], 'qcu': Tradeinfo['qcu']}
        pipeline = self.responsedb.pipeline(transaction=True)
        pipeline.publish(_RESPONSEDBNAME['TRADEHISTORY:PUBLISH'], json.dumps(data))
        pipeline.zadd(_RESPONSEDBNAME['TRADEHISTORY:SNAPSHOT'], json.dumps(data), Tradeinfo['sequence'])
        pipeline.execute()
        zcount = self.responsedb.zcount(_RESPONSEDBNAME['TRADEHISTORY:SNAPSHOT'], 0, float('inf'))
        if zcount > TRADEHISTORY_LIMIT:
            self.responsedb.zremrangebyrank(_RESPONSEDBNAME['TRADEHISTORY:SNAPSHOT'], 0, zcount - (TRADEHISTORY_LIMIT + 1))

    def MatchingData_tradefunds(self, Tradeinfo):
        ''' 거래대금 '''
        trade_time = int(Tradeinfo['trade_time'] / 10 ** 6)
        trade_time -= trade_time % 3600
        self.responsedb.zincrby(self.RESPONSEDBNAME['TRADEFUNDS'].format(product_code=Tradeinfo['product']), trade_time, Tradeinfo['trade_funds'])
        self.responsedb.zadd(self.RESPONSEDBNAME['TRADEFUNDS:TIME'].format(product_code=Tradeinfo['product']), trade_time, trade_time)

        keys = self.responsedb.zrangebyscore(name=self.RESPONSEDBNAME['TRADEFUNDS:TIME'].format(product_code=Tradeinfo['product']), min=0, max=time.time() - 86400, start=0, num=100)
        transaction = self.responsedb.pipeline(transaction=True)
        for key in keys:
            transaction.zrem(self.RESPONSEDBNAME['TRADEFUNDS'].format(product_code=Tradeinfo['product']), key)
            transaction.zrem(self.RESPONSEDBNAME['TRADEFUNDS:TIME'].format(product_code=Tradeinfo['product']), key)
        transaction.execute()

    def MatchingData_tradeprice(self, Tradeinfo):
        self.responsedb.set(self.RESPONSEDBNAME['TRADEPRICE'].format(product_code=Tradeinfo['product']), Tradeinfo['trade_price'])

    def MatchingData_ohlc(self, Tradeinfo):
        data = {
            'product_code': Tradeinfo['product'],
            'trade_price': float(Tradeinfo['trade_price']) / Tradeinfo['qcu'],
            'trade_size': float(Tradeinfo['trade_size']) / Tradeinfo['bcu'],
            'time': Tradeinfo['trade_time'] / 10 ** 6,
        }
        self.OhlcQueue.put(data)


class OhlcProcessor(exchange):
    def __init__(self, log, OhlcQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.OhlcQueue = OhlcQueue

        self.storagedb_init()

        self.PERIODS = [('1M', 60),
                        ('5M', 300),
                        ('15M', 900),
                        ('30M', 1800),
                        ('1H', 3600),
                        ('4H', 14400),
                        ('1D', 86400),
                        ('1W', 604800),
                        ]
        # self.SQL = "INSERT INTO `txquote`.`OHLC:{product_code}:{period}` (`time`, `open`, `high`, `low`, `close`, `volume`) VALUES (%(time)s, %(price)s, %(price)s, %(price)s, %(price)s, %(size)s) " \
        #            "ON DUPLICATE KEY UPDATE `high`=GREATEST(`high`, %(price)s), `low`=LEAST(`low`, %(price)s), `close`=%(price)s, `volume`=`volume`+%(size)s;"
        self.SQL = "INSERT INTO `txquote`.`OHLC:{product_code}:{period}` (`time`, `open`, `high`, `low`, `close`, `volume`) VALUES (%(time)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s) " \
                   "ON DUPLICATE KEY UPDATE `high`=GREATEST(`high`, %(high)s), `low`=LEAST(`low`, %(low)s), `close`=%(close)s, `volume`=`volume`+%(volume)s;"

        self.CREATE_SQL = "CREATE TABLE `txquote`.`OHLC:{product_code}:{period}` (" \
                          "`time` INT(1) UNSIGNED NOT NULL, " \
                          "`open` DOUBLE UNSIGNED NOT NULL, " \
                          "`high` DOUBLE UNSIGNED NOT NULL, " \
                          "`low` DOUBLE UNSIGNED NOT NULL, " \
                          "`close` DOUBLE UNSIGNED NOT NULL, " \
                          "`volume` DOUBLE UNSIGNED NOT NULL DEFAULT '0', " \
                          "PRIMARY KEY (`time`)" \
                          ") ENGINE=INNODB DEFAULT CHARSET=utf8;"

        self.ohlc_buffer = dict()
        self.last_execute_time = time.time()

        while True:
            try:
                try:
                    msg = self.OhlcQueue.get(timeout=1)
                except Queue.Empty:
                    msg = None
                    pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                    if os.path.exists(pidkill):
                        os.remove(pidkill)
                        exit(0)

                if msg:
                    self.run(msg=msg)
            except Exception as err:
                self.log.w('red', 'ERROR', traceback.format_exc(err))
                exit(1)

    def run(self, msg):
        OHLC_UPDATE_INTERVAL = 1
        current_time = time.time()
        for period, seconds in self.PERIODS:
            ohlc_time = self.get_ohlc_time(timestamp=msg['time'], period=period, seconds=seconds)
            key = '{product_code}:{ohlc_time}:{period}'.format(product_code=msg['product_code'], ohlc_time=ohlc_time, period=period)
            try:
                ohlc = self.ohlc_buffer[key]
                ohlc['high'] = max((ohlc['high'], msg['trade_price']))
                ohlc['low'] = min((ohlc['low'], msg['trade_price']))
                ohlc['close'] = msg['trade_price']
                ohlc['volume'] += msg['trade_size']
                self.ohlc_buffer[key] = ohlc  # 이거 안해줘도 된다. 근데 찜찜해서 그냥 해줌
            except KeyError:
                self.ohlc_buffer[key] = {
                    'time': ohlc_time,
                    'open': msg['trade_price'],
                    'high': msg['trade_price'],
                    'low': msg['trade_price'],
                    'close': msg['trade_price'],
                    'volume': msg['trade_size'],
                    'period': period,
                }

        if current_time - self.last_execute_time >= OHLC_UPDATE_INTERVAL:
            if self.ohlc_buffer:
                self.storagedb_check()
                for key, kwargs in self.ohlc_buffer.items():
                    product_code, _, period = key.split(':')
                    sql = self.SQL.format(product_code=product_code, period=period)
                    try:
                        self.cur.execute(sql, kwargs)
                    except MySQLdb.ProgrammingError as err:
                        if err[0] == 1146:
                            self.cur.execute(self.CREATE_SQL.format(product_code=product_code, period=period))
                            self.cur.execute(sql, kwargs)
                        else:
                            raise MySQLdb.ProgrammingError(err)
                    STAT.send('OHLC.execute')

                self.ohlc_buffer = dict()
                self.last_execute_time = current_time
                self.con.commit()

    def get_ohlc_time(self, timestamp, period, seconds):
        if period == '1W':
            shift = 86400 * 3
        else:
            shift = 0
        timestamp += shift
        ohlc_time = int(timestamp) - (int(timestamp) % seconds)
        ohlc_time -= shift
        return ohlc_time


class Main(exchange):
    process_list = list()

    def multiprocess_starter(self, name, target, kwargs):
        p = multiprocessing.Process(target=target, kwargs=kwargs)
        p.daemon = True
        p.start()
        self.process_list.append({'name': name, 'process': p, 'pid': p.pid})
        self.log.w('gray', 'INIT', 'Running', name, p.pid)
        return p

    def __init__(self, log):
        self.CONFIG = CONFIG
        self.log = log
        self.log.w('gray', 'INIT', 'Running', 'Main', os.getpid())

        # Process간 통신용 Queue 생성
        DistributeQueue = multiprocessing.Queue()
        OhlcQueue = multiprocessing.Queue()

        # Distribute
        self.multiprocess_starter(name='DistributeProcessor', target=DistributeProcessor,
                                  kwargs={'log': self.log, 'DistributeQueue': DistributeQueue})
        self.multiprocess_starter(name='OhlcProcessor', target=OhlcProcessor,
                                  kwargs={'log': self.log, 'OhlcQueue': OhlcQueue})
        for _ in range(self.CONFIG.POSTPROCESS['processor']):
            self.multiprocess_starter(name='PostProcessor', target=PostProcessor,
                                      kwargs={'log': self.log, 'DistributeQueue': DistributeQueue, 'OhlcQueue': OhlcQueue})

        self.process_manager()
        while True:
            self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')

            # PID 관련
            with open(os.path.join(self.RUN_DIRECTORY, 'xbit-post.pid'), 'w') as f:
                f.write(str(os.getpid()))

            self.kill_format = '{pid}.kill'
            pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))
            if os.path.exists(pidkill):
                os.remove(pidkill)
                exit(0)

            time.sleep(1)

    def process_manager(self):
        self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
        if not os.path.exists(self.RUN_DIRECTORY):
            os.makedirs(self.RUN_DIRECTORY)

        self.kill_format = '{pid}.kill'
        pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))

        while True:
            # PID 관련
            with open(os.path.join(self.RUN_DIRECTORY, 'xbit-post.pid'), 'w') as f:
                f.write(str(os.getpid()))

            if os.path.exists(pidkill):
                # PreProcessDaemon 안전 종료 순서
                # 1. DistributeProcessor
                self.killer(name='DistributeProcessor')
                # 2. PostProcessor
                self.killer(name='PostProcessor')
                # 3. OhlcProcessor
                self.killer(name='OhlcProcessor')
                os.remove(pidkill)

            # subprocess가 모두 죽으면 master로 죽자
            for process in self.process_list:
                if process['process'].is_alive():
                    break
            else:
                exit(0)
            time.sleep(0.1)

    def killer(self, name):
        for process in [p for p in self.process_list if p['name'] == name]:
            with open(os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=process['pid'])), 'w') as f:
                pass

        do = True
        while do:
            for process in [p for p in self.process_list if p['name'] == name]:
                if not process['process'].is_alive():
                    do = False
            time.sleep(0.1)


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