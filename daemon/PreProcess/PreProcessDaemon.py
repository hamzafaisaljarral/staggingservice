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
from lib.exchange import exchange, OrderValidCheck, OrderConversion, Response, exchangeConfigParser, WalletDB

### CONFIG PARSERING ###
config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'}
    ],
    'PREPROCESS': [
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
    'WALLETDB': [
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


class STAT:
    con = None

    @classmethod
    def send(cls, name):
        if cls.con is None:
            cls.con = redis.Redis(host=CONFIG.REQUESTDB['host'], port=CONFIG.REQUESTDB['port'], db=0)
        msg = json.dumps({'name': name, 'time': int(time.time())})
        cls.con.publish('STAT', msg)


class DistributeRun(exchange):
    ''' 분산용 Process '''
    #print("inside DistributeRun") 
    def __init__(self, log, DistributeQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.DistributeQueue = DistributeQueue
        self.requestdb_init()

        while True:
            try:
                while not self.DistributeQueue.empty():
                    pass
                Recv = self.requestdb.brpop(self.REQUESTDBNAME['ORDERSEND'], timeout=1)  # timeout발생시 None이 return되기때문에 Recv로 None를 받아서 처리
                if Recv:
                    _RecvName, _RecvMsg = Recv
                    self.trx_id = str(uuid.uuid4())
                    self.log.w('purple', 'RECV', self.trx_id, _RecvName, _RecvMsg)
                    self.run(_RecvName=_RecvName, _RecvMsg=_RecvMsg)

                pidkill = os.path.join(BASE_DIRECTORY, 'run', '{pid}.kill'.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            except Exception as err:
                self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                exit(1)

    def run(self, _RecvName, _RecvMsg):
        assert type(json.loads(_RecvMsg)) == dict
        self.currency_check()
        self.product_check()
        r=self.DistributeQueue.put({'_RecvMsg': _RecvMsg, 'trx_id': self.trx_id, 'currencys': self.currencys, 'products': self.products})
        #print(_RecvMsg,self.trx_id,self.currencys,self.products)
        #print(r)


class ResultRun(exchange):
    ''' 결과용 Process '''
    #print("result run ")

    def __init__(self, log, ResultQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.ResultQueue = ResultQueue
        self.processdb_init()
        self.responsedb_init()

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
                    self.run(msg)

            except Exception as err:
                self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                exit(1)

    def run(self, msg):
        #print("inside second run")
        if msg['type'] == 'response':
            #print("inside message")
            self.responsedb.publish(self.RESPONSEDBNAME['RESPONSE'], msg['_response'])
        elif msg['type'] == 'process_a':
            #print("inside process_a push")
            self.processdb.lpush(self.PROCESSDBNAME['PROCESS_A'].format(product_code=msg['product_code']), str((self.trx_id, msg['data'])))
        else:
            raise ValueError('unknown')


class PreProcess(exchange):
    def __init__(self, log, DistributeQueue, ResultQueue):
        self.CONFIG = CONFIG
        self.log = log
        self.DistributeQueue = DistributeQueue
        self.ResultQueue = ResultQueue
        self.wdb = WalletDB(WALLETDB=self.CONFIG.WALLETDB)

        self.requestdb_init()
        #print("inside the preProcess")
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
                    self.run(_RecvMsg=msg['_RecvMsg'], currencys=msg['currencys'], products=msg['products'])
                    STAT.send('PreProcess')

            except Exception as err:
                if DEBUG:
                    self.log.w('red', 'ERROR', self.trx_id, traceback.format_exc(err))
                else:
                    self.log.w('red', 'ERROR', self.trx_id, repr(traceback.format_exc(err)))
                exit(1)

    def run(self, _RecvMsg, currencys, products):
        ### json parsing & Check
        #print("inside 3rd run")
        try:
            RecvMsg = json.loads(_RecvMsg)
            RecvMsg['sequence'] = self.requestdb.incr(self.REQUESTDBNAME['SEQUENCE:ORDERSEND'])
            #print(RecvMsg['sequence'])
            RecvMsg['trx_id'] = self.trx_id
            #print(RecvMsg['trx_id'])
            if type(RecvMsg) != dict:
                raise ValueError()
        except ValueError:
            response = Response.JSON_ERROR(recvmsg=_RecvMsg)
            self.ResultQueue.put({'type': 'response', '_response': json.dumps(response), 'trx_id': self.trx_id})
            self.log.w('red', 'FAIL', self.trx_id, 'JSON_ERROR', response)
            return

        ### ValidCheck
        validcheck = OrderValidCheck(RecvMsg=RecvMsg)
        if validcheck.isvalid:
            ValidRecvMsg = validcheck.RecvMsg
        else:
            response = Response.INVALID_PARAM(recvmsg=_RecvMsg)
            self.ResultQueue.put({'type': 'response',
                                  '_response': json.dumps(response),
                                  'trx_id': self.trx_id})
            self.log.w('red', 'FAIL', self.trx_id, 'INVALID_PARAM', response)
            return

        ### Product 확인하기
        if not (ValidRecvMsg['product'] in products.keys()):
            response = Response.INVALID_PARAM(recvmsg=_RecvMsg)
            self.ResultQueue.put({'type': 'response',
                                  '_response': json.dumps(response),
                                  'trx_id': self.trx_id})
            self.log.w('red', 'FAIL', self.trx_id, 'INVALID_PARAM', response)
            return

        ### Conversion
        conversion = OrderConversion(currencys=currencys,
                                     products=products,
                                     RecvMsg=ValidRecvMsg)

        ### trade_funds 확인
        is_NOT_ALLOW_ORDER = False
        #print("inside trade funds")
        if conversion.data['order_type'] == ORDER_TYPE['LIMIT']:
            #print("inside first if")
            Orderinfo = conversion.data
            #print(Orderinfo['order_type'])
            if (Orderinfo['price'] * Orderinfo['size']) / Orderinfo['bcu'] < 1:
                #print("inside second if")  # 1은 quote currency의 최소 단위를 의미함
                is_NOT_ALLOW_ORDER = True
                response = Response.NOT_ALLOW_ORDER_LIMIT(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], side=Orderinfo['side'], price=Orderinfo['price'], size=Orderinfo['size'])
                #print("trade funds response",response)

        elif conversion.data['order_type'] == ORDER_TYPE['STOP']:
            #print("inside orderstop")
            Stopinfo = conversion.data
            Orderinfo = Stopinfo['Orderinfo']
            if Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['BUY']:
                size = (Orderinfo['remaining_funds'] * Orderinfo['bcu']) / Orderinfo['stop_price']  # taker.remaining_funds / maker.price
                if (Orderinfo['stop_price'] * size) / Orderinfo['bcu'] < 1:
                    is_NOT_ALLOW_ORDER = True
                    response = Response.NOT_ALLOW_ORDER_STOP_MARKET_BUY(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], funds=Orderinfo['funds'])

            elif Orderinfo['order_type'] == ORDER_TYPE['MARKET'] and Orderinfo['side'] == SIDE['SELL']:
                #print("second elif")
                if (Orderinfo['stop_price'] * Orderinfo['size']) / Orderinfo['bcu'] < 1:
                    is_NOT_ALLOW_ORDER = True
                    response = Response.NOT_ALLOW_ORDER_STOP_MARKET_SELL(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], size=Orderinfo['size'])

            elif Orderinfo['order_type'] == ORDER_TYPE['LIMIT']:
                #print("3rd order send")
                if (Orderinfo['price'] * Orderinfo['size']) / Orderinfo['bcu'] < 1:
                    is_NOT_ALLOW_ORDER = True
                    response = Response.NOT_ALLOW_ORDER_STOP_LIMIT(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], stop_price=Orderinfo['stop_price'], side=Orderinfo['side'], price=Orderinfo['price'],
                                                                   size=Orderinfo['size'])

        if is_NOT_ALLOW_ORDER:
            #print("not allow order")
            self.ResultQueue.put({'type': 'response',
                                  '_response': json.dumps(response),
                                  'trx_id': self.trx_id})
            self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ALLOW_ORDER', response)
            return

        ### holdup
        is_holdup = False
        if conversion.data['order_type'] in (ORDER_TYPE['MARKET'], ORDER_TYPE['LIMIT']):
            is_holdup = True
            Orderinfo = conversion.data
            #print('printing holdup orderinfo',Orderinfo)
        elif conversion.data['order_type'] == ORDER_TYPE['STOP']:
            is_holdup = True
            Stopinfo = conversion.data
            Orderinfo = Stopinfo['Orderinfo']
        if is_holdup:
            self.wdb.healthcheck()
            result, description = self.wdb.holdup(wallet_id=Orderinfo['wallet_id'], currency_code=Orderinfo['hold_currency_code'], amount=Orderinfo['hold_amount'])
            print("result coming from holdup",result)
            print("result for the description",description)
            if not result:
                if conversion.data['order_type'] in (ORDER_TYPE['MARKET'], ORDER_TYPE['LIMIT']):
                    Orderinfo = conversion.data
                    response = Response.NOT_ENOUGH_BALANCE(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], order_type=Orderinfo['order_type'], order_id=Orderinfo['order_id'],
                                                           side=Orderinfo['side'], hold_amount=Orderinfo['hold_amount'], hold_currency_code=Orderinfo['hold_currency_code'])
                elif conversion.data['order_type'] == ORDER_TYPE['STOP']:
                    Stopinfo = conversion.data
                    Orderinfo = Stopinfo['Orderinfo']
                    response = Response.NOT_ENOUGH_BALANCE_STOP(member_id=Orderinfo['member_id'], product_code=Orderinfo['product'], order_id=Orderinfo['order_id'],
                                                                side=Orderinfo['side'], hold_amount=Orderinfo['hold_amount'], hold_currency_code=Orderinfo['hold_currency_code'],
                                                                order_type2=Orderinfo['order_type'])
                else:
                    raise ValueError('UNKNOWN')
                self.ResultQueue.put({'type': 'response',
                                      '_response': json.dumps(response),
                                      'trx_id': self.trx_id})
                self.log.w('yellow', 'FAIL', self.trx_id, 'NOT_ENOUGH_BALANCE', response)
                return

        # MatchingProcess로 넘기기
        self.ResultQueue.put({'type': 'process_a',
                              'product_code': conversion.data['product'],
                              'data': json.dumps(conversion.data),
                              'trx_id': self.trx_id})
        #print("put resultQeue")                      
        self.log.w('green', 'SUCC', self.trx_id, 'PROCESS_A', json.dumps(conversion.data))
        return


class Main(exchange):
    process_list = list()

    def multiprocess_starter(self, name, target, kwargs):
        p = multiprocessing.Process(target=target, kwargs=kwargs)
        p.daemon = True
        p.start()
        self.process_list.append({'name': name, 'process': p, 'pid': p.pid})
        self.log.w('gray', 'INIT', 'Running', name, p.pid)

    def __init__(self, log):
        self.CONFIG = CONFIG
        self.log = log

        # Process간 통신용 Queue 생성
        self.DistributeQueue = multiprocessing.Queue()
        self.ResultQueue = multiprocessing.Queue()

        # Distribute
        self.multiprocess_starter(name='DistributeRun', target=DistributeRun, kwargs={'log': self.log, 'DistributeQueue': self.DistributeQueue})
        self.multiprocess_starter(name='ResultRun', target=ResultRun, kwargs={'log': self.log, 'ResultQueue': self.ResultQueue})
        for _ in range(self.CONFIG.PREPROCESS['processor']):
            self.multiprocess_starter(name='PreProcess', target=PreProcess, kwargs={'log': self.log, 'DistributeQueue': self.DistributeQueue, 'ResultQueue': self.ResultQueue})

        self.process_manager()

    def process_manager(self):
        self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
        if not os.path.exists(self.RUN_DIRECTORY):
            os.makedirs(self.RUN_DIRECTORY)

        self.kill_format = '{pid}.kill'
        pidkill = os.path.join(self.RUN_DIRECTORY, self.kill_format.format(pid=os.getpid()))

        while True:
            # PID 관련
            with open(os.path.join(self.RUN_DIRECTORY, 'xbit-pre.pid'), 'w') as f:
                f.write(str(os.getpid()))

            if os.path.exists(pidkill):
                # PreProcessDaemon 안전 종료 순서
                # 1. DistributeRun
                self.killer(name='DistributeRun')
                # 2. PreProcess
                self.killer(name='PreProcess')
                # 3. ResultRun
                self.killer(name='ResultRun')
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
    print(LoggingDirectory)
    log = Log(LoggingDirectory=LoggingDirectory, DEBUG=DEBUG)
    log.f_format = '%Y%m%d_%H.log'
    log.w('gray', 'LoggingDirectory', log.LoggingDirectory)

    # main
    me = singleton.SingleInstance()
    main = Main(log=log)
