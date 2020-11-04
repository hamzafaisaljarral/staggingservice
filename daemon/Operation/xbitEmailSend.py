# -*- encoding:utf8 -*-
import os, sys, time, optparse, smtplib, datetime, traceback, json, random
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

import MySQLdb, redis
from tendo import singleton
from boto.s3.connection import S3Connection
from boto.s3.key import Key as S3Key

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..', '..'))
sys.path.append(BASE_DIRECTORY)

from lib.define import *
from lib.exchange import exchange, exchangeConfigParser
from lib.function import *

### CONFIG PARSERING ###
config_file = os.path.join(BASE_DIRECTORY, 'settings', 'settings.ini')
config_arguments = {
    'SITE': [
        {'key': 'name', 'method': 'get', 'defaultvalue': None},
        {'key': 'url', 'method': 'get', 'defaultvalue': None},
    ],
    'LOG': [
        {'key': 'directory', 'method': 'get', 'defaultvalue': '/data/logs'}
    ],
    'REQUESTDB': [
        {'key': 'host', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'db', 'method': 'getint', 'defaultvalue': 0},
       # {'key': 'password', 'method': 'get', 'defaultvalue': None},
    ],
    'SMTP': [
        {'key': 'server', 'method': 'get', 'defaultvalue': None},
        {'key': 'port', 'method': 'getint', 'defaultvalue': None},
        {'key': 'auth_id', 'method': 'get', 'defaultvalue': None},
        {'key': 'auth_pw', 'method': 'get', 'defaultvalue': None},
    ],
    'S3_BOARD': [
        {'key': 'aws_bucket', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_access_key_id', 'method': 'get', 'defaultvalue': None},
        {'key': 'aws_secret_access_key', 'method': 'get', 'defaultvalue': None},
    ]
}
CONFIG = exchangeConfigParser(config_file, config_arguments)
if not CONFIG.isvalid:
    print CONFIG.errormsg
    exit(1)


class EmailValidCheck:
    @classmethod
    def verify(cls, _RecvMsg):
        # json 확인
        try:
            RecvMsg = json.loads(_RecvMsg)
        except ValueError:
            return False, 'No JSON object could be decoded'

        # emailtype key 확인
        try:
            emailtype = RecvMsg['emailtype']
        except KeyError:
            return False, 'Not Exists `emailtype`'

        # emailtype 가능 여부 확인
        try:
            emailtypeString = pEMAILTYPE[emailtype]
        except KeyError:
            return False, 'Not Allow `emailtype` : {}'.format(emailtype)

        ValidSet = {
            'Custom': ['subject', 'from_addr', 'to_addr', 'body'],
            'EmailVerification': ['member_id', 'userid', 'token', 'expired_time', 'code'],
            'PasswordReset': ['member_id', 'userid', 'token', 'expired_time'],
            'DepositComplete': ['member_id', 'userid', 'currency_code', 'amount'],
            'WithdrawRequest': ['wuuid', 'member_id', 'userid', 'currency_code', 'currency_digit', 'amount', 'fee_amount', 'req_time', 'address', 'addr_nickname', 'req_ip'],
            'WithdrawCancel': ['wuuid', 'member_id', 'userid', 'currency_code', 'currency_digit', 'amount', 'fee_amount', 'req_time', 'done_time', 'address', 'addr_nickname', 'req_ip'],
            'WithdrawDeny': ['wuuid', 'member_id', 'userid', 'currency_code', 'currency_digit', 'address', 'addr_nickname', 'amount', 'fee_amount', 'req_time', 'done_time', 'description'],
            'WithdrawComplete': ['wuuid', 'member_id', 'userid', 'currency_code', 'currency_digit', 'address', 'addr_nickname', 'amount', 'fee_amount', 'req_time', 'done_time', 'txid'],
            'WithdrawFail': ['wuuid', 'member_id', 'userid', 'currency_code', 'currency_digit', 'address', 'addr_nickname', 'amount', 'fee_amount', 'req_time', 'done_time', 'description'],
            'QnaAnswer': ['category', 'question', 'answer', 'userid', 'reg_date', 'done_date', 'reg_ip', 'reg_device', 'member_id', 'attachments'],
        }

        try:
            ValidSet[emailtypeString]
        except KeyError:
            return False, 'Not Implemented `emailtype` : {}'.format(emailtypeString)

        # 필요 key 확인 및 unicode encoding
        msg = {'emailtype': emailtype, 'values': dict()}
        for key in ValidSet[emailtypeString]:
            try:
                value = RecvMsg['values'][key]
            except KeyError:
                return False, 'KeyError `{key}`'.format(key=key)

            if type(value) == unicode:
                msg['values'][key] = value.encode('utf8')
            else:
                msg['values'][key] = value

        return True, msg


class Main(exchange, EmailValidCheck):
    def __init__(self, log):
        global CONFIG
        self.CONFIG = CONFIG
        self.log = log

        self.requestdb_init()
        self.smtp_connect()

        self.msgidentifier = long(int(random.random() * 10 ** 8))
        while True:
            try:
                Recv = self.requestdb.brpop('SEND:EMAIL', timeout=1)  # timeout발생시 None이 return되기때문에 Recv로 None를 받아서 처리
                if Recv:
                    self.msgidentifier += 1
                    _RecvName, _RecvMsg = Recv
                    self.log.w('yellow', 'RECV', self.msgidentifier, _RecvMsg)
                    self.run(_RecvMsg=_RecvMsg)
                else:
                    try:
                        self.smtp.ehlo()
                    except smtplib.SMTPServerDisconnected:
                        del self.smtp
                        self.smtp_connect()

                self.RUN_DIRECTORY = os.path.join(BASE_DIRECTORY, 'run')
                if not os.path.exists(self.RUN_DIRECTORY):
                    os.makedirs(self.RUN_DIRECTORY)

                ### PID 관련
                with open(os.path.join(self.RUN_DIRECTORY, 'xbit-emailsend.pid'), 'w') as f:
                    f.write(str(os.getpid()))

                self.kill_format = '{pid}.kill'
                pidkill = os.path.join(BASE_DIRECTORY, 'run', self.kill_format.format(pid=os.getpid()))
                if os.path.exists(pidkill):
                    os.remove(pidkill)
                    exit(0)

            except Exception as err:
                if log.DEBUG:
                    self.log.w('red', 'ERROR', traceback.format_exc(err))
                else:
                    self.log.w('red', 'ERROR', repr(traceback.format_exc(err)))
                exit(1)

    def __del__(self):
        try:
            self.smtp.close()
        except Exception:
            pass

    def smtp_connect(self):
        self.smtp = smtplib.SMTP(self.CONFIG.SMTP['server'], self.CONFIG.SMTP['port'])
        self.smtp.ehlo()
        self.smtp.starttls()
        self.smtp.ehlo()
        self.smtp.login(self.CONFIG.SMTP['auth_id'], self.CONFIG.SMTP['auth_pw'])

    def run(self, _RecvMsg):
        r, msg = EmailValidCheck.verify(_RecvMsg=_RecvMsg)
        if not r:
            self.log.w('red', 'FAIL', self.msgidentifier, msg)
            return
        if msg['emailtype'] == EMAILTYPE['Custom']:
            subject = msg['values']['subject']
            from_addr = msg['values']['from_addr']
            to_addrs = msg['values']['to_addr']
            body = msg['values']['body']

            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr)

        elif msg['emailtype'] == EMAILTYPE['EmailVerification']:
            subject = '[{site_name}] Email Verification'.format(site_name=self.CONFIG.SITE['name'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            url_link = '{baseurl}/emailcert?token={token}&code={code}'.format(baseurl=self.CONFIG.SITE['url'], token=msg['values']['token'], code=msg['values']['code'])
            with open(os.path.join(BASE_DIRECTORY, 'template', 'EmailVerification.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    expired_time=msg['values']['expired_time'],
                    url_link=url_link,
                    code=msg['values']['code'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])

        elif msg['emailtype'] == EMAILTYPE['PasswordReset']:
            subject = '[{site_name}] Password Reset Request'.format(site_name=self.CONFIG.SITE['name'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            url_link = '{baseurl}/passreset?token={token}'.format(baseurl=self.CONFIG.SITE['url'], token=msg['values']['token'])
            qna_link = '{baseurl}/support/qna'.format(baseurl=self.CONFIG.SITE['url'])
            with open(os.path.join(BASE_DIRECTORY, 'template', 'PasswordReset.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    expired_time=msg['values']['expired_time'],
                    url_link=url_link,
                    qna_link=qna_link,
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])
#updated deposit complete change this part 1
        elif msg['emailtype'] == EMAILTYPE['DepositComplete']:
            subject = '[{site_name}] {currency_code} Deposit is complete'.format(site_name=self.CONFIG.SITE['name'], currency_code=msg['values']['currency_code'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            with open(os.path.join(BASE_DIRECTORY, 'template', 'DepositComplete.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    site_url=self.CONFIG.SITE['url'],
                    currency_code=msg['values']['currency_code'],
                    done_time=datetime.datetime.fromtimestamp(msg['values']['done_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    #amount=float(msg['values']['amount']) / 10 ** msg['values']['currency_digit'],
                    amount=msg['values']['amount'], 
                    txid=msg['values']['txid'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])

        elif msg['emailtype'] == EMAILTYPE['WithdrawRequest']:
            subject = '[{site_name}] {currency_code} Withdrawal request is submitted'.format(site_name=self.CONFIG.SITE['name'], currency_code=msg['values']['currency_code'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            with open(os.path.join(BASE_DIRECTORY, 'template', 'WithdrawRequest.html')) as f:

                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    site_url=self.CONFIG.SITE['url'],
                    currency_code=msg['values']['currency_code'],
                    req_time=datetime.datetime.fromtimestamp(msg['values']['req_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    amount=float(msg['values']['amount']),
                    fee_amount=float(msg['values']['fee_amount']) / 10 ** msg['values']['currency_digit'],
                    addr_nickname=msg['values']['addr_nickname'],
                    address=msg['values']['address'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])

        elif msg['emailtype'] == EMAILTYPE['WithdrawCancel']:
            subject = '[{site_name}] {currency_code} Withdrawal is canceled'.format(site_name=self.CONFIG.SITE['name'], currency_code=msg['values']['currency_code'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            with open(os.path.join(BASE_DIRECTORY, 'template', 'WithdrawCancel.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    site_url=self.CONFIG.SITE['url'],
                    currency_code=msg['values']['currency_code'],
                    req_time=datetime.datetime.fromtimestamp(msg['values']['req_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    done_time=datetime.datetime.fromtimestamp(msg['values']['done_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    amount=float(msg['values']['amount']) / 10 ** msg['values']['currency_digit'],
                    fee_amount=float(msg['values']['fee_amount']) / 10 ** msg['values']['currency_digit'],
                    addr_nickname=msg['values']['addr_nickname'],
                    address=msg['values']['address'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])

        elif msg['emailtype'] in (EMAILTYPE['WithdrawDeny'], EMAILTYPE['WithdrawFail']):
            subject = '[{site_name}] {currency_code} withdrawal request is rejected'.format(site_name=self.CONFIG.SITE['name'], currency_code=msg['values']['currency_code'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            with open(os.path.join(BASE_DIRECTORY, 'template', 'WithdrawDeny.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    site_url=self.CONFIG.SITE['url'],
                    currency_code=msg['values']['currency_code'],
                    req_time=datetime.datetime.fromtimestamp(msg['values']['req_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    amount=msg['values']['amount'],
                    fee_amount=float(msg['values']['fee_amount']) / 10 ** msg['values']['currency_digit'],
                    addr_nickname=msg['values']['addr_nickname'],
                    address=msg['values']['address'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])
#updated this one amount 
        elif msg['emailtype'] == EMAILTYPE['WithdrawComplete']:
            subject = '[{site_name}] {currency_code} Withdrawal is complete'.format(site_name=self.CONFIG.SITE['name'], currency_code=msg['values']['currency_code'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            with open(os.path.join(BASE_DIRECTORY, 'template', 'WithdrawComplete.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    site_url=self.CONFIG.SITE['url'],
                    currency_code=msg['values']['currency_code'],
                    req_time=datetime.datetime.fromtimestamp(msg['values']['req_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    done_time=datetime.datetime.fromtimestamp(msg['values']['done_time']).strftime('%Y-%m-%d %H:%M:%S'),
                    amount=float(msg['values']['amount']),
                    fee_amount=float(msg['values']['fee_amount']) / 10 ** msg['values']['currency_digit'],
                    addr_nickname=msg['values']['addr_nickname'],
                    address=msg['values']['address'],
                    txid=msg['values']['txid'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])

        elif msg['emailtype'] == EMAILTYPE['QnaAnswer']:
            subject = '[{site_name}] Qna Answer'.format(site_name=self.CONFIG.SITE['name'])
            from_addr = 'test@bsbex.net'
            to_addrs = msg['values']['userid']

            with open(os.path.join(BASE_DIRECTORY, 'template', 'QnaAnswer.html')) as f:
                body = f.read().format(
                    site_name=self.CONFIG.SITE['name'],
                    site_url=self.CONFIG.SITE['url'],
                    category=msg['values']['category'],
                    question=msg['values']['question'],
                    answer=msg['values']['answer'],
                    userid=msg['values']['userid'],
                    reg_date=datetime.datetime.fromtimestamp(msg['values']['reg_date']).strftime('%Y-%m-%d %H:%M:%S'),
                    done_date=datetime.datetime.fromtimestamp(msg['values']['done_date']).strftime('%Y-%m-%d %H:%M:%S'),
                    reg_ip=msg['values']['reg_ip'],
                    reg_device=msg['values']['reg_device'],
                    member_id=msg['values']['member_id'],
                    attachments=msg['values']['attachments'],
                )
            mime = self.createMIME(subject=subject, body=body, to_addrs=to_addrs, from_addr=from_addr, from_name=self.CONFIG.SITE['name'])

            if msg['values']['attachments']:
                s3 = S3Connection(aws_access_key_id=CONFIG.S3_BOARD['aws_access_key_id'], aws_secret_access_key=CONFIG.S3_BOARD['aws_secret_access_key'])
                s3_bucket = s3.get_bucket(bucket_name=CONFIG.S3_BOARD['aws_bucket'])
                for attachment in msg['values']['attachments']:
                    s3file = s3_bucket.get_key(attachment['s3key'])
                    if s3file:
                        part = MIMEApplication(s3file.read(), Name=attachment['file_name'])
                        part['Content-Disposition'] = 'attachment; filename="{}"'.format(attachment['file_name'])
                        mime.attach(part)
                s3.close()
        else:
            raise ValueError('UNKNOWN')

        self.smtp.sendmail(from_addr=from_addr, to_addrs=to_addrs, msg=mime.as_string())
        self.log.w('green', 'SUCC', self.msgidentifier)

    def createMIME(self, subject, body, to_addrs, from_addr, from_name=None):
        mime = MIMEMultipart()
        mime['Subject'] = subject
        mime['To'] = to_addrs
        if from_addr is None:
            mime['From'] = from_addr
        else:
            mime['From'] = formataddr((from_name, from_addr))
        mime.attach(MIMEText(_text=body, _subtype='html', _charset='utf-8'))
        return mime


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
    try:
        main = Main(log=log)
    except KeyboardInterrupt:
        print 'KeyboardInterrupt'
        exit(1)
