# -*- encoding:utf8 -*-
import datetime, time


class Log:
    '''
    white : normal
    gray : query
    yellow : debug
    green : receive
    purple : send
    red : error
    '''
    import os, sys, datetime
    def __init__(self, LoggingDirectory, DEBUG=False):
        self.LoggingDirectory = LoggingDirectory
        self.DEBUG = DEBUG
        self.format = '{time} {pid} {msg}'
        self.f_format = '%Y%m%d.log'

    def logfile(self):
        return self.os.path.join(self.LoggingDirectory, self.datetime.datetime.now().strftime(self.f_format))

    def __time(self, _format='%Y.%m.%d %H:%M:%S.%f'):
        return self.datetime.datetime.now().strftime(_format)

    def __pid(self):
        return self.os.getpid()

    def w(self, color, *args):
        logfilename = self.logfile()
        logdirname = self.os.path.dirname(logfilename)
        if not self.os.path.exists(logdirname):
            try:
                self.os.makedirs(logdirname)
            except OSError:
                pass

        msg = self.format.format(time=self.__time(), pid=self.__pid(), msg=' '.join([str(t) for t in args]))
        with open(logfilename, 'a') as f:
            f.write(msg + '\n')
            f.flush()
        if self.DEBUG:
            try:
                if color == 'white':  # default
                    self.sys.stdout.write('\x1b[00m\x1b[00m')
                elif color == 'gray':
                    self.sys.stdout.write('\x1b[00m\x1b[37m')
                elif color == 'yellow':
                    self.sys.stdout.write('\x1b[00m\x1b[33m')
                elif color == 'green':
                    self.sys.stdout.write('\x1b[00m\x1b[32m')
                elif color == 'purple':
                    self.sys.stdout.write('\x1b[00m\x1b[35m')
                elif color == 'red':
                    self.sys.stdout.write('\x1b[00m\x1b[31m')
                else:
                    self.sys.stdout.write('\x1b[00m\x1b[00m')
                self.sys.stdout.write(msg + '\n')
                self.sys.stdout.write('\x1b[00m\x1b[00m')
                self.sys.stdout.write('\x1b[00m\x1b[00m\x1b[0m')
                self.sys.stdout.flush()
            except Exception as err:
                pass


def DATETIME_UNIXTIMESTAMP(_datetime=None, i_unit=10 ** 6):
    if _datetime == None:
        _datetime = datetime.datetime.now()
    return long(time.mktime(_datetime.timetuple()) * i_unit + int(_datetime.strftime('%f')))


def UNIXTIMESTAMP_DATETIME(_unixtime, i_unit=10 ** 6):
    return datetime.datetime.fromtimestamp(float(_unixtime) / i_unit)


def cursor2dict(cursor, values, wrap_func=None):
    if values:
        fieldnames = [t[0] for t in cursor.description]
        if type(values[0]) == tuple:  # fetchall
            RESULT = list()
            for row in values:
                returnItem = dict()
                for i in range(len(fieldnames)):
                    if wrap_func:
                        returnItem[fieldnames[i]] = wrap_func(row[i])
                    else:
                        returnItem[fieldnames[i]] = row[i]
                RESULT.append(returnItem)
        else:  # fetchone
            RESULT = dict()
            for i in range(len(fieldnames)):
                if wrap_func:
                    RESULT[fieldnames[i]] = wrap_func(values[i])
                else:
                    RESULT[fieldnames[i]] = values[i]
        return RESULT
    else:
        return values
