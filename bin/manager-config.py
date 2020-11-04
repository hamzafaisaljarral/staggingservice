#!/bin/python
# -*- encoding:utf8 -*-
import os, sys, time, ConfigParser, optparse, glob

FILENAME = os.path.abspath(__file__)
FILE_DIRECTORY = os.path.dirname(FILENAME)
BASE_DIRECTORY = os.path.abspath(os.path.join(FILE_DIRECTORY, '..'))
sys.path.append(BASE_DIRECTORY)

config_sets = {
    'TradeFeeRate.ini': [
        ['GENERAL', [
            ('INTERVAL_SECONDS', 600),
        ]],
    ],
    'xbitProfitRank.ini': [
        ['GENERAL', [
            ('IGNORE_USERID', '%@tesnine.com', 'ProfitRank에서 LIKE문으로 랭킹제외되는 userid'),
            ('INTERVAL_SECONDS', 600),
        ]],
    ],
    'xbitOrdersend.ini': [
        ['HURST', [
            ('HURST_SENDONLY_USERID', 'x-bit.so%@tesnine.com'),
        ]],
        ['BTCBXB_NORMAL', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.15),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 1),
            ('max_price_gap_ratio', 1),
            ('min_size_ratio', 15),
            ('max_size_ratio', 25),
        ]],
        ['BTCBXB_EXPAND', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.5),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 5),
            ('max_price_gap_ratio', 16),
            ('min_size_ratio', 2),
            ('max_size_ratio', 2),
        ]],
        ['BTCETH_NORMAL', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.15),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 1),
            ('max_price_gap_ratio', 1),
            ('min_size_ratio', 25),
            ('max_size_ratio', 45),
        ]],
        ['BTCETH_EXPAND', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.5),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 5),
            ('max_price_gap_ratio', 16),
            ('min_size_ratio', 2),
            ('max_size_ratio', 2),
        ]],
        ['BXBUSD_NORMAL', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.15),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 1),
            ('max_price_gap_ratio', 1),
            ('min_size_ratio', 25),
            ('max_size_ratio', 45),
        ]],
        ['BXBUSD_EXPAND', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.5),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 5),
            ('max_price_gap_ratio', 16),
            ('min_size_ratio', 2),
            ('max_size_ratio', 2),
        ]],
        ['ETHBXB_NORMAL', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.15),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 1),
            ('max_price_gap_ratio', 1),
            ('min_size_ratio', 35),
            ('max_size_ratio', 55),
        ]],
        ['ETHBXB_EXPAND', [
            ('s_price', 0),
            ('order_per_second', 2),
            ('proba', 0.5),
            ('length', 100),
            ('max_lookback', 100),
            ('min_price_gap_ratio', 5),
            ('max_price_gap_ratio', 16),
            ('min_size_ratio', 2),
            ('max_size_ratio', 2),
        ]],
    ],
}

class customConfigParser(ConfigParser.ConfigParser):
    def write(self, fp):
        DEFAULTSECT = "DEFAULT"
        """Write an .ini-format representation of the configuration state."""
        if self._defaults:
            fp.write("[%s]\n" % DEFAULTSECT)
            for (key, value) in self._defaults.items():
                fp.write("%s = %s\n" % (key, str(value).replace('\n', '\n\t')))
            fp.write("\n")
        for section in self._sections:
            fp.write("[%s]\n" % section)
            for (key, value) in self._sections[section].items():
                if key == "__name__":
                    continue
                if key.strip()[0] == ';':
                    for row in key.strip().splitlines():
                        fp.write(";%s\n" % (row, ))
                    continue
                elif (value is not None) or (self._optcre == self.OPTCRE):
                    key = " = ".join((key, str(value).replace('\n', '\n\t')))
                fp.write("%s\n" % (key))
            fp.write("\n")

class Main:
    def __init__(self, options):
        global config_sets
        self.config_sets = config_sets
        self.options = options
        self.config_directory = os.path.join(BASE_DIRECTORY, 'config')

        self.general_config = dict()
        for filename in config_sets:
            _config = customConfigParser()
            for section, values in config_sets[filename]:
                _config.add_section(section)
                for row in values:
                    try:
                        key, value, comment = row
                    except ValueError:
                        key, value = row
                        comment = None
                    if comment is not None:
                        _config.set(section, '; ----- {key} ----- {comment}'.format(key=key, comment=comment))  # comment 있으면 넣기

                    _config.set(section, key, str(value))  # str 처리하지 않으면 error 발생
            self.general_config[filename] = _config

        # 지정된 옵션으로 실행하기
        if self.options.initialize:
            self.initialize()
        if self.options.update:
            self.update()
        if self.options.check:
            self.check()

    def initialize(self):
        '''
        기존에 있는 항목은 수정하지 않고, 신규로 추가된 항목에 대해서만 수정한다.
        description은 init할때만 입력함
        '''
        for filename in self.general_config:
            config_general = self.general_config[filename]
            filepath = os.path.join(self.config_directory, filename)
            if os.path.exists(filepath):
                config_file = ConfigParser.ConfigParser()
                config_file.read(filepath)

                for section in config_general.sections():
                    if config_file.has_section(section):  # section이 존재할때
                        for key in config_general.options(section):
                            if key not in config_file.options(section):
                                value = config_general.get(section, key)
                                config_file.set(section, key, value)
                    else:  # section이 존재 하지 않을때
                        config_file.add_section(section)
                        for key, value in config_general.items(section):
                            config_file.set(section, key, value)
                with open(filepath, 'w') as f:  # 파일 덮어쓰기
                    config_file.write(f)
            else:
                try:
                    os.makedirs(os.path.dirname(filepath))
                except OSError:  # 기존에 디렉토리가 있을때 무시하기
                    pass
                with open(filepath, 'w') as f:  # 파일 생성
                    config_general.write(f)

    def update(self):
        '''default항목에 없는데, 기존파일에 있는 항목을 제거한다.'''
        for filename in self.general_config:
            config_general = self.general_config[filename]
            filepath = os.path.join(self.config_directory, filename)
            if os.path.exists(filepath):
                config_file = customConfigParser()
                config_file.read(filepath)

                for section in config_file.sections():
                    if config_general.has_section(section):
                        for key in config_file.options(section):
                            if not config_general.has_option(section, key):
                                print 'remove option : {filename} {section} {option}'.format(filename=filename, section=section, option=key)
                                config_file.remove_option(section, key)
                    else:
                        print 'remove section : {filename} {section}'.format(filename=filename, section=section)
                        config_file.remove_section(section)

                with open(filepath, 'w') as f:
                    config_file.write(f)

            else:
                print 'not exists config [{filename}]'.format(filename=filename)

    def check_print(self, flag, filename, section=None, option=None, verbose=False):
        colors = {
            '=': '\x1b[0;32;40m',
            '+': '\x1b[0;33;40m',
            '-': '\x1b[0;31;40m',
        }
        assert flag in ('=', '+', '-')
        row = '{flag} {filename} >> {section} >> {option}'.format(flag=flag, filename=filename, section=section, option=option)
        if verbose:
            if self.options.verbose:
                if self.options.color:
                    row = '{color}{row}\x1b[0m'.format(color=colors[flag], row=row)
                print row
        else:
            if self.options.color:
                row = '{color}{row}\x1b[0m'.format(color=colors[flag], row=row)
            print row

    def check(self):
        '''기존파일과 default항목을 비교한다.'''

        ### 파일 비교 START
        filenames = list()
        filenames.extend(self.general_config.keys())
        for filename in [os.path.basename(_t) for _t in glob.glob(os.path.join(self.config_directory, '*.ini'))]:
            if not (filename in filenames):
                filenames.append(filename)

        for filename in filenames:
            try:
                config_general = self.general_config[filename]
                is_general_ini = True
            except KeyError:
                is_general_ini = False

            filepath = os.path.join(self.config_directory, filename)
            if os.path.exists(filepath):
                is_file_ini = True
            else:
                is_file_ini = False

            if is_general_ini and is_file_ini:
                self.check_print(flag='=', filename=filename, verbose=True)
            elif is_general_ini:  # general에 있는데 file에 존재하지 않음
                self.check_print(flag='-', filename=filename, verbose=False)
                continue
            elif is_file_ini:  # general에 없는데 file에 존재함
                self.check_print(flag='+', filename=filename, verbose=False)
                continue
            ### 파일 비교 END

            ### section 비교 START
            config_file = ConfigParser.ConfigParser()
            config_file.read(filepath)
            sections = list()
            sections.extend(config_general.sections())
            for section in config_file.sections():
                if not (section in sections):
                    sections.append(section)

            for section in sections:
                is_general_section = config_general.has_section(section)
                is_file_section = config_file.has_section(section)
                if is_general_section and is_file_section:
                    self.check_print(flag='=', filename=filename, section=section, verbose=True)
                elif is_general_section:  # general에 있는데 file에 존재하지 않음
                    self.check_print(flag='-', filename=filename, section=section, verbose=False)
                    continue
                elif is_file_section:  # general에 없는데 file에 존재함
                    self.check_print(flag='+', filename=filename, section=section, verbose=False)
                    continue
                ### section 비교 END

                ### option 비교 START
                options = list()
                options.extend(config_general.options(section))
                for option in config_file.options(section):
                    if not (option in options):
                        options.append(option)

                for option in options:
                    is_general_option = config_general.has_option(section, option)
                    is_file_option = config_file.has_option(section, option)
                    if is_general_option and is_file_option:
                        self.check_print(flag='=', filename=filename, section=section, option=option, verbose=True)
                    elif is_general_option:  # general에 있는데 file에 존재하지 않음
                        self.check_print(flag='-', filename=filename, section=section, option=option, verbose=False)
                        continue
                    elif is_file_option:  # general에 없는데 file에 존재함
                        self.check_print(flag='+', filename=filename, section=section, option=option, verbose=False)
                        continue
                ### option 비교 END


if __name__ == '__main__':
    parser = optparse.OptionParser(version='0.0.1')
    parser.add_option("--init", action='store_true', default=False,
                      help="initialize config files",
                      dest="initialize")
    parser.add_option("--update", action='store_true', default=False,
                      help="update config files",
                      dest="update")
    parser.add_option("--check", action='store_true', default=False,
                      help="check diff config files",
                      dest="check")
    parser.add_option("-v", "--verbose", action='store_true', default=False,
                      help="verbose",
                      dest="verbose")
    parser.add_option("-c", "--color", action='store_true', default=True,
                      help="color",
                      dest="color")
    options, args = parser.parse_args()
    if options.initialize or options.update or options.check:
        Main(options=options)
    else:
        parser.print_help()
