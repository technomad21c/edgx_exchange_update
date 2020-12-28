import cx_Oracle
import requests
import json
import argparse
import sys

class SecMasterInvoker():
    def __init__(self, server_uri):
        self.server_uri = server_uri.rstrip('/')
    def get_symbol(self, symbol_name):
        symbol = {}
        symbol['symbol'] = symbol_name
        if symbol_name[0] == '^':
            return symbol, False

        endpoint = self.server_uri + '/symbols/' + symbol_name.strip()
        response = requests.get(endpoint)

        if response.ok:
            data = json.loads(response.content)
            symbol['shortname'] = data['shortName']
            symbol['longname'] = data['longName']
            return symbol, True
        else:
            return symbol, False

    def get_exchange_symbols(self, exchange):
        symbols = []
        endpoint = options.secmaster.rstrip('/') + '/exchanges/' + exchange + '/symbols'
        response = requests.get(endpoint)
        if response.ok:
            data = json.loads(response.content)['_embedded']
            _symbols = data['symbols']
            for symbol in _symbols:
                if symbol['excode'] == exchange:
                    symbols.append(symbol)
                else:
                    print(symbol['symbol'])

        print('The number of symbols retrieved from Sec Master: ' + str(len(self.symbols)))
        return symbols

class SymbolDB():
    def __init__(self, host, port, sid, username, password):
        self.dsn = cx_Oracle.makedsn(host, port, sid)
        self.username = username
        self.password = password
        self.connection = None
        self.cur = None
        self.encoding = 'UTF-8'

    def connect(self, query_session):
        try:
            self.connection = cx_Oracle.connect(
                self.username,
                self.password,
                self.dsn,
                encoding=self.encoding)

        except cx_Oracle.Error as error:
            print(error)

        self.cur = self.connection.cursor()
        self.cur.execute(query_session)

    def read(self, query_select):
        symbols = []
        self.cur.execute(query_select)
        for symbol in self.cur:
            symbols.append(symbol[0])

        return symbols

    def update(self, query_update):
        self.cur.execute(query_update)
        self.connection.commit()

    def close(self):
        if self.cur:
            self.cur.close()
        if self.connection:
            self.connection.close()

def getOptions(args=sys.argv[1:]):
    parser = argparse.ArgumentParser(description="Parse command")
    parser.add_argument("-u", "--username", help="user id", required=True)
    parser.add_argument("-p", "--password", help="user password", required=True)
    parser.add_argument("-d", "--dbserver", help="DB server", required=True)
    parser.add_argument("-t", "--port", help="DB port", required=True)
    parser.add_argument("-s", "--sid", help="database id on DB", required=True)
    parser.add_argument("-m", "--secmaster", help="security master", required=True)
    parser.add_argument("-e", "--excode", help="exchange")
    parser.add_argument("-n", "--names", help="update names")
    parser.add_argument("-r", "--dry-run", nargs='?', default='false', help="check result without database update")

    options = parser.parse_args(args)
    return options

def connect_db(server, port, sid, username, password):
    sym_db = SymbolDB(options.dbserver, options.port, options.sid, options.username, options.password)
    sql_alter_session = "ALTER SESSION SET CURRENT_SCHEMA=history"
    sym_db.connect(sql_alter_session)
    return sym_db

def close_db(db):
    db.close()

if __name__ == '__main__':
    options = getOptions(sys.argv[1:])

    sym_db = connect_db(options.dbserver, options.port, options.sid, options.username, options.password)
    sec_master = SecMasterInvoker(options.secmaster)


    if options.dry_run is None:
        is_dry_run = False
    else:
        is_dry_run = options.dry_run.lower() in ('yes', 'true', 'y', 't')
    is_name_update = options.names.lower() in ('yes', 'true', 'y', 't')

    if is_name_update and options.excode == 'EDGX':
        sql_select = "SELECT symbol FROM symbol WHERE excode = '{}'".format(options.excode)
        symbols_db = sym_db.read(sql_select)
        symbols = list(map(lambda x: x.split(':')[0], symbols_db))
        print(" Total symbols from database: " + str(len(symbols)))

        symbols_security_master = []
        for symbol_name in symbols:
            symbol, response = sec_master.get_symbol(symbol_name)
            if response:
                print(symbol)
                symbols_security_master.append(symbol)
        print(" Total symbols from Security Master: " + str(len(symbols_security_master)))

        sql_update = None
        for symbol in symbols_security_master:
            sql_update = "UPDATE symbol SET name='{0}', shortname='{1}' WHERE symbol='{2}'"\
                .format(symbol['longname'], symbol['shortname'], symbol['symbol'] + ':EGX')
            if is_dry_run:
                print("*** Symbol Name Change [{2}] --> name: '{0}', shortname: '{1}'"
                      .format(symbol['longname'], symbol['shortname'], symbol['symbol'] + ':EGX'))
            else:
                sym_db.update(sql_update)

    close_db(sym_db)
