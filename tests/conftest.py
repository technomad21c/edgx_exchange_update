import pytest
import json
import argparse
from edgx_update import SymbolDB

@pytest.fixture
def security_master_symbols_symbol_moat():
    with open('security_master_symbols_symbol_moat.json') as json_file:
        symbol = json.load(json_file)
    return symbol

@pytest.fixture
def server_uri():
    return "http://shaco.dev.quotemedia.com:9977"

@pytest.fixture
def sym_db():
    sym_db = SymbolDB('boreas.dev.quotemedia.com', 1521, 'pod', 'dlferrari', 'shifouge')
    sql_alter_session = "ALTER SESSION SET CURRENT_SCHEMA=history"
    sym_db.connect(sql_alter_session)
    return sym_db