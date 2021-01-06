from edgx_update import SymbolDB

def test_read(sym_db):
    query_select = "SELECT symbol, shortname, name FROM SYMBOL WHERE symbol='MOAT'"
    returned_symbols, _ = sym_db.read(query_select)
    assert returned_symbols == ['MOAT'] or returned_symbols == ['moat']
