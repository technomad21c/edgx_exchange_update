from edgx_update import SymbolDB

def test_read(sym_db):
    query_select = "SELECT symbol FROM SYMBOL WHERE symbol='MOAT'"
    returned_symbols = sym_db.read(query_select)
    assert returned_symbols == ['MOAT'] or returned_symbols == ['moat']
