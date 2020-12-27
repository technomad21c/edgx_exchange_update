from edgx_update import SecMasterInvoker

def test_get_symbol(security_master_symbols_symbol_moat, server_uri):
    smi = SecMasterInvoker(server_uri)
    assert smi.get_symbol("MOAT") == (security_master_symbols_symbol_moat, True)
