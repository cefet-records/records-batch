# plugins/web3_client.py
import json
import os

from airflow.hooks.base import BaseHook
# from airflow.models import Variable
# from eth_account import Account
from web3 import Web3
from web3.middleware.proof_of_authority import ExtraDataToPOAMiddleware


# Helper para construir conexão Web3 usando uma Connection do Airflow chamada "web3_rpc"
def get_web3():
    """
    Retorna um objeto Web3 conectado ao RPC definido na connection 'web3_rpc'
    (connection.host ou extras["rpc_uri"]).
    """
    try:
        conn = BaseHook.get_connection("web3_rpc")
        # prefer extra field "rpc_uri" else host
        extra = json.loads(conn.get_extra()) if conn.get_extra() else {}
        rpc_uri = extra.get("rpc_uri") or conn.host
    except Exception:
        # fallback para variável ambiente
        rpc_uri = os.environ.get("WEB3_RPC_URI")

    w3 = Web3(Web3.HTTPProvider(rpc_uri))
    # se for uma chain PoA (ex: goerli, some private chains) ative middleware:
    w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
    return w3


def load_abi_from_file(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_contract(w3, contract_address, abi):
    return w3.eth.contract(address=Web3.to_checksum_address(contract_address), abi=abi)


def sign_and_send_tx(w3, tx_dict, private_key, chain_id=None):
    """
    tx_dict deve conter: nonce, gas, maxFeePerGas/maxPriorityFeePerGas OR gasPrice, to/from/data...
    Retorna tx_hash hex
    """
    # se chain_id for necessário, inclua
    if chain_id:
        tx_dict.setdefault("chainId", chain_id)

    signed = w3.eth.account.sign_transaction(tx_dict, private_key)
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    return w3.to_hex(tx_hash)
