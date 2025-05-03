import os
from etherscan import Etherscan
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv(".env")
    YOUR_API_KEY = os.getenv('ETHERSCAN_API_KEY')
    eth = Etherscan(YOUR_API_KEY)
    address = "0x0ecddcf41754360ab129d7ca4c8abf220f9c32bd"
    result = eth.get_normal_txs_by_address(address, startblock=0, endblock=99999999, sort="asc")
    print(result[0].keys())





