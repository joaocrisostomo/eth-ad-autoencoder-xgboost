import re

def transactions(block: int = None, wallet: str = None) -> list:
    assert(wallet is not None)
    
    if block is not None:
        startblock = block
    else:
        startblock = 0
        
    url_params = {
        'module': 'account',
        'action': 'txlist',
        'address': wallet,
        'startblock': startblock,
        'endblock': 99999999,
        'page': 0,
        'offset': 10,
        'sort': 'asc',
        'apikey': ETHERSCAN_KEY
    }
    
    response = requests.get('https://api.etherscan.io/api', params=url_params)
    response_parsed = json.loads(response.content)
    #assert(response_parsed['message'] == 'OK')
    txs = response_parsed['result']
    
    return [ {'Trx_From_Wallet': str(tx['from']), 'Trx_To_Wallet': str(tx['to']), 'Trx_Amount': int(tx['value'])*10**-19, \
              'Trx_Datetime': datetime.utcfromtimestamp(int(tx['timeStamp'])), 'Trx_Hash': str(tx['hash']), 'Trx_Gas': float((int(tx['gas'])*10**-19)), \
              'Trx_GasPrice': float((int(tx['gasPrice'])*10**-19)),'Trx_Status': str(tx['txreceipt_status']), 'Trx_Method_ID': str(tx['methodId']), \
              'BlockNumber' : int(tx['blockNumber'])} 
            for tx in txs ]

def Get_All_Trx_Portfolio(databse_name, wallet_dict: dict = None) -> list:
    assert(wallet_dict is not None)
    
    df = pd.DataFrame(columns = ['Trx_From_Wallet', 'Trx_To_Wallet', 'Trx_Amount', 'Trx_Datetime', 'Trx_Hash', 'Trx_Gas', 'Trx_GasPrice', 'Trx_Status', 'Trx_Method_ID', 'BlockNumber'])
    
    for key, value in wallet_dict.items():
        
        Wallet = key
        
        if value is not None and len(value) == 1:
            block = int(re.sub(r"[\[\]]",'',str(value))) + 1
        else:
            block = 0
            
        print('\nWallet being processed: ' + Wallet)
        
        trx_rslt = transactions(block, Wallet)
        Trx_df = pd.DataFrame(trx_rslt, columns = ['Trx_From_Wallet', 'Trx_To_Wallet', 'Trx_Amount', 'Trx_Datetime', 'Trx_Hash', 'Trx_Gas', 'Trx_GasPrice', 'Trx_Status', 'Trx_Method_ID', 'BlockNumber'])
        
        Trx_df["receiverIsContract"] = False
        for index_label, row_series in Trx_df.iterrows():
            if len(row_series['Trx_To_Wallet']) >= 42:
                addr = Web3.toChecksumAddress(row_series['Trx_To_Wallet'])
                Trx_df.at[index_label , "receiverIsContract"] = web3.eth.getCode(addr) !=0
            else:
                Trx_df["receiverIsContract"] = False
        
        ###########
        #Update DF#
        ###########
        df = df.append(Trx_df)
        
        print('\nGo to sleep')
        time.sleep(5)
        
        ###########
        #Update BD#
        ###########
        #Trx_df.to_sql("transactions_aux", engine, if_exists="replace", index= False)
        #print('Added to aux')
        
        #mycursor.execute("""INSERT INTO ETH_TRX
        #                    SELECT A.Trx_From_Wallet,
        #                            A.Trx_To_Wallet,
        #                            A.Trx_Amount,
        #                            A.Trx_Datetime,
        #                            A.Trx_Hash,
        #                            A.Trx_Gas,
        #                            A.Trx_GasPrice,
        #                            A.Trx_Status,
        #                            A.Trx_Method_ID,
        #                            A.BlockNumber,
        #                            A.receiverIsContract
        #                    FROM (SELECT B.*,
        #                            A.Trx_Hash as TRXHASH
        #                        FROM transactions_aux as B
        #                        LEFT JOIN ETH_TRX as A
        #                        ON B.Trx_Hash = A.Trx_Hash) as A
        #                        WHERE A.TRXHASH is Null;""")
       
        #print('BD:ETH_TRX Updated\n')
        #del(Trx_df)
        
        #mycursor.execute(f"DROP TABLE {databse_name}.transactions_aux")
        
        
    return df
    
    
# Load ABIs and Interface IDs
contractABIs = json.load(open('./contractABIs.json')) # Loads the ABIs from the JSON file

# Constants to identify ERC20 Transfer and Approval events/transactions
TRANSFER_EVENT_HASH = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
APPROVAL_EVENT_HASH = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"

# Function to get ERC20 Information from a transaction
def verify_erc20(address: str) -> dict:
    """ Returns a map with the contract's properties if it's ERC20, else raise an error """

    # Load in contract with ERC20 ABI to use ERC20 functions
    contract = web3.eth.contract(address = address, abi = contractABIs["ERC20"])

    # Functions
    token = {
        "totalSupply" : contract.functions.totalSupply().call(),
        "symbol" : contract.functions.symbol().call(),
        "name" : contract.functions.name().call(),
        "decimals" : contract.functions.decimals().call()
    }

    return token # Returns token properties in a dictionary

# Function to get all of the transaction's relevant information
def get_transaction_info(address: str) -> dict:
    """ Returns a map with the address' relevant transaction information"""

    transactionInfo = {} # A dictionary with all of the transactions' information

    transaction = web3.eth.get_transaction(address) # Get all of the transaction information

    block = web3.eth.get_block(transaction["blockHash"]) # Get the block information

    receipt = web3.eth.get_transaction_receipt(address) # Get the receipt information

    # Addresses of all contracts created by the transaction TODO: ADD THIS FEATURE - Web3.py Latest Build Only
    # transactionInfo["createdContracts"] = transaction["creates"]
    
    # Transaction Hash
    transactionInfo["hash"] = transaction["hash"].hex()

    # Block Number
    transactionInfo["blockNumber"] = transaction["blockNumber"]

    # Transaction Timestamp
    transactionInfo["timestamp"] = block["timestamp"]

    # Detect if the receiver is a contract
    if "to" in transaction:
        if transaction["to"] != None:
            transactionInfo["receiverIsContract"] = web3.eth.getCode(transaction["to"]) != "0x"

    # Transaction Sender and Receiver
    transactionInfo["sender"] = transaction["from"]
    transactionInfo["receiver"] = transaction["to"] # Whom the transaction is sent to

    # Transaction Type - Can be ERC20 or Other. If It's ERC20 a tokenAddress will be added    
    try:
        token = verify_erc20(transaction["to"]) # Confirms it's ERC20
        transactionInfo["ERC20Info"] = {
            "eventInfo": None, # If transaction emits an event (Transfer or Approval)
            "tokenAddress": transaction["to"], # Pass the token address
            "tokenDetails": token # Information about the token
        }

        # Find Transfer Event and get associated value
        for log in receipt["logs"]:
            if HexBytes(TRANSFER_EVENT_HASH) in log["topics"]: # If the Transfer() event is found:
                transactionInfo["ERC20Info"]["eventInfo"] = {
                    "type": "transfer",
                    "transferReceiver": Web3.toChecksumAddress("0x" + log["topics"][2].hex()[26:]), # The address of the token's receiver
                    "tokenQuantity": int(log["data"], 16), # The amount of token transfered
                    "transferReceiverIsContract": web3.eth.getCode(Web3.toChecksumAddress("0x" + log["topics"][2].hex()[26:])) != "0x" # Whether the transfer receiver is a contract
                }

                break
            elif HexBytes(APPROVAL_EVENT_HASH) in log["topics"]: # If the Approval() event is found:
                transactionInfo["ERC20Info"]["eventInfo"] = {
                    "type": "approval",
                    "addressApproved": Web3.toChecksumAddress("0x" + log["topics"][1].hex()[26:]), # The address of the approved spender
                    "tokenAllowance": int(log["data"], 16), # The amount of tokens the spender can use
                    "approvedSpenderIsContract": web3.eth.getCode(Web3.toChecksumAddress("0x" + log["topics"][1].hex()[26:])) != "0x" # Whether the transfer receiver is a contract
                }
                break  
        
    except Exception: # If it's not an ERC20
        transactionInfo["ERC20Info"] = None

    # Ethereum Value
    transactionInfo["ethValue"] = transaction["value"] / (10 ** 18) # Convert to ETH

    return transactionInfo


from eth_api_functions import lint_transaction_list
from tqdm import tqdm

Wallet_Address = list(walletBlocked_dic)[244:]

# Create a list out of all the transaction hashes of transactions to and from the walletAddress
with tqdm(total=len(Wallet_Address)) as pbar:

    #declare empty dataframes:
    ETHEREUM_TRANSFERS = pd.DataFrame()
    ERC20_TRANSFERS = pd.DataFrame()
    ERC20_APPROVALs = pd.DataFrame()
    OTHER_TRANSACTIONS = pd.DataFrame()
    
    to_csv_flag = 0

    for walletAddress in Wallet_Address:
        
        print('Wallet being processed: ' + str(walletAddress))
        
        to_csv_flag += 1
        pbar.update(1)

        txnHashList = list(df[(df['Trx_To_Wallet'] == walletAddress) | (df['Trx_From_Wallet'] == walletAddress)]['Trx_Hash'])

        # Runs through txnHashList and fetches the info of those transactions to the blockchain
        processedTxnList = []

        for txn in txnHashList:
            print('Trx Hash: ' + str(txn))
            t = get_transaction_info(txn)
            time.sleep(1)
            processedTxnList.append(t)
        
        #Lint all transactions into 4 different types: Ethereum Transfers, ERC20 Transfers, ERC20 Approvals and Others
        lintedList = lint_transaction_list(processedTxnList)

        #ethList = lintedList["eth"]
        erc20TransferList = lintedList["erc20Transfer"]
        erc20ApprovalList = lintedList["erc20Approval"]
        otherList = lintedList["other"]

        # TO DO: Create 4 tables and add results of each list to each table

        tableNames = [
            #"ETHEREUM_TRANSFERS",
            "ERC20_TRANSFERS",
            "ERC20_APPROVALs",
            "OTHER_TRANSACTIONS"
        ]

        # engine = connect_to_db('admin', 'wWusLXWEsxNqaviwGPsP', 'cryptologic_BE_Dev')

        # Run through all lists and add them to their respective tables
        i = 0
        for lst in (erc20TransferList, erc20ApprovalList, otherList):
            tableName = tableNames[i]

            for txn in lst:
                flattenedTxn = flatdict.FlatDict(txn, delimiter='_')

                # Transform all values to lower case
                for key in flattenedTxn:
                    if type(flattenedTxn[key]) == str:
                        flattenedTxn[key] = flattenedTxn[key].lower()
                    flattenedTxn[key] = [flattenedTxn[key]]

                output = pd.DataFrame(dict(flattenedTxn)) # Must be converted to dict from FlatDict
                
                print(tableName)
                
                if tableName == "ETHEREUM_TRANSFERS":
                    #ETHEREUM_TRANSFERS = output.append(ETHEREUM_TRANSFERS)
                    continue
                if tableName ==  "ERC20_TRANSFERS":
                    ERC20_TRANSFERS = output.append(ERC20_TRANSFERS)
                if tableName == "ERC20_APPROVALs":
                    ERC20_APPROVALs = output.append(ERC20_APPROVALs)
                if tableName == "OTHER_TRANSACTIONS":
                    OTHER_TRANSACTIONS = output.append(OTHER_TRANSACTIONS)

                #df.to_sql(tableName, con=engine, if_exists='append', index = False)

            i += 1

        if to_csv_flag%2==0:
            print('Saving to CSV files...')
            #ETHEREUM_TRANSFERS.to_csv('ETHEREUM_TRANSFERS_v2.csv',mode='w+')
            ERC20_TRANSFERS.to_csv('ERC20_TRANSFERS_v6.csv',mode='w+')
            ERC20_APPROVALs.to_csv('ERC20_APPROVALs_v6.csv',mode='w+')
            OTHER_TRANSACTIONS.to_csv('OTHER_TRANSACTIONS_v6.csv',mode='w+')
            
            f = open("Notes_dataset_gen.txt", "a")
            f.write("Wallet: " + walletAddress + " processed with success!\n Iteration: " + str(to_csv_flag))
            f.close()

        print("Wallet: " + walletAddress + " Ended | Iteration: " + str(to_csv_flag))