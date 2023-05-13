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
    