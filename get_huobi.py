import asyncio
import websockets
import ssl
import gzip
import json
import time
import pandas as pd
import os

#no break logic here

async def capture_huobi():
    uri = "wss://api-aws.huobi.pro/ws"
    ssl_context = ssl.SSLContext()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    current = 1509494400

    async with websockets.connect(uri, ssl=ssl_context) as websocket:
        while True:
            
            

            data = await websocket.recv()
            decoded = gzip.decompress(data).decode('utf-8')
            decoded = json.loads(decoded)
            
            if 'ping' in decoded:    
                pong_res = json.dumps({
                    "pong": decoded['ping']
                })
                await websocket.send(pong_res)
                
            pull_data = json.dumps({
                    "req": "market.btcusdt.kline.5min",
                    "id": "id9",
                    "from": current,
                    "to": current + 86400
                })

            await websocket.send(pull_data)

            

            print(current)


            if 'status' in decoded:
                if decoded['status'] == 'ok':
                    df = pd.DataFrame(decoded['data'])
                    if len(df) > 0:
                        current = current + 86400

                        if os.path.isfile('huobi.csv'):
                            df.to_csv('huobi.csv', index=None, mode='a', header=None)
                        else:
                            df.to_csv('huobi.csv', index=None)

                        

            


asyncio.get_event_loop().run_until_complete(capture_huobi())