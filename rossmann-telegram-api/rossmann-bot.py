import requests
import json
import pandas as pd
from flask import Flask, request, Response

TOKEN = '7155918199:AAHqgB9FT1iOYk-4eibjS52GYdsP2vYy6cg'


# https://api.telegram.org/bot7155918199:AAHqgB9FT1iOYk-4eibjS52GYdsP2vYy6cg/getMe
# https://api.telegram.org/bot7155918199:AAHqgB9FT1iOYk-4eibjS52GYdsP2vYy6cg/getUpdates
# https://api.telegram.org/bot7155918199:AAHqgB9FT1iOYk-4eibjS52GYdsP2vYy6cg/setWebhook?url=https://40abacb6a073f5.lhr.life
# https://api.telegram.org/bot7155918199:AAHqgB9FT1iOYk-4eibjS52GYdsP2vYy6cg/sendMessage?chat_id=1214372537&text=Hi Thiago, i am doing good, tks

# 1214372537


def send_message(chat_id, text):

    url = f'https://api.telegram.org/bot{TOKEN}'
    url = f'/sendMessage?chat_id={chat_id}'

    r = request.post ( url, json={'text': text})
    print(f'status code: {r.status_code}')

    return None

def load_data_set(store_id):
    df10 = pd.read_csv( '../data/test.csv' )
    df_store_raw = pd.read_csv('../data/store.csv')


    df_test = pd.merge( df10, df_store_raw, how='left', on='Store' )

    df_test = df_test[ df_test['Store'] == store_id ]

    if not df_test.empty:

        df_test = df_test[df_test['Open'] != 0]
        df_test = df_test[~df_test['Open'].isnull()]
        df_test = df_test.drop( 'Id', axis=1 )

        data = json.dumps( df_test.to_dict( orient='records' ) )

    else:
        data = 'error'

    return data

def predict( data ):
    #url = 'http://0.0.0.0:5000/rossmann/predict'
    url = 'https://webapp-rossmann-wpna.onrender.com/rossmann/predict'
    header = {'Content-type': 'application/json' }
    data = data
    r = requests.post( url, data=data, headers=header )
    print( 'Status Code {}'.format( r.status_code ) )

    d1 = pd.DataFrame( r.json(), columns=r.json()[0].keys() )

    return d1

def parse_message(message):
    chat_id = message['message']['chat']['id']
    store_id = message['message']['text']
    print(chat_id)
    print(store_id)
    store_id = store_id.replace('/', '')  # Remove a barra do início, se houver
    
    try:
        store_id = int(store_id)  # Tenta converter o texto em um número inteiro

    except ValueError:
        send_message(chat_id, 'Store ID is Wrong')  # Se não puder ser convertido, envia uma mensagem de erro

    return chat_id, store_id

app = Flask( __name__ )

@app.route( '/', methods=['GET', 'POST'] )
def index():
    if request.method == 'POST':

        update = request.json
        updates = update['result']
        latest_update = updates[-1]  # Pega a última atualização

        message = latest_update['message']
        chat_id, store_id = parse_message( message )
        print(chat_id)
        if store_id != 'error':
            data = load_data_set(store_id)
            if store_id != 'error':
                d1 = predict( data )

                d2 = d1[['store', 'prediction']].groupby( 'store' ).sum().reset_index()

                msg = 'Store Number {} will sell R${:,.2f} in the next 6 weeks'.format( d2['store'].values[0], d2['prediction'].values[0] )

                send_message(chat_id,msg)

                return Response( 'ok' , status=200 )

            else:
                send_message( chat_id, 'Store Not Available')
                return Response( 'ok' , status=200 )
        else:
            send_message( chat_id, 'Store ID is Wrong')
            return Response( 'ok' , status=200 )

    else:
        return '<h1> Rossmann Telegram BOT </h1>'


if __name__ == '__main__':
    app.run( host='0.0.0.0', port=5000 )