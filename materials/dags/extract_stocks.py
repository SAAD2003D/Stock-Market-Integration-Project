
import pandas as pd
import mysql.connector
import alpha_vantage
import requests

API_key='BLERYGTTKC84JRJO'

def extract (symbol,**context):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&outputsize=full&apikey={API_key}'
    r = requests.get(url).json()
    data=r
    df=pd.DataFrame(data=data["Time Series (Daily)"]).T
    df = df.reset_index()

    # Rename the index column from 'index' to 'date'
    df.rename(columns={'index': 'date' ,'1. open':'open_price','2. high':'high','3. low':'low','4. close':'close_price','5. volume':'volume' }, inplace=True)
     
    context['ti'].xcom_push(key=f'{symbol}_raw_data', value=df.to_dict())
    
    print(df)
    

def transform(symbol,**context):
    
    stock_data_dict = context['ti'].xcom_pull(key=f'{symbol}_raw_data', task_ids=f'download_{symbol}')
    stock_data = pd.DataFrame(stock_data_dict)
    stock_data['open_price']=pd.to_numeric(stock_data['open_price'])
    stock_data['close_price']=pd.to_numeric(stock_data['close_price'])
    stock_data['high']=pd.to_numeric(stock_data['high'])
    stock_data['low']=pd.to_numeric(stock_data['low'])
    stock_data['volume']=pd.to_numeric(stock_data['volume'])
    
    stock_data['SMA_20'] = stock_data['close_price'].rolling(window=20).mean()
    stock_data['EMA_20'] = stock_data['close_price'].ewm(span=20, adjust=False).mean()

    delta = stock_data['close_price'].diff(1)
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14).mean()
    avg_loss = loss.rolling(window=14).mean()
    rs = avg_gain / avg_loss
    stock_data['RSI'] = 100 - (100 / (1 + rs))

    ema_12 = stock_data['close_price'].ewm(span=12, adjust=False).mean()
    ema_26 = stock_data['close_price'].ewm(span=26, adjust=False).mean()
    stock_data['MACD'] = ema_12 - ema_26
    stock_data['Signal_Line'] = stock_data['MACD'].ewm(span=9, adjust=False).mean()
    
    stock_data.dropna(inplace=True)
    
    context['ti'].xcom_push(key=f'{symbol}_transformed_data', value=stock_data.to_dict())
    


def load(symbol, **context):
    # Pull transformed data from XCom
    stock_data_dict = context['ti'].xcom_pull(key=f'{symbol}_transformed_data', task_ids=f'transform_{symbol}')
    stock_data = pd.DataFrame(stock_data_dict)
    print(stock_data_dict)

    # Ensure date is in the correct format and handle NaN values
    stock_data['date'] = pd.to_datetime(stock_data['date']).dt.date
    stock_data = stock_data.fillna(0)

    # Connect to MySQL and create table if it doesn't exist
    try:
        conn = mysql.connector.connect(
            host="mysql",
            user="airflow_user",
            password="airflow_password",
            database="airflow"
        )
        cursor = conn.cursor()
        
        # Create table
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {symbol}_data (
                date DATE PRIMARY KEY,
                open_price FLOAT,
                high FLOAT,
                low FLOAT,
                close_price FLOAT,
                volume INT,
                sma_20 FLOAT,
                ema_20 FLOAT,
                rsi FLOAT,
                macd FLOAT,
                signal_line FLOAT
            )
        ''')

        # Insert data
        for _, row in stock_data.iterrows():
            cursor.execute(f'''
                INSERT INTO {symbol}_data (date, open_price, high, low, close_price, volume, sma_20, ema_20, rsi, macd, signal_line)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open_price=VALUES(open_price), high=VALUES(high), low=VALUES(low), close_price=VALUES(close_price),
                volume=VALUES(volume), sma_20=VALUES(sma_20), ema_20=VALUES(ema_20),
                rsi=VALUES(rsi), macd=VALUES(macd), signal_line=VALUES(signal_line);
            ''', (
                row['date'], row['open_price'], row['high'], row['low'], row['close_price'], row['volume'],
                row['SMA_20'], row['EMA_20'], row['RSI'], row['MACD'], row['Signal_Line']
            ))

        conn.commit()
    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()
        
   
