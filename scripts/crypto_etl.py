import requests
import os
import sqlalchemy as db
from sqlalchemy.orm import declarative_base, Session
import pandas as pd
from typing import List, Dict

Base = declarative_base()

# Модель для SQLAlchemy
class Cryptocurrency(Base):
    __tablename__ = 'cryptocurrencies'
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String, unique=True)
    name = db.Column(db.String)
    cmc_rank = db.Column(db.Integer)
    volume_24h = db.Column(db.Float)
    volume_7d = db.Column(db.Float)
    volume_30d = db.Column(db.Float)

def fetch_data(url: str) -> Dict:
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def get_missing_coins() -> List[Dict]:
    # Загрузка данных с CoinMarketCap
    cmc_url = "https://api.coinmarketcap.com/data-api/v3/cryptocurrency/listing?start=1&limit=1500&sortBy=market_cap&sortType=desc&convert=USD,BTC,ETH&cryptoType=all&tagType=all&audited=false&aux=ath,atl,high24h,low24h,num_market_pairs,cmc_rank,date_added,max_supply,circulating_supply,total_supply,volume_7d,volume_30d,self_reported_circulating_supply,self_reported_market_cap"
    cmc_data = fetch_data(cmc_url)
    
    # Загрузка данных с SimpleSwap
    ss_url = "https://simpleswap.io/api/v3/currencies?fixed=false&includeDisabled=false"
    ss_data = fetch_data(ss_url)
    
    # Извлечение символов из CoinMarketCap
    cmc_coins = {coin['symbol'].upper(): coin for coin in cmc_data['data']['cryptoCurrencyList']}
    
    # Извлечение символов из SimpleSwap
    ss_symbols = {currency['symbol'].upper() for currency in ss_data}

    
    # Фильтрация отсутствующих монет
    missing_coins = [
        {
            'symbol': symbol,
            'name': cmc_coins[symbol]['name'],
            'cmc_rank': cmc_coins[symbol]['cmcRank'],
            'volume_24h': cmc_coins[symbol].get('quotes', [{}])[0].get('volume24h', 0),
            'volume_7d': cmc_coins[symbol].get('volume7d', 0),
            'volume_30d': cmc_coins[symbol].get('volume30d', 0)
        }
        for symbol in cmc_coins.keys() if symbol not in ss_symbols
    ]
    
    return missing_coins

def save_to_database(coins: List[Dict]) -> None:
    # Создаем папку data, если её нет
    data_dir = os.path.abspath('../crypto_comparison/data')
    os.makedirs(data_dir, exist_ok=True)

    # Подключение к SQLite (абсолютный путь)
    db_path = os.path.join(data_dir, 'crypto_data.db')
    engine = db.create_engine(f'sqlite:///{db_path}')
    
    Base.metadata.create_all(engine)
    
    # Сохранение данных
    with Session(engine) as session:
        for coin in coins:
            # Проверяем, существует ли уже запись с таким символом
            existing_coin = session.query(Cryptocurrency).filter_by(symbol=coin['symbol']).first()
            if existing_coin:
                # Если запись существует, обновляем её
                existing_coin.name = coin['name']
                existing_coin.cmc_rank = coin['cmc_rank']
                existing_coin.volume_24h = coin['volume_24h']
                existing_coin.volume_7d = coin['volume_7d']
                existing_coin.volume_30d = coin['volume_30d']
            else:
                # Если записи нет, создаем новую
                crypto = Cryptocurrency(**coin)
                session.add(crypto)
        session.commit()

def export_to_csv() -> None:
    # Чтение данных из базы и сортировка
    db_path = os.path.abspath('../crypto_comparison/data/crypto_data.db')
    engine = db.create_engine(f'sqlite:///{db_path}')
    query = "SELECT * FROM cryptocurrencies ORDER BY volume_24h DESC"
    df = pd.read_sql(query, engine)
    
    # Экспорт в CSV
    csv_path = os.path.abspath('../crypto_comparison/data/missing_coins.csv')
    df.to_csv(csv_path, index=False)

if __name__ == "__main__":
    missing_coins = get_missing_coins()
    save_to_database(missing_coins)
    export_to_csv()
    print("Данные успешно сохранены и экспортированы!")