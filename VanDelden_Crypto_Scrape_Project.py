# KEVIN VANDELDEN Coingecko Web Scraping PROJECT
# DOWNLOAD MySQL Community Server https://dev.mysql.com/downloads/mysql/
# DOWNLOAD MySQL Workbench https://dev.mysql.com/downloads/workbench/

import mysql.connector
import pandas as pd
import mysql.connector
import requests 
import time
import pymysql
import schedule
import bs4

from datetime import datetime
from mysql.connector import Error
from bs4 import BeautifulSoup

## CONNECT TO THE DATABASE
mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password=[""],
  database="coin_gecko_db"
)
mycursor = mydb.cursor()
## TO CREATE DATABASE
# mycursor.execute("CREATE DATABASE coin_gecko_db")

## TO DROP/CREATE NEW TABLE
# mycursor.execute("DROP TABLE IF EXISTS cg_data")
# mycursor.execute("CREATE TABLE cg_data (id INT(255),symbol VARCHAR(255),price_usd DECIMAL(65,30), vol_24h_btc DECIMAL(65,30), vol_24h_usd DECIMAL(65,30), mkt_cap_btc DECIMAL(65,30), mkt_cap_usd DECIMAL(65,30), collected_at DATETIME, updated_at DATETIME)")

def get_cg_data(): 
    URL = 'https://www.coingecko.com/en'
    r = requests.get(URL)
    soup = BeautifulSoup(r.content, 'html5lib')
    fetch_time = str(datetime.now())
    print()

    soup_list = soup.find_all('td',{'class':"td-price price text-right"})
    soup_volume = soup.find_all('td',{'class':"td-liquidity_score lit text-right %> col-market"})
    soup_mkt_cap = soup.find_all('td',{'class':"td-market_cap cap col-market cap-price text-right"})

    btc_usd = 0

    # to find price of BTC, i think volume + mkt cap data is given in BTC 
    for i in range(len(soup_list)):
        coin_name_body = soup_list[i].find('span')
        if coin_name_body['data-coin-symbol'] == 'btc':
            btc_usd = float(soup_list[i]['data-sort'])

    for i in range(len(soup_list)):
        # getting coin info from HTML
        coin_name_body = soup_list[i].find('span')
        coin_id = coin_name_body['data-coin-id']
        coin_symbol = coin_name_body['data-coin-symbol']
        coin_price = soup_list[i]['data-sort']
        coin_volume_btc = soup_volume[i]['data-sort']
        coin_volume_usd = float(coin_volume_btc)*btc_usd
        coin_mkt_cap_btc = soup_mkt_cap[i]['data-sort']
        coin_mkt_cap_usd = float(coin_mkt_cap_btc)*btc_usd

        # making string for easy insertion to MySQL db
        values_str = (coin_id, coin_symbol, coin_price, coin_volume_btc, coin_volume_usd, coin_mkt_cap_btc, coin_mkt_cap_usd, fetch_time, fetch_time)
        mycursor.execute("INSERT INTO cg_data (id, symbol, price_usd, vol_24h_btc, vol_24h_usd, mkt_cap_btc, mkt_cap_usd, collected_at, updated_at) VALUES " + str(values_str))
        mydb.commit()
    
    # for noting how many records are created, if can expect an exact number every run
    print('inserted',len(soup_list),'records at',fetch_time)
    
    ## CHECKING FOR DUPLICATE ROWS
    df = pd.read_sql("SELECT * FROM cg_data cg",mydb)
    dup_check = df.duplicated(subset=None, keep='first')

    if dup_check.any(axis=None) == False:
        print('No duplicate records found at',fetch_time)
    print()
    
    ## PERCENT CHANGE CHECK FOR DB
    percent_df = pd.read_sql("with pct_checker as (SELECT DISTINCT cg.id ,cg.symbol,cg.price_usd,cg.vol_24h_btc,cg.vol_24h_usd,cg.mkt_cap_btc,cg.mkt_cap_usd,cg.collected_at,cg.updated_at,RANK() OVER (PARTITION BY cg.id ORDER BY cg.collected_at DESC) as date_rank FROM cg_data cg ORDER BY cg.id asc )SELECT DISTINCT pct.id,pct.symbol,pct.price_usd,pct2.price_usd as price_usd_2,pct.collected_at,pct2.collected_at as collected_at_2,ABS(((pct2.price_usd - pct.price_usd)/pct.price_usd)*100.0) as abs_percent_change FROM pct_checker pct LEFT JOIN pct_checker pct2 ON pct2.id = pct.id AND pct.symbol = pct2.symbol AND pct.date_rank = pct2.date_rank + 1 HAVING abs_percent_change >= 50.0",mydb)
    print("records of percent-change between entries >= 50%",percent_df)

# schedule.every().day.at("01:00").do(get_cg_data)
# this method probably would experience some drift in collection time, still experimenting with the scheduling feature but ideally would run at an exact TOD
schedule.every(1440).minutes.do(get_cg_data)

while True:
    # Checks whether a scheduled task 
    # is pending to run or not
    schedule.run_pending()
    time.sleep(86400)
