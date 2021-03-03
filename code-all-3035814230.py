# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 17:36:25 2021

@author: Yi WEN
"""

# part 1 
# web scraping: scraping the contituents of Nasdaq-100 from wikipedia

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os


r = \
    requests.get(
        'https://en.wikipedia.org/wiki/Nasdaq-100',
        timeout=3)

s = BeautifulSoup(r.text, 'lxml')
table = s.find('table', {'id':'constituents'})

tikers = []
securities = []
sectors = []
for row in table.findAll('tr')[1:]:
    tiker = row.findAll('td')[1].text
    security = row.findAll('td')[0].text
    sector = row.findAll('td')[2].text
    tikers.append(tiker)
    securities.append(security)
    sectors.append(sector)

df = pd.DataFrame({'tiker':tikers,'security':securities,'sector':sectors})





# part 2 
# Twitter scraping: with python package "tweepy", scraping the twitter that mentioned the constituents of Nasdaq-100. 
# limitation: Tweppy only provide access to titter of last 7-10 days.
# So, in order to get a better structeured data, I only pick 9 firms to show the whole process of my project.
# The firms that I choosed have twitter data start from 2021-02-21 to 2021-02-28.

import tweepy
import time
import pandas as pd
import xlsxwriter
import os


os.chdir(r'C:\Users\Yi WEN\Desktop\7036-midterm-project-3035814230')

access_token = "1365230828796530688-YUxFfcXxGsPlSWu0Kv8VmL5PhMkGVv"
access_token_secret = "u3z8vW9HyQFalUN4nkZjCxgQ6z40woQ8PpkkFQnAbcQyv"
consumer_key = "RFATpkNJtGCbhqY9goKzCjRyN"
consumer_secret = "ZL0LcmIBA1YzgP55dikkhp0CnMN1ZipHYoIM45uSgqOp1FrrDo"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth,wait_on_rate_limit=True)


for t in range(102):
    df.tiker[t] = '$' + df.tiker[t]  # preparing for scrapping Twitter, df is the infomation about Nasdaq-100 that I generated in part1


start_time = time.time()
for d in range(15):
    firm = pd.DataFrame()
    ls= []
    l = []
    for tweet in tweepy.Cursor(api.search, q = df.tiker[d], rpp = 100, tweet_mode = 'extended').items(2000):
        l = [tweet.created_at, tweet.full_text, tweet.user.followers_count] 
        l = tuple(l)                    
        ls.append(l)
    
    firm = pd.DataFrame(ls)
    firm.columns = ['created at', 'text', 'follower count']
    
    file_name = df.tiker[d] + '.xlsx'
    writer_firm = pd.ExcelWriter(file_name, engine='xlsxwriter')

    firm.to_excel(writer_firm)

    writer_firm.save()
print("--- %s seconds ---" % (time.time() - start_time))



# from the first 15 constituents, pick 10 firms with data from 2021-02-21 to 2021-02-28.
all_firms = list(df.tiker[0:15])

[all_firms.remove(i) for i in ['$AMD','$GOOGL','$GOOG','$AAPL','$AMZN','$ANSS']]




# part 3 
# data cleaning and preprocessing
# sentiment analytics using Vader package
# combineing the sentiment analytics with the stock price from yahoo finance

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import numpy as np
from sklearn.preprocessing import StandardScaler
import datetime as dt
import pandas_datareader.data as web
import math



# this 'def' part code comes from the internet, using vader to do sentiment analytics
def sentimentScore(Tweet):
    analyzer = SentimentIntensityAnalyzer()
    results = []
    for sentence in Tweet:
        vs = analyzer.polarity_scores(sentence)
        print("Vader score: " + str(vs))
        #print("{: <65} {}".format(sentence, str(vs))) 
        #NOTE! I blocked the second print command so the sentences are
        #left out in the cell below, purely for clarity reasons
        results.append(vs)
    return results



# for each firm, individually take sentiment analytics
for i in all_firms:
    file_name = i +'.xlsx'
    dfirm = pd.read_excel(file_name)
    Tweet = dfirm['text']
    
    # sentiment analytics and combine the result with the original dataframe
    dfirm_results = pd.DataFrame(sentimentScore(Tweet))
    dfirm_tweets = pd.merge(dfirm, dfirm_results, left_index=True, right_index=True)
    
    
    # basic data cleaning and preprocessing
    dfirm_tweets = dfirm_tweets.drop('Unnamed: 0', axis=1) # delete the unnecessary column
    dfirm_tweets = dfirm_tweets[(dfirm_tweets[['compound']] != 0).all(axis=1)] #  delete the rows with zero 'compound' which is meaningless
    nan_rows = dfirm_tweets[dfirm_tweets['follower count'].isnull()] # delete the user without followers. 
    dfirm_tweets = dfirm_tweets[np.isfinite(dfirm_tweets['follower count'])] # delete the abnormal observations
    
    
    # Create new column with the 'compound' multiplied by number of followers of the account
    dfirm_tweets['Compound_multiplied'] = dfirm_tweets['compound']*dfirm_tweets['follower count']
    
    
    # scale the 'Compound_multiplied'
    x_1 = dfirm_tweets[['Compound_multiplied']].values.astype(float) # change the data type, from float64 to float

    scaler = StandardScaler().fit(x_1)
    scaled_data = scaler.transform(x_1)
    dfirm_tweets['Compound_multiplied_scaled'] = scaled_data
    
    
    # change the format of 'created at' column and set it to be the index
    dfirm_tweets['created at'] = dfirm_tweets['created at'].apply(lambda x: x.strftime('%Y-%m-%d'))  # delet the hour, minute and second
    dfirm_tweets = dfirm_tweets.set_index('created at') # set to be the index
    
    
    # calculate the daily mean of dfirm_tweets
    dfirm_daily_mean=(dfirm_tweets.groupby('created at').mean())
    
    
    
    # download the stock price from yahoo finance
    start = dt.datetime(2021, 2, 21)
    end =  dt.datetime(2021, 2, 28)
    stock = i[1:]
    
    df_stock = web.DataReader(stock, 'yahoo', start, end)
    
    
    # basic data cleaning and preprocessing
    df_stock['volatility'] = (df_stock['High'] - df_stock['Low']) / df_stock['Adj Close'] * 100.0  # generate the % spread based on the closing price
    
    df_stock['Pct_change'] = (df_stock['Close'] - df_stock['Open']) / df_stock['Open'] * 100.0   # generate the price change on the open price
    
    
    # scale the daily stock price change
    stock_1 = df_stock[['Pct_change']].values.astype(float)
    scaler = StandardScaler().fit(stock_1)
    scaled_data = scaler.transform(stock_1)
    df_stock['Pct_change_scaled'] = scaled_data
    
    
    # combining the tweet sentiment dataframe with stock price dataframe
    df_full = pd.concat([df_stock[['Volume','Adj Close','volatility','Pct_change', 'Pct_change_scaled']],\
                     dfirm_daily_mean], axis=1, sort=False)
        
    
    # Interpolate for missing weekend stock data
    df_full[[ "Volume", "Adj Close", "volatility", "Pct_change", "Pct_change_scaled"]] = \
    df_full[[ "Volume", "Adj Close", "volatility", "Pct_change", "Pct_change_scaled"]] \
    .interpolate(method='linear', limit_direction='forward', axis=0)
    
    
    # reducing the missing values
    start = dt.datetime(2021, 2, 19)
    end =  dt.datetime(2021, 2, 19)
    stock = i[1:]
    
    df_stock_19 = web.DataReader(stock, 'yahoo', start, end)
    for n in range(6):
        df_full.iloc[0,n] = df_stock_19.iloc[1,n]
    
    
    pd.DataFrame.describe(df_full)
    
    
    # Create 'label' -column for the forecast; 'Predicted_change' for the next day
    forecast_col = 'Pct_change'
    forecast_out = int(math.ceil(0.125 * len(df_full)))
    df_full['Predicted_change'] = df_full[forecast_col].shift(-forecast_out) # move upwards
    
    
    
    # Create another 'label' -column - 'Buy/Sell' - which is 1 if 'Predicted_change' is positive (=buy) and -1 if negative (=sell)
    buy_or_sell = []
    for row in df_full['Pct_change']:
        if row >= 0:
            buy_or_sell.append(1)
        elif row < 0:
            buy_or_sell.append(-1)
            
    df_full['Buy/Sell'] = buy_or_sell
    df_full['Buy/Sell'] = df_full['Buy/Sell'].shift(-1)
    
    
    # save to file
    file_name_end = i + '_for_ml' + '.xlsx'
    writer_df = pd.ExcelWriter(file_name_end, engine='xlsxwriter')
    df_full.to_excel(writer_df)
    writer_df.save()




# part 4
# merchine learning
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import MultinomialNB
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import cross_val_score
from sklearn import metrics 
from sklearn.neighbors import KNeighborsClassifier

result = []
df_result = pd.DataFrame()

# the train also will be done seperatly
for i in all_firms:
    file_name = i + '_for_ml' + '.xlsx'
    df_full = pd.read_excel(file_name)

    df_full_non = df_full.iloc[0:7,:]  # drop the last line since the 'Predicted_change' is NaN


    # define x and y
    x = np.array(df_full_non[['Compound_multiplied_scaled']]) 
    y = np.array(df_full_non['Buy/Sell'])


    # Split up the data into training and testing.
    count_train, count_test, y_train, y_test = \
        train_test_split(x, y, test_size=0.2, random_state=42)
        
        
    # using KNN - K-Nearest-Neighbors to train the model and check the accuracy
    
    neigh = KNeighborsClassifier(n_neighbors=5)
    neigh.fit(count_train, y_train) 
    accuracy_score = neigh.score(count_test, y_test)
    
    result.append(accuracy_score)

df_result['KNN'] = result
df_result['company'] = all_firms

writer_df = pd.ExcelWriter('result.xlsx', engine='xlsxwriter')
df_result.to_excel(writer_df)
writer_df.save()    












