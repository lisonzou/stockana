import tushare as ts
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import mysql.connector
import re, time, datetime
import MySQLdb

startday = '2019-10-09'
df = ts.get_hist_data('688068', ktype='15', start=startday, end=startday)
if df is not None:
    if not df.empty:
        print("empty")
# if df.empty: print('empty')
print(df)

exit(0)
# df = ts.get_stock_basics()
# print(len(df))

startday = "2019-10-09"
starttime = startday + " 09:30:00"
endtime = startday + "15:00:00"

print(startday)

print("开始获取数据时间：" + str(datetime.datetime.now()))
df7 = ts.get_hist_data('000002', ktype='D', start=startday, end=startday)
df7['period'] = 'D'
print(df7)
df1 = ts.get_hist_data('000002', ktype='W', start=startday, end=startday)
df1['period'] = 'W'
print(df1)
df2 = ts.get_hist_data('000002', ktype='M', start=startday, end=startday)
df2['period'] = 'M'
print(df2)
print(starttime)
df3 = ts.get_hist_data('000002', ktype='5', start=starttime, end=endtime)
df3['period'] = '5'
print(df3)
df4 = ts.get_hist_data('000002', ktype='15', start=starttime, end=endtime)
df4['period'] = '15'
print(df4)
df5 = ts.get_hist_data('000002', ktype='30', start=starttime, end=endtime)
df5['period'] = '30'
print(df5)
df6 = ts.get_hist_data('000002', ktype='60', start=starttime, end=endtime)
df6['period'] = '60'
print(df6)
pd1 = pd.concat([df7, df1, df2, df3, df4, df5, df6],axis=0, ignore_index=False, sort=True)
pd1['code'] = '000001'
pd1['name'] = '万科A'
print(pd1)

exit(0)
print("结束获取数据时间：" + str(datetime.datetime.now()))
# print(pd1)

"""
stocks = ['000066', '002057', '300368']

# exit(0)
# data = ts.get_k_data('300001', ktype='5')
data = ts.get_realtime_quotes(stocks[0:3])
print(type(data))

if data is not None:
    print(data)
else:
    print(data)

exit(0)
engine = create_engine('mysql://stock:123@127.0.0.1/stock?charset=utf8')

weekdata = ts.get_k_data('300008', ktype='D')
weekdata.to_sql('stock' + '300008',
                engine,
                if_exists='replace',
                dtype={'date': sqlalchemy.DATETIME()
                       })

exit(0)
"""


mysqlconfig = dict(host='127.0.0.1', user='root', password='123', port=3306, database='stock', charset='utf8')
conn = mysql.connector.connect(**mysqlconfig)
cursor = conn.cursor()
# cursor.execute('create table stock_' + '688068' + ' (date varchar(32),open varchar(32),close varchar(32),high varchar(32),low varchar(32),volume varchar(32),p_change varchar(32),unique(date))')
# 利用tushare包获取单只股票的阶段性行情
# df = ts.get_hist_data('688068', ktype= '5')
print(len(pd1))
df = pd1
# sql = "insert into stock_" + "000002" + " (date,open,close,high,low,volume,p_change,period) values ('%s','%s','%s','%s','%s','%s','%s','%s')"

print("开始插入数据时间：" + str(datetime.datetime.now()))

# cursor.executemany(sql,*df)

for i in range(0, len(df)):
    # print(df.open[i],df.close[i],df.high[i])
    sql = "insert into stock_" + "000002" + " (date,open,close,high,low,volume,p_change,period) values ('%s','%s','%s','%s','%s','%s','%s','%s')" % (
                                       str(df.index[i]), df.open[i], df.close[i], df.high[i], df.low[i], df.volume[i],
                                       df.p_change[i], df.period[i])
    # print(sql)
    cursor.execute(sql)

conn.commit()
cursor.close()
conn.close()

print("结束插入数据时间：" + str(datetime.datetime.now()))
# 统计总共插入了多少张表的数据
print('所有股票总共插入1数据库表格')
