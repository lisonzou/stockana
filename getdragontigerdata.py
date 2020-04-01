#获取龙虎榜数据
import tushare as ts
import datetime
from createdbengine import *
from stocklogger import *


def getdragontigerdata():
    curday = datetime.date.today()
    curdate = curday.strftime('%Y%m%d')
    print(curdate)

    mylogger = getmylogger()

    # 每日龙虎榜列表
    df = ts.top_list(curday.strftime('%Y-%m-%d'))
    if df is not None:
        df['date'] = curdate
        tosql(df, 'toplistdata', "append", "每日龙虎榜数据", mylogger)
    else:
        mylogger.info("没有每日龙虎榜数据。")

    # 个股上榜统计
    for i in [5, 10, 30, 60]:
        df = ts.cap_tops(i)
        logmsg = "个股上榜数据" + "%d日：" % i
        if df is not None:
            df['date'] = curdate
            df['period'] = i
            tosql(df, 'captops', "append", logmsg, mylogger)
        else:
            mylogger.info("没有" + logmsg)

    # 营业部上榜统计
    for i in [5, 10, 30, 60]:
        df = ts.broker_tops(i)
        logmsg = "营业部上榜数据" + "%d日：" % i
        if df is not None:
            df['date'] = curdate
            df['period'] = i
            tosql(df, 'brokertops', "append", logmsg, mylogger)
        else:
            mylogger.info("没有" + logmsg)

    # 机构席位追踪
    for i in [5, 10, 30, 60]:
        df = ts.inst_tops(i)
        logmsg = "机构席位追踪数据" + "%d日：" % i
        if df is not None:
            df['date'] = curdate
            df['period'] = i
            tosql(df, 'instops', "append", logmsg, mylogger)
        else:
            mylogger.info("没有" + logmsg)

    # 机构成交明细
    df = ts.inst_detail()
    logmsg = "机构成交明细："
    if df is not None:
        df['date'] = curdate
        tosql(df, 'instdetail', "append", logmsg, mylogger)
    else:
        mylogger.info("没有机构成交明细。")


getdragontigerdata()
