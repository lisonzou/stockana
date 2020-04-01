import tushare as ts
from createdbengine import *
from stocklogger import *
import traceback


def getstockinfo():
    mylogger = getmylogger()

    try:
        df = ts.get_stock_basics()
        if df is not None:
            tosql(df, 'stockinfo', 'replace', '股票列表信息', mylogger)
        else:
            mylogger.info("没有股票列表信息数据。")
    except Exception:
        tracelog = traceback.format_exc()
        mylogger.info("获取数据异常。")
        mylogger.info(tracelog)


getstockinfo()
