# 投资参考数据
import tushare as ts
import traceback
from createdbengine import *
from stocklogger import *
import  datetime


# 获取业绩报告
def getvrdata(isinit='no'):
    mylogger = getmylogger()
    curday = datetime.date.today()
    print(curday)
    curyear = curday.year
    curmonth = curday.month
    quarter = (curmonth - 1) // 3 + 1
    # prdict = {'profitdivi': '分配预案', 'forecast': '业绩预告', 'xsg': '限售股解禁', 'fundholdings': '基金持股', 'newstocks': '新股'}
    prdict = {'fundholdings': '基金持股'}
    for dv in prdict:
        vrtbname = dv
        vrinfo = prdict[vrtbname]
        try:
            if dv == "profitdivi":
                df = ts.profit_data(year=curyear, top=100)
                if isinit == 'no' and df is not None:
                    df = df[df['report_date'] >= str(curday)]
            elif dv == "forecast":
                df = ts.forecast_data(curyear, quarter)
                if isinit == 'no' and df is not None:
                    # print(df[df['report_date'].isin(['2019-12-28'])])
                    df = df[df['report_date'] >= str(curday)]
            elif dv == "xsg":
                df = ts.xsg_data()
                if isinit == 'no' and df is not None:
                    curym = datetime.date.today().strftime("%Y-%m")
                    df = df[df['date'].str.contains(curym)]
            elif dv == "fundholdings":
                if quarter == 1:
                    quarter = 4
                    curyear -= 1
                else:
                    quarter -= 1
                df = ts.fund_holdings(curyear, quarter)
                if isinit == 'no' and df is not None:
                    lastyq = str(curyear) + '-' + str(quarter)
                    df = df[df['date'].str.contains(lastyq)]
                if df is not None:
                    newcolumns = ['nums', 'count', 'nlast', 'name', 'amount', 'date', 'ratio', 'code', 'clast']
                    df.columns = newcolumns
            elif dv == "newstocks":
                df = ts.new_stocks()
                if isinit == 'no' and df is not None:
                    df = df[df['ipo_date'] >= str(curday)]
            else:
                mylogger.info("没有执行命令。")
            if df is not None:
                tosql(df, vrtbname, "append", vrinfo, mylogger)
            else:
                mylogger.info("没有%s数据。" % vrinfo)
        except Exception:
            tracelog = traceback.format_exc()
            mylogger.info("获取数据异常。")
            mylogger.info(tracelog)


getvrdata(isinit='yes')
# getvrdata(isinit='no')
