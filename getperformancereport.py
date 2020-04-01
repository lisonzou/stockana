# 基本面数据
import tushare as ts
import traceback
import datetime
from createdbengine import *
from stocklogger import *


# 获取业绩报告
def getprdata(year, quarter):
    mylogger = getmylogger()
    prdict = {'report': '业绩报告', 'cashflow': '现金流量', 'debtpaying': '偿债能力', 'operation': '营运能力', 'profit': '盈利能力', 'growth': '成长能力'}
    for dv in prdict:
        prtbname = dv
        prinfo = prdict[prtbname]
        try:
            if dv == "report":
                df = ts.get_report_data(year, quarter)
            elif dv == "cashflow":
                df = ts.get_cashflow_data(year, quarter)
            elif dv == "debtpaying":
                df = ts.get_debtpaying_data(year, quarter)
            elif dv == "operation":
                df = ts.get_operation_data(year, quarter)
            elif dv == "profit":
                df = ts.get_profit_data(year, quarter)
            elif dv == "growth":
                df = ts.get_growth_data(year, quarter)
            else:
                mylogger.info("没有执行命令。")
            if df is not None:
                df['year'] = year
                df['quarter'] = quarter
                tosql(df, prtbname, "append", prinfo, mylogger)
            else:
                mylogger.info("没有%s数据。" % prinfo)
        except Exception:
            tracelog = traceback.format_exc()
            mylogger.info("获取数据异常。")
            mylogger.info(tracelog)


def gethistprdata(year, quarter):
    if year == 'All':
        curday = datetime.date.today()
        year = curday.strftime('%Y')
        for i in range(1990, int(year) + 1):
            for j in [1, 2, 3, 4]:
                getprdata(i, j)
    else:
        getprdata(year, quarter)


gethistprdata(2019, 1)
