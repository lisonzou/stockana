# 获取股票分类数据
import tushare as ts
from createdbengine import *
from stocklogger import *
import traceback


def getstockclassifieddata():
    mylogger = getmylogger()
    print("start")
    scdict = {'industry': '行业分类', 'concept': '概念分类', 'area': '地域分类', 'sme': '中小板分类', 'gem': '创业板分类', 'st': '风险警示板分类', 'hs300s': '沪深300成份及权重', 'sz50s': '上证50成份股', 'zz500s': '中证500成份股', 'terminated': '终止上市股票列表', 'suspended': '暂停上市股票列表'}

    for sc in scdict:
        sctbname = sc
        scinfo = scdict[sctbname]
        try:
            if sc == "industry":
                df = ts.get_industry_classified()
            elif sc == "concept":
                df = ts.get_concept_classified()
            elif sc == "area":
                df = ts.get_area_classified()
            elif sc == "sme":
                df = ts.get_sme_classified()
            elif sc == "gem":
                df = ts.get_gem_classified()
            elif sc == "st":
                df = ts.get_st_classified()
            elif sc == "hs300s":
                df = ts.get_hs300s()
            elif sc == "zz500s":
                df = ts.get_zz500s()
            elif sc == "sz50s":
                df = ts.get_sz50s()
            elif sc == "terminated":
                df = ts.get_terminated()
            elif sc == "suspended":
                df = ts.get_suspended()
            else:
                mylogger.info("没有执行命令。")
            if df is not None:
                tosql(df, sctbname, "replace", scinfo, mylogger)
            else:
                mylogger.info("没有%s数据。" % scinfo)
        except Exception:
            tracelog = traceback.format_exc()
            mylogger.info("获取数据异常。")
            mylogger.info(tracelog)


getstockclassifieddata()
