from sqlalchemy import create_engine
from readconfig import get_config
import sqlalchemy


def createdbengine():
    host = get_config('DATABASE', 'host')
    port = int(get_config('DATABASE', 'port'))
    user = get_config('DATABASE', 'user')
    passwd = get_config('DATABASE', 'passwd')
    database = get_config('DATABASE', 'database')
    dbchar = get_config('DATABASE', 'dbchar')

    # engine = create_engine('mysql://stock:123@127.0.0.1/stock?charset=utf8')
    enginestr = 'mysql://' + user + ':' + passwd + '@' + host + ':' + str(port) + '/' + database + '?charset=' + dbchar
    dbengine = create_engine(enginestr)
    return dbengine


def tosql(df, tbname, action, logmsg, mylogger):
    dbengine = createdbengine()
    df.to_sql(tbname,
              dbengine,
              if_exists=action,
              dtype={'index': sqlalchemy.types.INTEGER(),
                     'name': sqlalchemy.types.NVARCHAR(length=16),
                     'code': sqlalchemy.types.NVARCHAR(length=8),
                     'area': sqlalchemy.types.NVARCHAR(length=8),
                     'industry': sqlalchemy.types.NVARCHAR(length=16),
                     'c_name': sqlalchemy.types.NVARCHAR(length=16),
                     'year': sqlalchemy.types.NVARCHAR(length=8),
                     'date': sqlalchemy.types.NVARCHAR(length=32),
                     'period': sqlalchemy.types.NVARCHAR(length=8),
                     'quarter': sqlalchemy.types.NVARCHAR(length=8),
                     'sheqratio': sqlalchemy.types.NVARCHAR(length=20),
                     'adratio': sqlalchemy.types.NVARCHAR(length=20),
                     'pchange': sqlalchemy.types.NVARCHAR(length=20),
                     'amount': sqlalchemy.types.NVARCHAR(length=20),
                     'buy': sqlalchemy.types.NVARCHAR(length=20),
                     'sell': sqlalchemy.types.NVARCHAR(length=20),
                     'bratio': sqlalchemy.types.NVARCHAR(length=20),
                     'sratio': sqlalchemy.types.NVARCHAR(length=20),
                     'report_date': sqlalchemy.types.NVARCHAR(length=32),
                     })

    mylogger.info(logmsg + tbname + "表：插入%s条数据。" % len(df))
