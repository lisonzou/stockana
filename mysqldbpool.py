import MySQLdb
from DBUtils.PooledDB import PooledDB
from readconfig import get_config

def dbpool():
    host = get_config('DATABASE', 'host')
    port = int(get_config('DATABASE', 'port'))
    user = get_config('DATABASE', 'user')
    passwd = get_config('DATABASE', 'passwd')
    database = get_config('DATABASE', 'database')
    dbchar = get_config('DATABASE', 'dbchar')

    dbconfig = {
        # 指定数据库连接驱动
        'creator': MySQLdb,
        'host': host,
        'port': port,
        'user': user,
        'password': passwd,
        'db': database,
        'charset': dbchar,
        # 初始化时,连接池至少创建的空闲连接,0表示不创建
        'mincached': 1,
        # 连接池中空闲的最多连接数,0和None表示没有限制
        'maxcached': 20,
        # 连接池中最多共享的连接数量,0和None表示全部共享(其实没什么卵用)
        'maxshared':50,
        # 连接池允许的最大连接数,0和None表示没有限制
        'maxconnections': 70,
        # 连接池中如果没有可用共享连接后,是否阻塞等待,True表示等等待，False表示不等待然后报错
        'blocking':True,
        # ping Mysql服务器检查服务是否可用
        'ping':0,
    }

    pool = PooledDB(**dbconfig)
    return pool
