import os, configparser


def get_config(key, keyname):
    # 得到readConfig.py文件的上级目录C:\Users\songlihui\PycharmProjects\test001keshanchu\test_db
    path = os.path.split(os.path.realpath(__file__))[0]
    # print(path)
    # 得到配置文件目录，配置文件目录为path下的\config.ini
    config_path = os.path.join(path, 'config.ini')
    # 调用配置文件读取# 打印输出config_path测试内容是否正确
    config = configparser.ConfigParser()
    config.read(config_path, encoding='utf-8')
    # 通过config.get拿到配置文件中DATABASE的name的对应值
    value = config.get(key, keyname)
    return value


if __name__ == '__main__':
    print('通过config.get拿到配置文件中DATABASE的host的对应值:', get_config('DATABASE', 'host'))
