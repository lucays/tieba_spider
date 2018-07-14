import datetime
import traceback
import MySQLdb
from MySQLdb import cursors
from .log_config import logger
from .config import host, user, password, database, tablename

today = datetime.datetime.strftime(datetime.datetime.now(), '%Y%m%d')
yesterday = datetime.datetime.strftime(datetime.datetime.now()+datetime.timedelta(-1), '%Y%m%d')


def get_old_urls(name) ->set:
    '''
    获取投资数据库所有某个贴吧的url
    :return dict {url: id_}
    '''
    conn = MySQLdb.connect(host=host, user=user, password=password, database=database, charset='utf8mb4', cursorclass=cursors.SSCursor)
    try:
        with conn.cursor() as cur:
            sql = "select id, link from %s where media=%s"
            cur.execute(sql, (tablename, name))
            r = {url: i for i, url in cur}
            return r
    except:
        logger.exception('select old urls error')
        traceback.print_exc()
    finally:
        conn.close()
