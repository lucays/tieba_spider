# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import MySQLdb
from MySQLdb import cursors

from .urls import get_old_urls
from .config import tieba_name, host, user, password, database

old_urls = get_old_urls(f'百度贴吧-{tieba_name}吧')


class TiebaSpiderPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect(host=host, user=user, password=password, database=database, charset='utf8mb4', cursorclass=cursors.SSCursor)
        self.cur = self.conn.cursor()

    def process_item(self, item, spider):
        # 由于未对帖子url去重，要在这里判断url in old_urls，来决定是insert还是update
        pass

    def close_spider(self, spider):
        pass
