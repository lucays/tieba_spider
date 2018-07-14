# -*- coding: utf-8 -*-
import datetime
import json

import scrapy
from lxml import etree

from tieba_spider.config import tieba_name
from tieba_spider.log_config import logger
from tieba_spider.items import TiebaSpiderItem
from tieba_spider.urls import get_old_urls, today, yesterday

old_urls = get_old_urls(f'百度贴吧-{tieba_name}吧')


class TiebaSpider(scrapy.Spider):
    name = 'tieba'
    allowed_domains = ['tieba.baidu.com']
    start_urls = [f'http://tieba.baidu.com/f?kw={tieba_name}&pn=0']

    def is_recently(self, time):
        # 判断发帖日期是否大于yesterday
        if ':' in time:
            return True
        return True if datetime.datetime.strptime(time, '%m-%d').strftime('%m%d') >= yesterday[4:] else False

    def parse(self, response):
        # 爬取目标贴吧每页的帖子url
        url = response.url
        text = []
        for i in response.text.split('<!--'):
            text.extend([j for j in i.split('-->') if len(j) > 200 and '最后回复时间' in j])

        h = etree.HTML(text[0])

        article_urls = ['http://tieba.baidu.com'+i for i in h.xpath('//div[@class="threadlist_title pull_left j_th_tit "]/a/@href')]
        article_times = [i.strip() for i in h.xpath('//span[@title="最后回复时间"]/text()')]

        if self.is_recently(article_times[-1]):
            for article_url, article_time in zip(article_urls, article_times):
                if ':' in article_time:
                    yield scrapy.Request(article_url, meta={'date': today}, callback=self.article_parse)
                else:
                    yield scrapy.Request(article_url, callback=self.article_parse)
            current_pn = int(url.split('pn=')[1])
            next_page_url = f'http://tieba.baidu.com/f?kw={self.tieba_name}&pn={current_pn+50}'
            yield scrapy.Request(next_page_url, callback=self.parse)
        else:
            for article_url, article_time in zip(article_urls, article_times):
                if ':' in article_time:
                    yield scrapy.Request(article_url, meta={'date': today}, callback=self.article_parse)
                else:
                    date = datetime.datetime.strptime(article_time, '%m-%d').strftime('%m%d')
                    if date >= yesterday[4:]:
                        yield scrapy.Request(article_url, callback=self.article_parse)

    def article_parse(self, response):
        '''
        爬取帖子的第一页（不包含楼中楼）
        '''
        url = response.url
        logger.info(f'now {url} fetched')
        tie_id = url.split('/')[-1]
        if 'date' in response.meta:
            date = response.meta['date']
        else:
            # 部分贴吧是dates = [i for i in response.xpath('//span[@class="tail-info"]/text()').extract() if len(i) > 6]
            date = datetime.datetime.strptime(json.loads(response.xpath('//div[@class="l_post j_l_post l_post_bright noborder "]/@data-field').extract()[0])['content']['date'], '%Y-%m-%d %H:%M').strftime('%Y%m%d')
            other_post_dates = [datetime.datetime.strptime(json.loads(i)['content']['date'], '%Y-%m-%d %H:%M').strftime('%Y%m%d') for i in response.xpath('//div[@class="l_post j_l_post l_post_bright  "]/@data-field').extract()]
            if other_post_dates:
                date = max(other_post_dates)
        title = response.xpath('//h1[@class="core_title_txt  "]/text()').extract()[0]

        item = TiebaSpiderItem()
        item['url'] = url
        item['title'] = title
        if 'date' in response.meta:
            item['date'] = response.meta['date']
        else:
            item['date'] = date
        item['media'] = f'百度贴吧-{tieba_name}吧'
        item['content'] = []

        page_count = int(response.xpath('//li[@class="l_reply_num"]/span/text()').extract()[-1])
        page_counts = [1 for i in range(page_count)]
        page_counts.pop()
        if page_count > 1:
            next_page_url = url + '?pn=2'
            yield scrapy.Request(next_page_url, meta={'item': item, 'page_counts': page_counts}, callback=self.next_page_article_parse)

        building_datas = response.xpath('//div[@class="d_post_content j_d_post_content  clearfix"]')
        building_nums = [1 for i in range(len(building_datas))]
        for l, i in enumerate(building_datas):
            item['content'].append(i.xpath('string(.)').extract()[0].strip())
            inside_building_id = i.xpath('@id').extract()[0].split('_')[-1]
            inside_building_url = f'https://tieba.baidu.com/p/comment?tid={tie_id}&pid={inside_building_id}&pn=1'
            yield scrapy.Request(inside_building_url, meta={'item': item, 'l': l, 'page_counts': page_counts, 'inside_nums': building_nums}, callback=self.inside_building_parse)

    def inside_building_parse(self, response):
        '''
        爬取楼中楼
        '''
        url = response.url
        tmp, current_pn = url.split('pn=')
        item = response.meta['item']
        inside_content = ''.join('\n'+i.strip() for i in response.xpath('//span[@class="lzl_content_main"]/text()').extract())

        if inside_content:
            item['content'][response.meta['l']] += inside_content
        inside_dates = [datetime.datetime.strptime(i, '%Y-%m-%d %H:%M').strftime('%Y%m%d') for i in response.xpath('//span[@class="lzl_time"]/text()').extract()]
        if inside_dates:
            inside_date = max(inside_dates)
            if item['date'] < inside_date:
                item['date'] = inside_date
        next_inside_building_page_datas = response.xpath('//p[@class="j_pager l_pager pager_theme_2"]/a[contains(text(),"下一页")]/@href').extract()
        if next_inside_building_page_datas:
            next_inside_building_page_url = tmp + f'pn={int(current_pn)+1}'
            yield scrapy.Request(next_inside_building_page_url, meta={'item': item, 'l': response.meta['l'], 'page_counts': response.meta['page_counts'], 'inside_nums': response.meta['inside_nums']}, callback=self.inside_building_parse)
        if url.split('pn=')[1] == '1':
            # 楼中楼翻页时inside_nums不应减
            response.meta['inside_nums'].pop()

        # 爬完标志是True，楼中楼没有下一页，yield item
        if not next_inside_building_page_datas and not response.meta['page_counts'] and (not response.meta['inside_nums']):
            yield item

    def next_page_article_parse(self, response):
        '''
        爬取帖子的下一页
        '''
        url = response.url
        tie_id = url.split('/')[-1].split('?')[0]
        item = response.meta['item']
        item['date'] = max(datetime.datetime.strptime(json.loads(i)['content']['date'], '%Y-%m-%d %H:%M').strftime('%Y%m%d') for i in response.xpath('//div[@class="l_post j_l_post l_post_bright  "]/@data-field').extract())

        response.meta['page_counts'].pop()
        page_count = int(response.xpath('//li[@class="l_reply_num"]/span/text()').extract()[-1])
        current_pn = int(url.split('pn=')[1])
        if current_pn < page_count:
            next_page_url = url.split('pn=')[0] + f'pn={current_pn+1}'
            logger.info(next_page_url)
            yield scrapy.Request(next_page_url, meta={'item': item, 'page_counts': response.meta['page_counts']}, callback=self.next_page_article_parse)

        building_datas = response.xpath('//div[@class="d_post_content j_d_post_content  clearfix"]')
        building_nums = [1 for i in range(len(building_datas))]
        for l, i in enumerate(building_datas):
            item['content'].append(i.xpath('string(.)').extract()[0].strip())
            inside_building_id = i.xpath('@id').extract()[0].split('_')[-1]
            inside_building_url = f'https://tieba.baidu.com/p/comment?tid={tie_id}&pid={inside_building_id}&pn=1'
            yield scrapy.Request(inside_building_url, meta={'item': item, 'l': len(item['content']) - 1, 'page_counts': response.meta['page_counts'], 'inside_nums': building_nums}, callback=self.inside_building_parse)
