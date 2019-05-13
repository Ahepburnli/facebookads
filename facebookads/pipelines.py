# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import json

# class FacebookadsPipeline(object):
#
#     def open_spider(self, spider):
#         # 爬虫打开时打开文件
#         if spider.name == 'fbads':
#             self.file = open('fbads.jsonlines', 'w', encoding='utf8')
#
#     def process_item(self, item, spider):
#
#         if spider.name == 'fbads':
#             # 把item转化为字典
#             json.dump(dict(item), self.file, ensure_ascii=False)
#             # 换行
#             self.file.write('\n')
#         return item
#
#     def close_spider(self, spider):
#         # 爬虫关闭时关闭文件
#         if spider.name == 'fbads':
#             self.file.close()



'''
把数据存到mysql中,将数据增量去重
'''

from pymysql import *
import pandas as pd
import logging
from redis import Redis
from scrapy.exceptions import DropItem
import time
from facebookads.settings import REDIS_HOST, REDIS_PORT,REDIS_DB,REDIS_PASSWORD
from facebookads.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD, MYSQL_CHARSET



# from facebookads.settings import REDIS_URL


# redis_db = Redis(host='127.0.0.1', port=6379, db=1)  # 连接redis，相当于MySQL的conn


class FacebookadsPymysqlPipeline(object):

    def __init__(self):

        # self.redis_db = Redis(host='127.0.0.1', port=6379, password='Leite012068', db=1)
        # self.redis_db = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD)
        self.redis_db = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
        self.redis_data_dict = "adid"  # hash表的名字
        self.redis_db.delete(self.redis_data_dict, )  # 删除key，保证key为0，不然多次运行时候hlen不等于0。
        # print('只删除哈希')
        # self.conn = connect(host='shopifydata.cq1s6mjfadcw.us-east-1.rds.amazonaws.com',
        #                     port=3306,
        #                     database='facebook',
        #                     user='morningfast',
        #                     password='morningfast999',
        #                     charset='utf8'
        #                     )
        self.conn = connect(host=MYSQL_HOST,
                            port=MYSQL_PORT,
                            database=MYSQL_DB,
                            user=MYSQL_USER,
                            password=MYSQL_PASSWORD,
                            charset=MYSQL_CHARSET
                            )
        # 创建游标
        self.cs1 = self.conn.cursor()

        if self.redis_db.hlen(self.redis_data_dict) == 0:  #
            sql = "SELECT adid FROM fb_ads;"  # MySQL里提数据，取adid来去重.
            df = pd.read_sql(sql, self.conn)  # 读MySQL数据
            # print(df)
            # time.sleep(0.1)
            for adid in df['adid'].get_values():  # 把每一条的值写入hash的字段里
                # print(adid)
                adid = int(adid)  # 转换格式，不然redis不认
                self.redis_db.hset(self.redis_data_dict, adid, 6)  # 把字段的值都设为6后面对比的是字段，而不是值。

    # @pysnooper.snoop('/home/hepburn/桌面/insertredisandmysql.log')  # 可以指定日志输出位置
    def process_item(self, item, spider):
        if spider.name == 'fbads':
            if self.redis_db.hexists(self.redis_data_dict,
                                     item['adid']):  # 取item里的adid和key里的字段对比，看是否存在，存在就丢掉这个item。不存在返回item给后面的函数处理
                raise DropItem("已存在的adid的所有数据不再入库" )  # 并且不再往下执行

            # 若在adid在hash中不存在,存入mysql数据库
            insert_sql = "insert ignore into fb_ads (publish_date,video_img,origin_video_url,page_id,title,country_id,introduce,landpage,spider_date,adid,ad_archive_id,ad_creative_id,ad_md5,ad_type,ads_url) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            print('sql语句正确')
            # 增加鲁棒性
            try:
                self.cs1.execute(insert_sql,
                                 (item['publish_date'], item['video_img'], item['origin_video_url'], item['page_id'],
                                  item['title'], item['country_id'], item['introduce'], item['landpage'],
                                  item['spider_date'],
                                  item['adid'], item['ad_archive_id'], item['ad_creative_id'], item['ad_md5'],
                                  item['ad_type'], item['ads_url']
                                  ))

                self.conn.commit()
                # time.sleep(random.uniform(0.15, 0.25))  # 降低速度
                print('insert_success')
                logging.debug('存入mysql成功')
                # 若在adid在hash中不存在就 以 字符串 存入redis集合中,若要使用，读出后应转为字典。因为向redis存数据str优先。
                video_img = {'video_img': item['video_img'], 'adid': item['adid']}
                origin_video_url = {'origin_video_url': item['origin_video_url'], 'adid': item['adid']}
                self.redis_db.sadd('videoInfo', str(video_img))
                self.redis_db.sadd('videoInfo', str(origin_video_url))
                # time.sleep(1)
                print('存入redis')
                logging.debug('存入redis成功')
                # 提交操作

            except Exception as e:
                print(e)
                logging.debug(e)

            # 返回数据,否则后边pipeline得不到数据
        return item

    def close_spider(self, spider):
        if spider.name == 'fbads':
            # 当爬虫结束,关闭连接
            # 关闭游标
            self.cs1.close()
            # 关闭连接
            self.conn.close()


'''
虽然redis是一个键值对应的数据库，但这里为了速度用的是哈希(hash)
redis哈希结构：
结构：key field(字段) value
对应：redis_data_dict   adid(实际的adid)   6(代码里设置成了6)
而item里key是'adid' value是实际的adid
相当于用key(redis_data_dict)的字段(adid)来对比item['adid']的值，存在为1(true)，不存在就是0(false)
'''


# class FacebookadsSaveSpiderPipeline(object):
#     def __init__(self):
#         # self.redis_db = Redis(host='127.0.0.1', port=6379, db=1)
#         # self.redis_data_dict = "adid"  # hash表的名字
#         # self.redis_db.delete(self.redis_data_dict, )  # 删除key，保证key为0，不然多次运行时候hlen不等于0。
#         # # print('只删除哈希')
#         self.conn = connect(host='shopifydata.cq1s6mjfadcw.us-east-1.rds.amazonaws.com',
#                             port=3306,
#                             database='facebook',
#                             user='morningfast',
#                             password='morningfast999',
#                             charset='utf8'
#                             )
#         # 创建游标
#         self.cs1 = self.conn.cursor()
#
#     # @pysnooper.snoop('/home/hepburn/桌面/updatemysql.log')  # 可以指定日志输出位置
#     def process_item(self, item, spider):
#         if spider.name == 'saveSpider':
#             update_sql = "update fb_ads set is_download=%s where adid=%s"
#             # print('sql语句正确')
#             try:
#                 self.cs1.execute(update_sql, (item['isDownload'], item['adid']))
#                 print('update success---%s' % item['adid'])
#             except Exception as e:
#                 print(e)
#                 # 提交操作
#             self.conn.commit()
#
#         # return item
#
#     def close_spider(self, spider):
#         if spider.name == 'saveSpider':
#             # 当爬虫结束,关闭连接
#             # 关闭游标
#             self.cs1.close()
#             # 关闭连接
#             self.conn.close()
