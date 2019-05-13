# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class FacebookadsItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    publish_date = scrapy.Field()  # 广告投放时间,时间戳格式
    video_img = scrapy.Field()  # 视频首帧图
    # imgSrc = scrapy.Field()  # 视频首帧图服务器保存地址
    origin_video_url = scrapy.Field()  # 视频地址
    # video_url = scrapy.Field()  # 视频服务器保存地址
    page_id = scrapy.Field()  # 主页id
    title = scrapy.Field()  # 标题
    country_id = scrapy.Field()  # 国家id,美国为1
    introduce = scrapy.Field()  # 广告介绍
    landpage = scrapy.Field()  # 广告商品链接
    # pageview_url = scrapy.Field()  # 视频预览地址
    spider_date = scrapy.Field()  # 第一次爬取时间
    adid = scrapy.Field()  # 广告id
    ad_archive_id = scrapy.Field()  # 广告存档id
    ad_creative_id = scrapy.Field()  # 广告创意id
    ad_md5 = scrapy.Field()  # 视频路径md5值,用于关联另一个表
    ad_type = scrapy.Field()  # 广告类型,0-视频,1-图片
    ads_url = scrapy.Field()  # 附加链接
    isDownload = scrapy.Field()  # 是否下载,0-未下载,1-下载
