# -*- coding: utf-8 -*-
import scrapy
import json
import time
import random
import re
import hashlib
import logging
from facebookads.items import FacebookadsItem
from pymysql import *
import pandas as pd
# from facebookads.settings import REDIS_URL
from redis import Redis
from facebookads.settings import REDIS_HOST, REDIS_PORT,REDIS_DB,REDIS_PASSWORD
from facebookads.settings import MYSQL_HOST, MYSQL_PORT, MYSQL_DB, MYSQL_USER, MYSQL_PASSWORD, MYSQL_CHARSET
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


redis_db = Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
redis_data_dict = 'pageid'


class FbadsSpider(scrapy.Spider):
    name = 'fbads'
    allowed_domains = ['facebook.com']
    # start_urls = ['http://facebook.com/']

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            'authority': 'www.facebook.com',
            # 请求报文可通过一个“Accept”报文头属性告诉服务端 客户端接受什么类型的响应。
            'accept': '*/*',
            # 指定客户端可接受的内容编码
            'accept-encoding': 'gzip, deflate',
            # 指定客户端可接受的语言类型
            'accept-language': 'zh-CN,zh;q=0.9',
            # 跨域的时候get，post都会显示origin，同域的时候get不显示origin，post显示origin，说明请求从哪发起，仅仅包括协议和域名
            'origin': 'https://www.facebook.com',
            # 表示这个请求是从哪个URL过来的，原始资源的URI
            'referer': 'https://www.facebook.com/ads/library',
            # 设置请求头信息User-Agent来模拟浏览器
            # 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            # cookie也是报文属性，传输过去
            # 'cookie': 'cna=/oN/DGwUYmYCATFN+mKOnP/h; tracknick=adimtxg; _cc_=Vq8l%2BKCLiw%3D%3D; tg=0; thw=cn; v=0; cookie2=1b2b42f305311a91800c25231d60f65b; t=1d8c593caba8306c5833e5c8c2815f29; _tb_token_=7e6377338dee7; CNZZDATA30064598=cnzz_eid%3D1220334357-1464871305-https%253A%252F%252Fmm.taobao.com%252F%26ntime%3D1464871305; CNZZDATA30063600=cnzz_eid%3D1139262023-1464874171-https%253A%252F%252Fmm.taobao.com%252F%26ntime%3D1464874171; JSESSIONID=8D5A3266F7A73C643C652F9F2DE1CED8; uc1=cookie14=UoWxNejwFlzlcw%3D%3D; l=Ahoatr-5ycJM6M9x2/4hzZdp6so-pZzm; mt=ci%3D-1_0',
            # 就是告诉服务器我参数内容的类型
            'Content-Type': 'application/json',
        }
    }

    # @pysnooper.snoop('/home/hepburn/桌面/fbadsstart_requests.log')  # 可以指定日志输出位置
    # 需要重写start_requests方法
    def start_requests(self):
        # 建立mysql数据库连接
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
        # 所有请求集合
        requests = []
        # 使用pandas链接数据库读取
        sql = 'select pageId from fb_homepage'
        # 从mysql中读取pageid
        df = pd.read_sql(sql, self.conn)
        # 取出的pageid存入redis
        # 遍历取出每一个pageId
        for page_id in df['pageId'].get_values():
            # print('这是pageid', page_id)
            # 构建ajax链接
            url = "https://www.facebook.com/ads/library/async/search_ads/?count=30&active_status=all&category=-1&countries[0]=US&view_all_page_id={}".format(
                page_id)
            # count是30的整数倍，用于取出下拉加载的网页数据
            # 网页里ajax链接
            # url = "https://www.facebook.com/ads/library/async/search_ads/?count=600&active_status=all&category=-1&countries[0]=US&view_all_page_id=243308806222171"
            # 封装post请求体参数,请求参数不用改变
            my_data = {'__user': 0,
                       '__a': 1,
                       # 'dyn': '7xe6FoO3-Q8zo5Obx679uC1swgE98nwgU7SbzEdF8aUuw92ewXx63C260Y8hw8C5o4m0nCq1ewcG14wtoswDwb62W2y11xmcG0nSV8y1vwiE3Jgao88vwa2',
                       '__req': 1,
                       '__be': 1,
                       '__pc': 'PHASED:DEFAULT',
                       'dpr': 1,
                       # '__rev': 1000587325,
                       '__comet_req': 'false',
                       # 'lsd': 'AVpDSdPR',
                       # 'jazoest': 2676
                       }
            # 模拟ajax发送post请求
            request = scrapy.Request(url, method='POST',
                                     callback=self.parse,
                                     body=json.dumps(my_data),
                                     encoding='utf-8')
            requests.append(request)
        return requests

    # def __init__(self):
    #     # 创建Options对象
    #     opt = Options()
    #     # 实际爬取中设置无头提高效率
    #     opt.set_headless()
    #     opt.add_argument('--no-sandbox')
    #     opt.add_argument('--disable-dev-shm-usage')
    #     opt.add_argument('--headless')
    #     opt.add_argument('blink-settings=imagesEnabled=false')
    #     opt.add_argument('--disable-gpu')
    #     # 创建chrome驱动对象
    #     self.driver = webdriver.Chrome(options=opt)

    # @pysnooper.snoop('/home/hepburn/桌面/fbads.log')  # 可以指定日志输出位置
    def parse(self, response):
        # 可以利用json库解析返回来得数据，在此省略
        # jsonBody = json.loads(response.body)
        url = response.url
        print(response.url)
        # print('一次')
        jsonBody = response.body.decode('utf-8')
        # print(jsonBody)
        totalCount = json.loads(jsonBody.strip('for (;;);'))['payload']['totalCount']
        totalCount = int(totalCount)
        if totalCount > 30:
            # 替换coun=30为相应的count
            count = ((totalCount // 30) + 1) * 30
            a = 'count=%s' % count
            url = url.replace('count=30', a)

            my_data = {'__user': 0,
                       '__a': 1,
                       # 'dyn': '7xe6FoO3-Q8zo5Obx679uC1swgE98nwgU7SbzEdF8aUuw92ewXx63C260Y8hw8C5o4m0nCq1ewcG14wtoswDwb62W2y11xmcG0nSV8y1vwiE3Jgao88vwa2',
                       '__req': 1,
                       '__be': 1,
                       '__pc': 'PHASED:DEFAULT',
                       'dpr': 1,
                       # '__rev': 1000587325,
                       '__comet_req': 'false',
                       # 'lsd': 'AVpDSdPR',
                       # 'jazoest': 2676
                       }
            # print('转到parse_more')

            yield scrapy.Request(url, method='POST',
                                 callback=self.parse_more,
                                 body=json.dumps(my_data),
                                 encoding='utf-8')

        # 这是包含广告信息的列表
        ads = json.loads(jsonBody.strip('for (;;);'))['payload']['results']
        # print(ads)
        # 遍历ads取出每一个广告数据
        for ad in ads:
            # 创建item对象
            item = FacebookadsItem()
            # ad 会出现空值的情况，此时是没有数据的
            if not ad:
                item['publish_date'] = 0
                item['video_img'] = None
                item['origin_video_url'] = None
                item['page_id'] = 0
                item['title'] = None
                item['country_id'] = 1
                item['introduce'] = None
                item['landpage'] = None
                item['adid'] = 0
                item['ad_archive_id'] = 0
                item['ad_creative_id'] = 0
                item['ad_md5'] = None
                item['ad_type'] = 0
                item['ads_url'] = None
            else:

                try:
                    # 广告投放时间, 时间戳格式
                    item['publish_date'] = ad[0]['startDate']
                except Exception as e:
                    print(e)  # 数据被清空
                    item['publish_date'] = 0

                try:
                    # 广告id
                    item['adid'] = ad[0]['adid']
                except Exception as e:
                    print(e, '没有广告数据')
                    item['adid'] = 0
                # 广告存档id
                item['ad_archive_id'] = ad[0]['adArchiveID']
                try:
                    # 广告创意id
                    item['ad_creative_id'] = ad[0]['snapshot']['ad_creative_id']
                except Exception as e:
                    print(e)
                    item['ad_creative_id'] = 0
                # 主页id
                item['page_id'] = ad[0]['pageID']
                try:
                    # 广告介绍
                    introduce = ad[0]['snapshot']['body']['markup']['__html']
                    item['introduce'] = introduce.replace('<br />', '')
                except Exception as e:
                    print(e)
                    item['introduce'] = None
                # 国家id,默认美国为1
                item['country_id'] = 1
                try:
                    # 广告类型,部分广告本地区查看不了，此数据也获取不到
                    type = ad[0]['snapshot']['display_format']
                except Exception as e:
                    print(e, ':没有广告数据')
                    type = None
                # cards = ad[0]['snapshot']['cards']
                if type == 'video':
                    # 此时是视频数据
                    # 广告类型
                    item['ad_type'] = 0
                    # 标题
                    item['title'] = ad[0]['snapshot']['title']
                    try:
                        # 视频首帧图
                        item['video_img'] = ad[0]['snapshot']['videos'][0]['video_preview_image_url']
                    except Exception as e:
                        print(e)
                        item['video_img'] = None
                    # 视频地址
                    item['origin_video_url'] = ad[0]['snapshot']['videos'][0]['video_sd_url']
                    # 广告商品链接
                    item['landpage'] = ad[0]['snapshot']['link_url']
                    # 视频地址
                    str = ad[0]['snapshot']['videos'][0]['video_sd_url']
                    # 提取的视频路径作为加密对象
                    urlmap = re.findall('\d+_\d+_\d+_n', str)[0]
                    # 创建md5对象
                    m = hashlib.md5()
                    # 将urlmap转为二进制
                    b = bytes(urlmap, encoding='utf-8')
                    m.update(b)
                    urlmap_md5 = m.hexdigest()
                    # 视频路径md5值,用于关联另一个表
                    item['ad_md5'] = urlmap_md5
                    # 附加链接为空
                    item['ads_url'] = None

                elif type == 'image':
                    # 此时是图片数据
                    # 标题
                    item['title'] = ad[0]['snapshot']['title']
                    try:
                        # 视频首帧图
                        item['video_img'] = ad[0]['snapshot']['images'][0]['original_image_url']
                        # 提取的图片路径作为md5
                        imgmap = re.findall('\d+_\d+_\d+_n', item['video_img'])[0]
                        # 创建md5对象
                        m = hashlib.md5()
                        # 将urlmap转为二进制
                        b = bytes(imgmap, encoding='utf-8')
                        m.update(b)
                        imgmap_md5 = m.hexdigest()
                        # 视频路径md5值,用于关联另一个表
                        item['ad_md5'] = imgmap_md5
                    except Exception as e:
                        print(e)
                        item['video_img'] = None
                        item['ad_md5'] = None
                    # 广告类型
                    item['ad_type'] = 1
                    # 广告商品链接
                    item['landpage'] = ad[0]['snapshot']['link_url']
                    # 附加链接为空
                    item['ads_url'] = None
                    # 视频链接为空
                    item['origin_video_url'] = None

                else:

                    # 广告类型
                    item['ad_type'] = 1
                    try:
                        # 数据在cards中,得到一个列表
                        cards = ad[0]['snapshot']['cards']
                    except Exception as e:
                        print(e, ':没有广告数据')
                    try:
                        # 标题
                        cards = ad[0]['snapshot']['cards']
                        title = cards[3]['title']
                        item['title'] = title
                    except Exception as e:
                        print(e)
                        item['title'] = ad[0]['snapshot']['title']  # 或者是None

                    try:
                        # 视频首帧图
                        cards = ad[0]['snapshot']['cards']
                        item['video_img'] = cards[3]['original_image_url']
                        # 提取的图片路径作为md5
                        imgmap = re.findall('\d+_\d+_\d+_n', item['video_img'])[0]
                        # 创建md5对象
                        m = hashlib.md5()
                        # 将urlmap转为二进制
                        b = bytes(imgmap, encoding='utf-8')
                        m.update(b)
                        imgmap_md5 = m.hexdigest()
                        # 视频路径md5值,用于关联另一个表
                        item['ad_md5'] = imgmap_md5
                    except Exception as e:
                        print(e)
                        item['video_img'] = None
                        item['ad_md5'] = None

                    try:
                        # 广告商品链接
                        cards = ad[0]['snapshot']['cards']
                        item['landpage'] = cards[3]['link_url']
                    except Exception as e:
                        print(e)
                        item['landpage'] = ad[0]['snapshot']['link_url']
                    # 视频链接为空
                    item['origin_video_url'] = None
                    try:
                        # 建立空列表存放附加链接
                        cards = ad[0]['snapshot']['cards']
                        ads_urls = []
                        ads_urls.append(cards[0]['original_image_url'])
                        ads_urls.append(cards[1]['original_image_url'])
                        ads_urls.append(cards[2]['original_image_url'])
                        ads_urls.append(cards[4]['original_image_url'])
                        ads_urls = ';'.join(ads_urls)
                        # print(ads_urls)
                        item['ads_url'] = ads_urls
                    except Exception as e:
                        print(e)
                        item['ads_url'] = None

            # 第一次爬取时间
            item['spider_date'] = int(time.time())


            yield item

    # @pysnooper.snoop('/home/hepburn/桌面/fbadsmore30.log')  # 可以指定日志输出位置
    def parse_more(self, response):
        '''
        它的首行简述函数功能，第二行空行，第三行为函数的具体描述。用于获取广告数量大于30的页面，为了不造成死循环另定义一个方法。

        具体方法与parse相同。
        :param response:
        :return:
        '''
        jsonBody = response.body.decode('utf8')
        # 这是包含广告信息的列表
        ads = json.loads(jsonBody.strip('for (;;);'))['payload']['results']
        # print(ads)
        # 遍历ads取出每一个广告数据
        for ad in ads:
            # 创建item对象
            item = FacebookadsItem()
            # ad 会出现空值的情况，此时是没有数据的
            if not ad:
                item['publish_date'] = 0
                item['video_img'] = None
                item['origin_video_url'] = None
                item['page_id'] = 0
                item['title'] = None
                item['country_id'] = 1
                item['introduce'] = None
                item['landpage'] = None
                item['adid'] = 0
                item['ad_archive_id'] = 0
                item['ad_creative_id'] = 0
                item['ad_md5'] = None
                item['ad_type'] = 0
                item['ads_url'] = None
            else:
                try:
                    # 广告投放时间, 时间戳格式
                    item['publish_date'] = ad[0]['startDate']
                except Exception as e:
                    print(e)  # 数据被清空
                    item['publish_date'] = 0
                try:
                    # 广告id
                    item['adid'] = ad[0]['adid']
                except Exception as e:
                    print(e, '没有广告数据')
                    item['adid'] = 0
                # 广告存档id
                item['ad_archive_id'] = ad[0]['adArchiveID']
                try:
                    # 广告创意id
                    item['ad_creative_id'] = ad[0]['snapshot']['ad_creative_id']
                except Exception as e:
                    print(e)
                    item['ad_creative_id'] = 0
                # 主页id
                item['page_id'] = ad[0]['pageID']
                try:
                    # 广告介绍
                    introduce = ad[0]['snapshot']['body']['markup']['__html']
                    item['introduce'] = introduce.replace('<br />', '')
                except Exception as e:
                    print(e)
                    item['introduce'] = None
                # 国家id,默认美国为1
                item['country_id'] = 1
                try:
                    # 广告类型,部分广告本地区查看不了，此数据也获取不到
                    type = ad[0]['snapshot']['display_format']
                except Exception as e:
                    print(e, ':没有广告数据')
                    type = None
                # cards = ad[0]['snapshot']['cards']
                if type == 'video':
                    # 此时是视频数据
                    # 广告类型
                    item['ad_type'] = 0
                    # 标题
                    item['title'] = ad[0]['snapshot']['title']
                    try:
                        # 视频首帧图
                        item['video_img'] = ad[0]['snapshot']['videos'][0]['video_preview_image_url']
                    except Exception as e:
                        print(e)
                        item['video_img'] = None
                    # 视频地址
                    item['origin_video_url'] = ad[0]['snapshot']['videos'][0]['video_sd_url']
                    # 广告商品链接
                    item['landpage'] = ad[0]['snapshot']['link_url']
                    # 视频地址
                    str = ad[0]['snapshot']['videos'][0]['video_sd_url']
                    # 提取的视频路径作为加密对象
                    urlmap = re.findall('\d+_\d+_\d+_n', str)[0]
                    # 创建md5对象
                    m = hashlib.md5()
                    # 将urlmap转为二进制
                    b = bytes(urlmap, encoding='utf-8')
                    m.update(b)
                    urlmap_md5 = m.hexdigest()
                    # 视频路径md5值,用于关联另一个表
                    item['ad_md5'] = urlmap_md5
                    # 附加链接为空
                    item['ads_url'] = None

                elif type == 'image':
                    # 此时是图片数据
                    # 标题
                    item['title'] = ad[0]['snapshot']['title']
                    try:

                        # 视频首帧图
                        item['video_img'] = ad[0]['snapshot']['images'][0]['original_image_url']
                        # 提取的图片路径作为md5
                        imgmap = re.findall('\d+_\d+_\d+_n', item['video_img'])[0]
                        # 创建md5对象
                        m = hashlib.md5()
                        # 将urlmap转为二进制
                        b = bytes(imgmap, encoding='utf-8')
                        m.update(b)
                        imgmap_md5 = m.hexdigest()
                        # 视频路径md5值,用于关联另一个表
                        item['ad_md5'] = imgmap_md5
                    except Exception as e:
                        print(e)
                        item['video_img'] = None
                        item['ad_md5'] = None
                    # 广告类型
                    item['ad_type'] = 1
                    # 广告商品链接
                    item['landpage'] = ad[0]['snapshot']['link_url']
                    # 附加链接为空
                    item['ads_url'] = None
                    # 视频链接为空
                    item['origin_video_url'] = None

                else:
                    # 此时是图片数据 'display_format': 'dpa'
                    # 广告类型
                    item['ad_type'] = 1
                    try:
                        # 数据在cards中,得到一个列表
                        cards = ad[0]['snapshot']['cards']
                    except Exception as e:
                        print(e, ':没有数据')
                    try:
                        # 标题
                        cards = ad[0]['snapshot']['cards']
                        title = cards[3]['title']
                        item['title'] = title
                    except Exception as e:
                        print(e)
                        item['title'] = ad[0]['snapshot']['title']  # 或者是None

                    try:
                        # 视频首帧图
                        cards = ad[0]['snapshot']['cards']
                        item['video_img'] = cards[3]['original_image_url']
                        # 提取的图片路径作为md5
                        imgmap = re.findall('\d+_\d+_\d+_n', item['video_img'])[0]
                        # 创建md5对象
                        m = hashlib.md5()
                        # 将urlmap转为二进制
                        b = bytes(imgmap, encoding='utf-8')
                        m.update(b)
                        imgmap_md5 = m.hexdigest()
                        # 视频路径md5值,用于关联另一个表
                        item['ad_md5'] = imgmap_md5
                    except Exception as e:
                        print(e)
                        item['video_img'] = None
                        item['ad_md5'] = None
                    try:
                        # 广告商品链接
                        cards = ad[0]['snapshot']['cards']
                        item['landpage'] = cards[3]['link_url']
                    except Exception as e:
                        print(e)
                        item['landpage'] = ad[0]['snapshot']['link_url']
                    # 视频链接为空
                    item['origin_video_url'] = None
                    try:
                        # 建立空列表存放附加链接
                        cards = ad[0]['snapshot']['cards']
                        ads_urls = []
                        ads_urls.append(cards[0]['original_image_url'])
                        ads_urls.append(cards[1]['original_image_url'])
                        ads_urls.append(cards[2]['original_image_url'])
                        ads_urls.append(cards[4]['original_image_url'])
                        ads_urls = ';'.join(ads_urls)
                        # print(ads_urls)
                        item['ads_url'] = ads_urls
                    except Exception as e:
                        print(e)
                        logging.debug(e)
                        item['ads_url'] = None

            # 第一次爬取时间
            item['spider_date'] = int(time.time())


            yield item

    # print(parse_more.__doc__)  # 与def对齐
