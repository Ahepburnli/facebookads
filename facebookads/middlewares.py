# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.http import HtmlResponse, Response


class FacebookadsSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class FacebookadsDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        print('selenium开始执行...')
        # 获取requests中的url
        spider.driver.get(request.url)
        # 滚动
        js = "var q=document.documentElement.scrollTpo=1000"
        spider.driver.execute_script(js)
        # 获取页面源码
        origin_code = spider.driver.page_source
        time.sleep(0.1)
        # 构造response对象并返回
        response = HtmlResponse(url=request.url, encoding='utf-8', body=origin_code, request=request)
        print('返回response对象')
        return response

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


"""
实现随机的User-Agent
思路:
1. 准备User-Agent列表, 在settings.py中
2. 实现一个下载器中间件, 实现process_request方法
3. 在process_request方法中, 从User-Agent中, 随机取出一个User-Agent
4. 把这个User-Agent设置给requests的headers
"""

from facebookads.settings import USER_AGENT_LIST
import random


class RandomUserAgentDownloaderMiddleware(object):
    """随机user-Agent下载器中间件"""

    def process_request(self, request, spider):
        # 3. 在process_request方法中, 从User-Agent中, 随机取出一个User-Agent
        user_agent = random.choice(USER_AGENT_LIST)
        #  把这个User-Agent设置给requests的headers
        request.headers['User-Agent'] = user_agent
        
        
        
        
# from facebookads.settings import PROXY_LIST_OLD
from base64 import b64encode

class ProxyDownloaderMiddleware(object):

    def process_request(self, request, spider):

        # 代理服务器
        proxy = 'http://127.0.0.1:24000'
        # 取出对应ip和端口号, 代理地址
        request.meta['proxy'] = proxy
        # 如果有用户名和密码就需要对用户名和密码进行一个认证
        # user_pwd = ''
        #
        # user_pwd = b64encode(user_pwd.encode()).decode()
        # # 2. 设置请求的headers进行认证
        # request.headers['Proxy-Authorization'] = "Basic " + user_pwd

        '''
        import base64

""" 阿布云ip代理配置，包括账号密码 """
proxyServer = "http://http-dyn.abuyun.com:9020"
proxyUser = "HWFHQ5YP14Lxxx"
proxyPass = "CB8D0AD56EAxxx"
# for Python3
proxyAuth = "Basic " + base64.urlsafe_b64encode(bytes((proxyUser + ":" + proxyPass), "ascii")).decode("utf8")


class ABProxyMiddleware(object):
    """ 阿布云ip代理配置 """
    def process_request(self, request, spider):
        request.meta["proxy"] = proxyServer
        request.headers["Proxy-Authorization"] = proxyAuth
        '''


# 实现新版的随机代理IP

# from facebookads.settings import PROXIES_NEW
#
# class RandomProxyDownloaderMiddlewareNew(object):
#
#     def process_request(self, request, spider):
#
#         # 1. 取出请求请求的协议头
#         http = request.url.split('://')[0]
#         # 2. 获取http或https的代理列表
#         proxies = PROXIES_NEW.get(http)
#         # 3. 只有有对应代理列表, 才设置代理
#         if proxies:
#             # 随机取出一个代理
#             request.meta['proxy'] = random.choice(proxies)

