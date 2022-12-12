# -*- coding: utf-8 -*-
"""
Created on  Nov 30 16:00:002022
福州所有小区的名称及地址
@author: Yza
"""
from random import random
import time
import requests
from lxml import etree
import pandas as pd

class Spider_xq:
    def generate_random_str(self, randomlength=16):  # 生成随机字符串
        """
        生成一个指定长度的随机字符串
        """
        random_str = ''
        base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789'
        length = len(base_str) - 1
        for i in range(randomlength):
            random_str += base_str[random.randint(0, length)]
        return random_str

    def proxies(self):
        (username, password, ip, port) = ('13261692613', 'ylkj_12311', '140.249.73.234', '15033')
        proxies = {'http': f'http://{username}:{password}@{ip}:{port}',
                   'https': f'http://{username}:{password}@{ip}:{port}'}
        return proxies

    def query(self, url):  # 读取网页数据
        retried = 0
        while retried < 10:
            # 修改头文件避免反爬虫
            headers = {"Referer": "http://www.baidu.com/",
                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                    }
            try:
                result = requests.get(
                    headers=headers, url=url, timeout=5, proxies=self.proxies())
                break
            except:  # 异常
                retried = retried + 1
                if retried < 10:
                    print('正在重试')
                else:
                    print('读取数据失败')
                time.sleep(1)
        return result

    def parseurl(self, html, xpath):
        e_html = etree.HTML(html)
        text = e_html.xpath(xpath)
        return text

    def clear(self, list_item):
        clear_list = []
        for list_value in list_item:
            list = list_value.replace("\n", "").replace(" ","").replace("(","").replace(")","")
            clear_list.append(list)
        return clear_list

if __name__ == '__main__':
    titles = []
    addresses = []
    citys = []
    baseurl = 'https://fz.ke.com/xiaoqu'
    url_list = ['/gulouqu3/','/taijiangqu1/','/jinanqu1/','/maweiqu1/','/cangshanqu1/','/minhouxian/','/lianjiangxian/','/pingtanxian/','/fuqingshi/','/changlequ1/']
    page = [43,17,23,7,22,10,4,2,8,7]
    city = ['鼓楼区','台江区','晋安区','马尾区','仓山区','闽侯县','连江县','平潭县','福清市','长乐区']
    spider = Spider_xq()
    for i in range(0,2):
        for j in range(1, page[i] + 1):
            url = baseurl + url_list[i] + '/pg' + str(j) + '/'
            xpath = '//*[@id="beike"]/div[1]/div[4]/div[1]/div[3]/ul/li/div[@class= "info"]/div[@class="title"]/a/@href'
            req = spider.query(url)
            text = req.text
            url_xqs = spider.parseurl(text, xpath)
            for url in url_xqs:
                req_xq = spider.query(url)
                text_xq = req_xq.text
                title_xpath = '//*[@id="beike"]/div[1]/div[2]/div[2]/div/div/div[1]/h1/text()'
                address_xpath = '//*[@id="beike"]/div[1]/div[2]/div[2]/div/div/div[1]/div/text()'
                if len(spider.parseurl(text_xq, title_xpath)) == 0:
                    title = ''
                else:
                    title = spider.clear(spider.parseurl(text_xq, title_xpath))[0]

                if len(spider.parseurl(text_xq, address_xpath)) == 0:
                    address = ''
                else:
                    address = spider.clear(spider.parseurl(text_xq, address_xpath))[0]
                titles.append(title)
                addresses.append(address)
                citys.append(city[i])
                time.sleep(0.1)

    print(len(titles),len(citys),len(addresses))
    df = pd.DataFrame({'小区名': titles, '区县': citys, '地址': addresses})
    print(df)
    df.to_excel('福州小区.xlsx', index=False)