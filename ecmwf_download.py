# -*- coding: utf-8 -*-
"""
Created on Wed Nov 23 16:00:002022
欧洲网格气象数据下载
@author: Yza
"""

import datetime
import os
import time
import random
from lxml import etree
import requests
from multiprocessing.pool import ThreadPool
import http.client
http.client.HTTPConnection._http_vsn = 10
http.client.HTTPConnection._http_vsn_str = 'HTTP/1.0'


class Download:
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
                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
            try:
                result = requests.get(url=url,stream=True,timeout=20,proxies=self.proxies(),headers=headers)
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

    def download_file(self, ml_1, ml_2):
        baseurl = 'https://data.ecmwf.int/forecasts/'
        now = datetime.datetime.now()
        yes = (now + datetime.timedelta(days=-1)).strftime('%Y%m%d')
        xpath = "//*[@id='outerTable']/tr[4]/td/pre[2]/a/@href"
        url = baseurl + yes + ml_1 + ml_2
        result = self.query(url)
        download_urls = self.parseurl(result.text,xpath)
        for download_url in download_urls:
            url = 'https://data.ecmwf.int' + download_url
            # print(download_url[11:])
            file = download_url[11:38]
            if not os.path.exists(file):
                print("文件不存在，已创建！")
                os.makedirs(file)

            if not os.path.exists(download_url[11:]):
                print('开始下载' + download_url[38:] + '文件')
                r = self.query(url)
                with open(download_url[11:], 'wb') as f:
                    for ch in r:
                        f.write(ch)

            print('下载完成')

if __name__ == '__main__':
    ml1_1 = ['/00z/0p4-beta/', '/12z/0p4-beta/']
    ml1_2 = ['/06z/0p4-beta/', '/18z/0p4-beta/']
    ml2_1 = ['enfo/', 'oper/', 'waef/', 'wave']
    ml2_2 = ['enfo/', 'scda/', 'scwv/', 'waef']
    download = Download()
    # download.download_file(ml1_2[0], ml1_2[1])


    for i in range(2):
        for j in range(1, 4):
            ThreadPool(9).imap_unordered(download.download_file(ml1_1[i], ml2_1[j]))
            ThreadPool(9).imap_unordered(download.download_file(ml1_2[i], ml2_2[j]))


