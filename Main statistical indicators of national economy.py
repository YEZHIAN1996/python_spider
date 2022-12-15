import json
import random
import time
import requests
from lxml import etree


class Crawl:
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
            headers = {"Referer": "https://tjj.fujian.gov.cn/xxgk/jdsj/",
                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                       'Cookie': "Secure; JSESSIONID=8C385635BC7420F5A230562FAF5BE95E; _gscu_849304753=69771410b79j5660; _gscbrs_849304753=1; Hm_lvt_8f90361f1cba1f3dbc91676f0aa63047=1669771410,1671093016; Hm_lvt_6cbb9cc9ce83fb03212bc458e023776b=1669771410,1671093016; insert_cookie=85610366; _gscs_849304753=71093015dnr4ce29|pv:17; Hm_lpvt_8f90361f1cba1f3dbc91676f0aa63047=1671095243; Hm_lpvt_6cbb9cc9ce83fb03212bc458e023776b=1671095243"}
            try:
                result = requests.get(headers=headers, url=url, timeout=5) #, proxies=self.proxies()
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

if __name__ == '__main__':
    crawl = Crawl()
    # 省份, 指标, 年份, 月份, 绝对量, 同比值, 累计绝对量, 累计同比值
    sf = []
    zb = []
    year = []
    month = []
    jdl = []
    tbz = []
    ljjdl = []
    ljtbz = []
    base_url = 'https://tjj.fujian.gov.cn/was5/web/search?channelid=298174&templet=advsch.jsp&sortfield=-docorderpri%2C-docreltime&classsql=(chnlid%3D3524)*(doctitle%3D%25%E5%85%A8%E7%9C%81%E5%9B%BD%E6%B0%91%E7%BB%8F%E6%B5%8E%E4%B8%BB%E8%A6%81%E7%BB%9F%E8%AE%A1%E6%8C%87%E6%A0%87%25)&prepage=15&page='

    url = base_url + '1'
    req = crawl.query(url)
    result = req.text
    json_value = json.loads(result, strict=False)
    for value in json_value['docs']:
        print(value['url'])

