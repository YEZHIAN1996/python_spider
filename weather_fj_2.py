
import base64
import http
import os
import random
import requests
import time
from lxml import etree
from bs4 import BeautifulSoup
import xlwt
import pandas as pd

import re
import json

class TQ_Spider:
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
            headers = {"Referer": "http://www.weather.com.cn/",
                       'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36',
                       'Cookie': "f_city=%E7%A6%8F%E5%B7%9E%7C101230101%7C; __bid_n=18484802ae323858e14207; Hm_lvt_080dabacb001ad3dc8b9b9049b36d43b=1669038017,1669085208,1669107280,1669172622; Hm_lpvt_080dabacb001ad3dc8b9b9049b36d43b=1669185952"}
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
        clear = []
        for list_value in list_item:
            list = list_value.replace("\n", "")
            clear_list.append(list)
        for i in clear_list:
            if i != '' and i != '详情':
                clear.append(i)
        return clear

    def list_of_groups(self, init_list, children_list_len):
        """
        :param init_list: 原列表
        :param children_list_len: 指定切割的子列表的长度（就是你要分成几份）
        :return: 新列表
        """
        list_of_groups = zip(*(iter(init_list),) * children_list_len)
        end_list = [list(i) for i in list_of_groups]
        count = len(init_list) % children_list_len
        end_list.append(init_list[-count:]) if count != 0 else end_list
        return end_list

    def del_adjacent(self, alist):
        for i in range(len(alist) - 1, 0, -1):
            if alist[i] == alist[i - 1]:
                del alist[i]

    def cols_set(self, list1, value, week_value, date_value):
        for i in range(0,95):
            qx.append(list1[i][0])
            max_tem.append(list1[i][3])
            min_tem.append(list1[i][6])
            date.append(time.strftime('%Y-%m-%d', time.localtime(time.time())))
            tq.append(list1[i][1])
            week.append(week_value)
            date_time1.append(date_value)
            yc_date_value.append(value)



    # 解析资讯网页
    def parsetext(self, url):
        tq_result = self.query(url)
        content6 = tq_result.content.decode('utf-8')
        tq_json = json.loads(content6)
        tqzxbt = tq_json['result']['content']['title']
        reportAbstract = tq_json['result']['content']['reportAbstract']
        tq_fbsj = tq_json['result']['content']['reportCreateTime']
        nr = tq_json['result']['content']['reportContent']
        return tqzxbt, reportAbstract,tq_fbsj,nr

    # 判断文件夹是否存在，不存在则创建
    def Judge_folder(self, folder):
        if not os.path.exists(folder):
            print("文件不存在，已创建！")
            os.mkdir(folder)
        else:
            print("开始下载图片")
if __name__ == '__main__':
    qx = []
    max_tem = []
    min_tem = []
    date = []
    tq = []
    yc_date_value = []
    week = []
    date_time = []
    date_time1 = []

    alarm_url_list = []

    # 预警发布时间、预警标题、发布内容、信号等级、发布单位、标准、图例、防御指南
    yjfb_time = []
    yjbt = []
    yj_fbnr = []
    xhdj = []
    yj_fbdw = []
    bz = []
    tl = []
    fyzn = []

    # 预警类别
    yjlb = ['台风', '暴雨', '暴雪', '寒潮', '大风', '沙尘暴', '高温', '干旱', '雷电', '冰雹', '霜冻', '大雾', '霾',
             '道路结冰']
    # 预警颜色
    yjyc = ['蓝色', '黄色', '橙色', '红色', '白色']

    yjlb_index = ['01','02','03', '04','05','06','07','08','09','10','11','12','13','14']
    yjyc_index = ['01', '02', '03', '04', '05']

    url_1 = 'http://www.weather.com.cn/textFC/fujian.shtml'
    url_2 = 'https://news.fznews.com.cn/dsxw/20221121/6y25W4F8M6.shtml'
    alarm = "http://product.weather.com.cn/alarm/grepalarm_cn.php?_=1669186111578"
    # 天气趋势相关资讯
    url_3 = ['http://productsapi.weather.com.cn/products/getTextAndPicData?type=YB_TQQS_3D','http://productsapi.weather.com.cn/products/getTextAndPicData?type=YB_TQQS_10D','http://productsapi.weather.com.cn/products/getTextAndPicData?type=YB_QDLWD_XML']
    spider = TQ_Spider()
    spider.Judge_folder('image')
    spider.Judge_folder('tl_pic')
    pics = []
    # 下载所有预警图片
    for i in range(0, len(yjlb)):
        for j in range(0, len(yjyc)):
            tp_url = 'http://www.weather.com.cn/m2/i/about/alarmpic/' + yjlb_index[i] + yjyc_index[j] + '.gif'
            result_pic = spider.query(tp_url)
            pic_content = result_pic.content
            file = r'C:\Users\gientech\python_spider\tl_pic\\'
            # 完整保存图片的链接
            filename = file + yjlb[i] + yjyc[j] + '预警信号.gif'
            # 进行图片保存
            with open(filename, 'wb') as f:
                f.write(pic_content)
                print(filename)
            time.sleep(1)


    # 气温预测数据
    for i in range(0, 7):
        date_time.append(time.strftime('%Y-%m-%d',time.localtime(time.time() + 86400*i)))
    result = spider.query(url_1)
    content = result.content.decode('utf-8')
    xpath = "/html/body/div[4]/div[2]/div/div/ul/li/text()"
    week_list = spider.parseurl(content, xpath)
    week_list1 = []
    for week_value in week_list:
        index = week_value.index('周')
        week_list1.append(week_value[index:index+2])
    soup = BeautifulSoup(content, 'lxml')
    tds = soup.select('.hanml > .conMidtab > .conMidtab3 > table > tr > td ')
    td_cols = []
    for td in tds:
        td_cols.append(td.text)
    td_cols = spider.clear(td_cols)
    spider.del_adjacent(td_cols)

    split_list = spider.list_of_groups(td_cols, 7)
    today = split_list[0:95]
    today_add_one = split_list[96:191]
    today_add_two = split_list[192:287]
    today_add_three = split_list[288:383]
    today_add_four = split_list[384:479]
    today_add_five = split_list[480:575]
    today_add_six = split_list[576:671]

    city = ['福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '福州', '厦门', '厦门','厦门','厦门','厦门',
            '厦门','厦门', '宁德', '宁德', '宁德', '宁德', '宁德', '宁德', '宁德', '宁德', '宁德', '宁德', '莆田', '莆田','莆田','莆田','莆田','莆田', '泉州',
            '泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州', '漳州', '漳州','漳州','漳州','漳州','漳州','漳州',
            '漳州','漳州','漳州','漳州','漳州', '龙岩', '龙岩','龙岩','龙岩','龙岩','龙岩','龙岩','龙岩', '三明','三明','三明','三明','三明','三明','三明','三明','三明','三明','三明','三明','三明',
            '南平','南平','南平','南平','南平','南平','南平','南平','南平','南平','南平',] * 7

    spider.cols_set(today, 0, week_list1[0], date_time[0])
    spider.cols_set(today_add_one, 1, week_list1[1], date_time[1])
    spider.cols_set(today_add_two, 2, week_list1[2], date_time[2])
    spider.cols_set(today_add_three, 3, week_list1[3], date_time[3])
    spider.cols_set(today_add_four, 4, week_list1[4], date_time[4])
    spider.cols_set(today_add_five, 5, week_list1[5], date_time[5])
    spider.cols_set(today_add_six, 6, week_list1[6], date_time[6])
    # print(len(date), len(city), len(qx), len(yc_date_value), len(max_tem), len(min_tem))
    df1 = pd.DataFrame({'日期': date, '城市': city, '区县': qx, '预测未来第几天':yc_date_value, '预测最高气温':max_tem, '预测最低气温':min_tem})
    print(df1)
    df1.to_excel('气温预测数据.xlsx', index=False)
    print('气温预测数据运行结束')


    # 天气情况预测数据
    # print(len(date_time1), len(city), len(qx), len(week), len(max_tem), len(min_tem),len(tq))
    df2 = pd.DataFrame({'日期': date_time1, '地市': city, '区县': qx, '星期': week, '最高气温': max_tem,
                       '最低气温': min_tem, '天气': tq})
    print(df2)
    df2.to_excel('天气情况预测数据.xlsx', index=False)
    print('天气情况预测数据运行结束')


    # 气象预警咨询
    result3 = spider.query(alarm)
    content3 = result3.content.decode('utf-8')
    # print(content3)
    json_str = re.findall('"data":\[(.*)\]', content3)[0]
    # print(json_str)
    split_text = json_str.split('"],')
    for text in split_text:
        if text[2:5] == '福建省':
            url = 'http://product.weather.com.cn/alarm/webdata/' + text.split(',')[1][1:-1]
            result4 = spider.query(url)
            content4 = result4.content.decode('utf-8')[14:]
            json_value = json.loads(content4)
            # 发布单位
            yj_fbdw.append(json_value['STATIONNAME'] + '气象台')
            # 标题
            yjbt.append(json_value['head'])
            # 发布时间
            yjfb_time.append(json_value['ISSUETIME'])
            # 发布内容
            yj_fbnr.append(json_value['ISSUECONTENT'])
            lb = json_value['TYPECODE']
            yc = json_value['LEVELCODE']
            alarm_url = 'http://www.weather.com.cn/data/alarminfo/' + lb + yc +'.html'
            pic_url = 'http://www.weather.com.cn/m2/i/about/alarmpic/' + lb + yc + '.gif'
            time.sleep(1)
            result5 = spider.query(alarm_url)
            content5 = result5.content.decode('utf-8')[15:-1]
            content5_list = content5.split('",')
            # 信号等级
            xhdj.append(content5_list[1][1:])
            # 标准
            bz.append(content5_list[2][1:])
            # 防御指南
            fyzn.append(content5_list[3][1:])
            tl.append(pic_url)

    # print(len(yjfb_time), len(yjbt), len(yj_fbnr), len(xhdj), len(yj_fbdw), len(yj_fbdw),len(bz),len(tl),len(fyzn))
    df3 = pd.DataFrame({'预警发布时间': yjfb_time, '预警标题': yjbt, '发布内容': yj_fbnr, '信号等级':xhdj, '发布单位':yj_fbdw,'标准':bz,'图例':tl, '防御指南':fyzn})
    print(df3)
    df3.to_excel('气象预警资讯.xlsx', index=False)
    print('气象预警咨询运行结束')


    # 天气趋势相关咨询
    titles = []
    reportAbstracts = []
    reportCreateTimes = []
    reportContents = []
    pic_list = []
    j = 0
    for tq_url in url_3:
        b64_pics = []
        result6 = spider.query(tq_url)
        content6 = result6.content.decode('utf-8')
        json_str6 = json.loads(content6)
        # print(json_str6)
        if json_str6['status'] == 'success':
            titles.append(json_str6['result']['content']['title'])
            reportCreateTimes.append(json_str6['result']['content']['reportCreateTime'])
            reportAbstracts.append(json_str6['result']['content']['reportAbstract'])
            reportContents.append(json_str6['result']['content']['reportContent'])
            html = json_str6['result']['content']['reportContent']
            xpath_pic = '/html/body/p/span/img/@src'
            # xpath_content = '/html/body/p/span/text()'
            # content = spider.parseurl(html, xpath_content)
            pic_url = spider.parseurl(html, xpath_pic)
            # print(content)
            # pics.append(pic_url)

            if len(pic_url) > 0:
                i = 1
                for p_url in pic_url:
                    # 定义要下载的内容
                    download = spider.query(p_url)
                    # 循环打开文件创建jpg
                    with open("image/" + time.strftime("%Y%m%d",time.localtime(time.time())) + '_' + str(j) + '_' + str(i) + ".jpg", mode="wb") as f:
                        # 开始下载
                        f.write(download.content)
                    # 循环打开图片
                    # with open("image/" + time.strftime("%Y%m%d",time.localtime(time.time())) + '_'  + str(j) + '_' + str(i) + ".jpg", "rb") as f:
                    #     # 转为二进制格式，并且使用base64进行加密
                    #     base64_data = base64.b64encode(f.read())
                    #     b64_pics.append(base64_data)
                    # 停顿
                    time.sleep(0.01)
                    i += 1
        #     if len(b64_pics) < 5:
        #         b64_pics = b64_pics + [''] * (5-len(b64_pics))
        # pic_list.append(b64_pics)
        # j += 1

    # print(len(titles), len(reportAbstracts), len(reportCreateTimes),len(reportContents), len(pic_list))
    print('图片下载完成')
    df4 = pd.DataFrame({'发布时间': reportCreateTimes, '天气资讯标题': titles, '文章标题': reportAbstracts, '发布内容': reportContents})
    print(df4)
    df4.to_excel('天气趋势相关资讯.xlsx', index=False)
    print('天气趋势相关咨询数据运行结束')
