# -*- coding: utf-8 -*-
"""
Created on Wed Nov 9 16:00:002022
福建省疫情高中风险地区数据及确诊病例相关的数据
@author: Yza
"""
import openpyxl
import json
import re
import time
import random
import requests
from lxml import etree
import pandas as pd

def generate_random_str(randomlength=16):  # 生成随机字符串
    """
    生成一个指定长度的随机字符串
    """
    random_str = ''
    base_str = 'ABCDEFGHIGKLMNOPQRSTUVWXYZabcdefghigklmnopqrstuvwxyz0123456789'
    length = len(base_str) - 1
    for i in range(randomlength):
        random_str += base_str[random.randint(0, length)]
    return random_str
def proxies():
    (username, password, ip, port) = ('13261692613', 'ylkj_12311', '140.249.73.234', '15033')
    proxies = {'http': f'http://{username}:{password}@{ip}:{port}'}
    return proxies
def query(url):  # 读取网页数据
    retried = 0
    while retried < 10:
        # 修改头文件避免反爬虫
        headers = {"Referer": "http://www.baidu.com/", 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'}
        try:
            result = requests.get(
                headers=headers, url=url, timeout=5,proxies=proxies())
            break
        except:  # 异常
            retried = retried + 1
            if retried < 10:
                print('正在重试')
            else:
                print('读取数据失败')
            time.sleep(1)
    return result
def clear(list_item):
    clear_list = []
    clear = []
    for list_value in list_item:
        list = list_value.replace(" ","").replace("\r\n","")
        clear_list.append(list)
    for i in clear_list:
        if i != '':
            clear.append(i)
    return clear
def getGDCor(key,add):
    baseUrl = 'http://restapi.amap.com/v3/geocode/geo?'
    params = {
        'key': key,
        'address': add,
        # 'city':'350100'
    }
    requests.DEFAULT_RETRIES = 5
    retried = 0
    while retried < 10:
        try:
            result = requests.get(baseUrl,params,timeout=100,proxies=proxies())
            # time.sleep(1)
            content = result.content
            # print(content)
            jsonDate = json.loads(content)
            lon, lat = '', ''
            if jsonDate['status'] == '1':
                try:
                    corr = jsonDate['geocodes'][0]['location']
                    lon, lat = corr.split(',')[0], corr.split(',')[1]
                except:
                    lon, lat = '0', '0'
            else:
                print('error')
            break
        except:
            retried = retried + 1
            if retried < 10:
                print('正在重试')
            else:
                print('读取数据失败')
            time.sleep(1)

    return add,lon,lat
def parseurl(result,xpath):
    html = etree.HTML(result.content, etree.HTMLParser(encoding='utf-8'))
    text = html.xpath(xpath)
    return text


if __name__ == '__main__':
    # 高风险地区数据采集
    city_list = ['fz','xm','pt','sm','qz','zhangzhou', 'np','longyan','nd']
    city = ['福州','厦门','莆田','三明','泉州','漳州','南平','龙岩','宁德']
    city_codes = ['350100','350200','350300','350400','350500','350600','350700','350800','350900']
    key = '1a5cf10de9c06e71aa9fc4003678d6ad'
    province_cols = []
    city_cols = []
    town_cols = []
    city_code_cols = []
    time_cols = []
    laiyuan_cols = []
    address_cols = []
    lon_cols = []
    lat_cols = []
    dengji_cols = []
    ditu_datalist = []
    add_cols =[]
    for i in range(0,len(city_list)):
        url = 'http://{}.bendibao.com/news/gelizhengce/fengxianmingdan.php?isqg=1&qu=%E5%85%A8%E9%83%A8'.format(city_list[i])
        height = "//*[@id='info']/div[1]/div/div/div[@class='detail-message-show']/div[@class='ditu']/text()"
        result = query(url)
        html = etree.HTML(result.content, etree.HTMLParser(encoding='utf-8'))
        height_item_text = html.xpath(height)
        height_item = clear(height_item_text)
        if len(height_item) != 0:
            print(city_list[i],height_item)
            for add in height_item:
                if add[0:2] != '沙县':
                    town_cols.append(add[0:3])
                else:
                    town_cols.append('沙县')
                add_cols.append(add)
                dengji_cols.append('高风险')
                province_cols.append('福建')
                time_cols.append(time.strftime('%Y-%m-%d', time.localtime(time.time())))
                laiyuan_cols.append('福建本地宝（福建发布）公众号')
                city_cols.append(city[i])
                city_code_cols.append(city_codes[i])
                address = '中国福建省'+ city[i] + add
                add, lon, lat = getGDCor(key, address)
                address_cols.append(add)
                lon_cols.append(lon)
                lat_cols.append(lat)
    # print(len(province_cols),len(city_cols),len(town_cols),len(city_code_cols),len(add_cols),len(dengji_cols),len(address_cols),len(lon_cols),len(lat_cols),len(time_cols),len(laiyuan_cols))
    df_1 = pd.DataFrame({'省份':province_cols,'地级市':city_cols,'区县':town_cols,'行政区划编码':city_code_cols,'具体地址':add_cols,'风险等级':dengji_cols,'详细地址':address_cols,'lon':lon_cols,'lat':lat_cols,'时间':time_cols,'数据来源':laiyuan_cols})
    print(df_1)
    df_1.to_excel(r'福建省疫情高风险地区.xlsx',index=False)




    # 福建省确诊病例数据
    baseurl = 'https://voice.baidu.com/act/newpneumonia/newpneumonia/?from=osari_aladin_banner&city=福建'
    result = query(baseurl)
    json_str=re.findall('"component":\[(.*)\],',result.text)[0]
    json_dict = json.loads(json_str)
    caseList = json_dict['caseList']
    area = []
    nativeRelative = []
    asymptomaticLocalRelative = []
    overseasInputRelative = []
    confirmedRelative = []
    curConfirm = []
    confirmed = []
    crued = []
    died = []
    city_code_dict = {'福建':'350000','福州':'350100','厦门':'350200','莆田':'350300','三明':'350400','泉州':'350500','漳州':'350600','南平':'350700','龙岩':'350800','宁德':'350900','平潭综合实验区':'350128'}
    city_code = []
    update_time = []
    for list in caseList:
        if list['area'] == '福建':
            area.append(list['area'])
            nativeRelative.append(list['nativeRelative'])
            asymptomaticLocalRelative.append(list['asymptomaticLocalRelative'])
            # overseasInputRelative.append(list['overseasInputRelative'])
            confirmedRelative.append(list['confirmedRelative'])
            curConfirm.append(list['curConfirm'])
            confirmed.append(list['confirmed'])
            crued.append(list['crued'])
            died.append(list['died'])
            city_code.append(city_code_dict['福建'])
            update_time.append(time.strftime('%Y-%m-%d', time.localtime(time.time())))
            for city_data in list['subList']:
                if city_data['city'] not in ['境外输入','待确认人员']:
                    city_code.append(city_code_dict[city_data['city']])
                    area.append(city_data['city'])
                    nativeRelative.append(city_data['nativeRelative'])
                    asymptomaticLocalRelative.append(city_data['asymptomaticLocalRelative'])
                    # overseasInputRelative.append(city_data['overseasInputRelative'])
                    confirmedRelative.append(city_data['confirmedRelative'])
                    curConfirm.append(city_data['curConfirm'])
                    confirmed.append(city_data['confirmed'])
                    crued.append(city_data['crued'])
                    died.append(city_data['died'])
                    update_time.append(time.strftime('%Y-%m-%d', time.localtime(time.time())))
    # print(len(city_code),len(area),len(nativeRelative),len(asymptomaticLocalRelative),len(confirmedRelative),len(curConfirm),len(confirmed),len(crued),len(died),len(update_time))
    df_2 = pd.DataFrame({'地区行政编码':city_code,'地区':area,'新增确诊':confirmedRelative,'累计确诊':confirmed,'新增本土确诊':nativeRelative,'新增本土无症状':asymptomaticLocalRelative,'现有确诊':curConfirm,'累计治愈':crued,'累计死亡':died,'更新时间':update_time})
    print(df_2)
    df_2.to_excel(r'福建省疫情数据.xlsx', index=False)




