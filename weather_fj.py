# -*- coding: utf-8 -*-
"""
Created on Fri Mar 25 08:58:37 2022
天气爬虫
@author: yza
"""
import pandas as pd
import numpy as np
import requests
import re
import json
import random
import time
import arrow
import warnings

warnings.filterwarnings("ignore")

class weather:
    region_list ={'福州':35401,'厦门':35402,'宁德':35403,'莆田':35404,'泉州':35405,'漳州':35406,'龙岩':35407,'三明':35408,'南平':35409,'台江':3540101,
          '闽侯':3540107,'福清':3540108,'长乐':3540109,'连江':3540110,'罗源':3540111,'闽清':3540112,'永泰':3540113,'平潭':3540114,'仓山':3540117,'晋安':3540119,
          '思明':3540201,'湖里':3540202,'集美':3540203,'海沧':3540204,'蕉城':3540301,'福安':3540302,'福鼎':3540303,'古田':3540304,'屏南':3540305,'周宁':3540306,
          '寿宁':3540307,'柘荣':3540308,'霞浦':3540309,'涵江':3540402,'秀屿':3540403,'仙游':3540404,'城厢':3540407,'荔城':3540408,'秀屿港':3540409,'泉港':3540502,
          '洛江':3540503,'南安':3540506,'晋江':3540507,'石狮':3540508,'惠安':3540509,'安溪':3540510,'永春':3540511,'德化':3540512,'鲤城':3540513,'丰泽':3540514,
          '金门':3540515,'芗城':3540601,'龙文':3540602,'龙海':3540603,'长泰':3540604,'漳浦':3540605,'云霄':3540606,'东山':3540608,'诏安':3540609,'平和':3540610,
          '南靖':3540611,'华安':3540612,'新罗':3540704,'漳平':3540705,'长汀':3540706,'永定':3540707,'上杭':3540708,'连城':3540709,'武平':3540710,'永安':3540802,
          '清流':3540805,'沙县':3540806,'宁化':3540807,'泰宁':3540808,'建宁':3540809,'大田':3540810,'明溪':3540811,'尤溪':3540812,'将乐':3540813,'三元':3540814,
          '梅列':3540815,'邵武':3540903,'武夷山':3540905,'建阳':3540906,'建瓯':3540907,'延平':3540908,'光泽':3540909,'浦城':3540910,'顺昌':3540911,'政和':3540912,
          '松溪':3540913,'鼓楼':3540121,'马尾':3540122,'同安':3540209,'翔安':3540210,'崇武':3540516}
    def proxies(self):
        (username, password, ip, port) = ('13261692613', 'ylkj_12311', '140.249.73.234', '15033')
        proxies = {'http': f'http://{username}:{password}@{ip}:{port}'}
        return proxies
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
    def his_county(self):  # 2345天气网(历史到县)
        def query(url):# 读取网页数据
            retried = 0
            while retried < 10:
                # 修改头文件避免反爬虫
                headers = {'User-Agent': self.generate_random_str(8)}
                try:
                    # result = requests.get(
                    #     headers=headers, url=url,proxies=self.proxies(),timeout=5)
                    result = requests.get(headers=headers, url=url,timeout=5)
                    break
                except:  # 异常
                    retried = retried+1
                    if retried < 10:
                        print('正在重试')
                    else:
                        print('读取数据失败')
                    time.sleep(1)
            return result
        city_list = {'福州': 58847, '长乐': 60396, '仓山': 71913, '福清': 60084, '鼓楼': 71911, '晋安': 71915, '罗源': 60071,
                     '连江': 60004, '闽清': 60073, '闽侯': 60301, '马尾': 71914, '平潭': 60302, '台江': 71912, '永泰': 60072,
                     '长汀': 70017, '连城': 60873, '上杭': 60717, '武平': 60716, '新罗': 71929, '永定': 60077, '漳平': 60607,
                     '光泽': 70021, '建阳': 60655, '建瓯': 60080, '浦城': 60654, '顺昌': 60081, '邵武': 60079, '松溪': 60705,
                     '武夷山': 60082, '延平': 71932, '政和': 60900, '福安': 60398, '福鼎': 60401, '古田': 70022, '蕉城': 71921,
                     '屏南': 60075, '寿宁': 60303, '霞浦': 60397, '周宁': 60703, '柘荣': 60656, '城厢': 71397, '涵江': 71394,
                     '荔城': 71396, '秀屿': 71395, '仙游': 60076, '秀屿港': 70032, '石狮': 71237, '安溪': 70016, '崇武': 70018,
                     '德化': 60653, '丰泽': 71924, '惠安': 71236, '晋江': 71149, '金门': 71922, '鲤城': 71923, '洛江': 71925,
                     '南安': 60304, '泉港': 71926, '永春': 60085, '大田': 70019, '将乐': 70024, '建宁': 70025, '明溪': 70027,
                     '梅列': 71930, '宁化': 60899, '清流': 60657, '沙县': 60400, '三元': 71931, '泰宁': 60714, '尤溪': 60715,
                     '永安': 60083, '海沧': 71917, '湖里': 71918, '集美': 71919, '思明': 71916, '同安': 70030, '翔安': 71920,
                     '长泰': 61002, '东山': 70020, '华安': 70023, '龙海': 60078, '龙文': 71928, '南靖': 70028, '平和': 70029,
                     '芗城': 71927, '云霄': 60399, '漳浦': 70034, '诏安': 60702, '南平': 58834, '龙岩': 58927, '厦门': 59134,
                     '漳州': 59126, '三明': 58828, '宁德': 58846, '莆田': 58946, '泉州': 59131}
        city_father=['福州','福州','福州','福州','福州','福州','福州','福州','福州','福州','福州','福州','福州','福州','龙岩','龙岩','龙岩','龙岩','龙岩','龙岩','龙岩','南平','南平','南平','南平','南平','南平','南平','南平','南平','南平','宁德','宁德','宁德','宁德','宁德','宁德','宁德','宁德','宁德','莆田','莆田','莆田','莆田','莆田','莆田','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','泉州','三明','三明','三明','三明','三明','三明','三明','三明','三明','三明','三明','三明','厦门','厦门','厦门','厦门','厦门','厦门','漳州','漳州','漳州','漳州','漳州','漳州','漳州','漳州','漳州','漳州','漳州','南平','龙岩','厦门','漳州','三明','宁德','莆田','泉州']
        date_his = []
        weekend_his = []
        Tem_max_his = []
        Tem_min_his = []
        weather_his = []
        wind_his = []
        city_temp1=[]
        city_temp2=[]
        j=0
        for city in city_list:
            for t_temp in [arrow.now().shift(months=-1).strftime('%Y%m'), arrow.now().strftime('%Y%m')]:
                year = t_temp[:4]
                month = t_temp[4:]
                print(city+str(year)+month)
                url = 'http://tianqi.2345.com/Pc/GetHistory?areaInfo[areaId]='+str(
                    city_list[city])+'&areaInfo[areaType]=2&date[year]='+str(year)+'&date[month]='+month
                result = query(url)
                result = json.loads(result.text)['data']
                maxTem = re.findall(
                    '<td style="color:#ff5040;">(.*?)°</td>', result)
                minTem = re.findall(
                    '<td style="color:#3097fd;" >(.*?)°</td>', result)
                data = re.findall('<td>(.*?)</td>', result)
                num = 0
                if len(maxTem) != 0:
                    num = int(len(data)/len(maxTem))
                for i in range(len(maxTem)):
                    Tem_max_his.append(maxTem[i])
                    Tem_min_his.append(minTem[i])
                    city_temp1.append(city_father[j])
                    city_temp2.append(city)
                    if num == 4:
                        temp = data[4*i]
                        date_his.append(temp[:10])
                        weekend_his.append(temp[11:])
                        weather_his.append(data[4*i+1])
                        wind_his.append(data[4*i+2])
                    else:
                        temp = data[3*i]
                        date_his.append(temp[:10])
                        weekend_his.append(temp[11:])
                        weather_his.append(data[3*i+1])
                        wind_his.append(data[3*i+2])
            j=j+1
        result_total = np.array(
            [date_his,city_temp1,city_temp2, weekend_his, Tem_max_his, \
              Tem_min_his, weather_his, wind_his]).T
        result_total = pd.DataFrame(
            result_total, columns=['RQ','DLWZ1','DLWZ2', 'XQ', 'ZGQW', \
                                    'ZDQW', 'TQ', 'FS'])
        result_total['ZGQW']=pd.to_numeric(result_total['ZGQW'],errors='coerce')
        result_total['ZDQW']=pd.to_numeric(result_total['ZDQW'],errors='coerce')
        result_total.set_index('RQ', inplace=False)
        result_total.index=pd.to_datetime(result_total.index).strftime('%Y%m%d')
        result_total['ORG_NO']=result_total['DLWZ2']
        for i in self.region_list:
            result_total['ORG_NO'][result_total['ORG_NO']==i]=self.region_list[i]
        result_total['ORG_NO']=result_total['ORG_NO'].astype(str)
        # writer = pd.ExcelWriter(
        #     '历史数据到县.xlsx')
        # result_total.to_excel(writer)
        result_total.to_csv('历史数据到县.csv',encoding='gbk')
    def fore_county(self):  # 中国天气网(预测到县)
        def query(url):# 读取网页数据
            retried = 0
            while retried < 10:
                # 修改头文件避免反爬虫
                headers = {"Referer": "http://www.weather.com.cn/",'User-Agent': self.generate_random_str(8)}
                try:
                    result = requests.get(
                        headers=headers, url=url,timeout=5)  #, proxies=self.proxies()
                    break
                except:  # 异常
                    retried = retried+1
                    if retried < 10:
                        print('正在重试')
                    else:
                        print('读取数据失败')
                    time.sleep(1)
            return result
        city_list = []
        date1 = []
        date2 = []
        Tem_max_fore = []
        Tem_min_fore = []
        w1 = []
        wd1 = []
        fe = []
        city_temp=[]
        city_temp1=[]
        for city1 in ['01', '02', '03', '04', '05', '06', '07', '08', '09']:  # 九地市
            for city2 in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17',
                          '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33']:  # 县域
                url = "http://d1.weather.com.cn/calendar_new/2021/10123" + \
                    city1+city2+"_202101.html"  # 以某个月为例看看这个地址存不存在
                result = query(url)
                if result.status_code == 200:  # 获取成功
                    content = result.content.decode(encoding='utf-8')
                    state = re.findall('<title>(.*?)</title>', content)
                    if state == ['非常抱歉，网页无法访问']:
                        break
                url = "http://www.weather.com.cn/weather1d/10123"+city1+city2+".shtml"
                result=query(url)
                if result.status_code == 200:  # 获取成功
                    content = result.content.decode(encoding='utf-8')
                    city_name = re.findall('<title>(.*?)天气预报', content)
                    city_list.append(city_name[0])
                    print(city_name[0])
                    if city2=='01':
                        city_temp2=city_name[0]
                for t_temp in [arrow.now().strftime('%Y%m'), arrow.now().shift(months=1).strftime('%Y%m'), arrow.now().shift(months=2).strftime('%Y%m')]:
                    year = t_temp[:4]
                    month = t_temp[4:]
                    url = "http://d1.weather.com.cn/calendar_new/" + \
                        year+"/10123"+city1+city2+"_"+year+month+".html"
                    result = query(url)
                    if result.status_code == 200:  # 获取成功
                        content = result.content.decode(encoding='utf-8')
                        weathers = json.loads(content[11:])
                        for i in range(len(weathers)):
                            date1.append(weathers[i]['date'])
                            date2.append(
                                weathers[i]['nlyf']+weathers[i]['nl'])
                            Tem_max_fore.append(weathers[i]['max'])
                            Tem_min_fore.append(weathers[i]['min'])
                            w1.append(weathers[i]['w1'])
                            wd1.append(weathers[i]['wd1'])
                            fe.append(weathers[i]['fe'])
                            city_temp.append(city_name[0])
                            city_temp1.append(city_temp2)
        result_total = np.array(
            [date1,city_temp1,city_temp, date2, Tem_max_fore, Tem_min_fore, w1, wd1, fe]).T
        result_total = pd.DataFrame(result_total, columns=[
                              'RQ','DLWZ1','DLWZ2', 'NL', 'YCZGQW', 'YCZDQW', 'TQYC', 'FSYC', 'JR'])
        result_total.drop_duplicates(inplace=True)
        result_total.set_index('RQ', inplace=True)
        result_total['YCZGQW'].replace('',np.nan,inplace=True)
        result_total.dropna(subset=['YCZGQW'],inplace=True)
        result_total['YCZGQW']=pd.to_numeric(result_total['YCZGQW'],errors='coerce')
        result_total['YCZDQW']=pd.to_numeric(result_total['YCZDQW'],errors='coerce')
        result_total['CREATETIME']=arrow.now().strftime('%Y%m%d')
        result_total['ORG_NO']=result_total['DLWZ2']
        for i in range(len(result_total)):
            result_total['ORG_NO'][i]=self.region_list[result_total['ORG_NO'][i]]
        result_total['ORG_NO']=result_total['ORG_NO'].astype(str)
        result_total['RQ'] = result_total.index
        # writer = pd.ExcelWriter(
        #     '预测数据到县.xlsx')
        # result_total.to_excel(writer)
        result_total.to_csv('预测数据到县.csv',encoding='gbk')

W = weather() # 天气爬虫实例
W.his_county()  # 获取最新分县历史天气(每天执行，去重插入)
W.fore_county()  # 获取最新分县40天预测天气(每天执行，全部插入)

