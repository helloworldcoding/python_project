#!/usr/bin   python3

import requests
import json
import urllib.parse

def get_list():
    '''
    筛选条件：
    区域：全国范围，
    企业类型: 土壤风险源
    违规情况： 有不良记录
    :return:  所有的记录点
    '''

    # url params headers 这三个参数都是直接从浏览器获取的。
    url = 'http://www.ipe.org.cn/data_ashx/GetAirData.ashx?xx=getindustryzhoubian_map'
    params = {
        'cmd': 'getindustryzhoubian_map',
        'lat_leftdown': 3.931333,
        'lng_leftdown': 107.54335999999999,
        'lat_rightup': 35.135332999999996,
        'lng_rightup': 109.78996,
        'level': 5,
        'parentid': 0,
        'province': 0,
        'city': 0,
        'industrytype': 5,
        'hasvg': 1,
        'issearch': 1
    }
    headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer': 'http://www.ipe.org.cn/IndustryMap/IndustryMap.aspx?q=7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        'Cookie': 'acw_tc=7b39758815640324801285746ed6be5443ccbe3dd5fdf8e38e0cfcb2297665; ASP.NET_SessionId=gloctx5p0kdw0ve4w3wnk1ju; __utma=105455707.1834450751.1564032650.1564032650.1564032650.1; __utmc=105455707; __utmz=105455707.1564032650.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _dx_uzZo5y=f940492d5055c1176a822dabb20087f9603810f8ea44b4051cdb5cc9ee540a42ef0b14be; ajaxkey=F2FB35F25190281BBEA89C0286F21C0BC23346B1F6FD4414; SERVERID=8abfb74b5c7dce7c6fa0fa50eb3d63af|1564042776|1564042771'
    }

    try:
        result = requests.post(url,data=params, headers=headers)
        content = result.content.decode('utf-8')
        list = json.loads(content)
        # [['1334593', '37.204320', '119.953803', '0', '216', '18', '1'], ['309156', '28.980050', '111.688940', '0', '0', '0', '1'] .... ]
        return list['Data']
    except Exception as err:
        print(err)
        return None

def  node_info(id):
    '''
    获取单个点的信息
    :param id:  点的ID
    :return: {}
    '''
    url = 'http://www.ipe.org.cn/data_ashx/GetAirData.ashx?xx=getindustryclick_map'
    headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer': 'http://www.ipe.org.cn/IndustryMap/IndustryMap.aspx?q=7'
    }
    params = {
        'cmd': 'getindustryclick_map',
        'id':  id,
        'typeid': 1
    }
    result = requests.post(url,data=params,headers=headers)
    content = result.content.decode('utf-8')
    node = json.loads(content)
    # {"IsSuccess":"1","Data":[{"id":"77625","f_name":"广西百色东信化工有限责任公司","Year":"2019","recordcount":"1","HY":"无机酸制造","CJId":"0","HaD":"0","gltype":"0","glname":""}]}
    return node['Data']

def deal_result(file='./result.csv'):
    '''
    :param file: 最后生成的csv文件
    :return:
    '''
    list = get_list()
    if not list:
        print("get list error")
        return None
    with open(file,'wt',encoding='utf_8_sig') as f:
        f.write("ID,f_name,Year,recordcount,HY,CJId,HaD,gltype,glname,latitude,longitude\n")
        for item in list:
            id = item[0]
            latitude = item[1] # 纬度
            longitude = item[2] # 经度
            nodeInfo = node_info(id)
            for node in nodeInfo:
                node['latitude'] = latitude
                node['longitude'] = longitude
                line = "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (
                    str(node['id']),node['f_name'],str(node['Year']),str(node['recordcount']),
                    str(node['HY']),str(node['CJId']),str(node['HaD']),str(node['gltype']),str(node['glname']),latitude,longitude
                )
                f.write(line+"\n")




if __name__ == '__main__':
    deal_result()
