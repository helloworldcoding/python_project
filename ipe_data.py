#!/usr/bin   python3

import requests
import json
import getopt
import sys
import time
import random
import redis


'''
从浏览器中复制cookie的值过来
'''
#cookie="acw_tc=65c86a0b15646457645586587e3edfa07c6851152f8ff5213f60fbf03fb25e; ajaxkey=DB1D7B94E2801743E49A5995AEA448136C8256AC4E7407A6; ASP.NET_SessionId=rlkza1zyjb1c4o2neb1yebhe; SERVERID=8abfb74b5c7dce7c6fa0fa50eb3d63af|1564722761|1564722741"
cookie="acw_tc=7b39758715668591300895559e0501e995d741bf7fbc36b7c35af15cbe217b; ASP.NET_SessionId=qu3ltqf1zrq5bkc01korfczf; ajaxkey=DB1D7B94E28017431941B2A373D24148A6AADA7095EE3409; SERVERID=8abfb74b5c7dce7c6fa0fa50eb3d63af|1566861342|1566861327"

# 连接本地redis
r = redis.Redis(host="127.0.0.1",port=6379,db=0)
setKey= "ipe_set"   # 记录已经获取详情的id
cacheKey = "ipe_list_data"

def get_list(industrytype=5,watertype=None,hasvg=None,level=12):
    '''
    筛选条件：
    区域：全国范围，
    企业类型: 土壤风险源
    违规情况：
    :return:  所有的记录点
    '''

    # url params headers 这三个参数都是直接从浏览器获取的。
    url = 'http://www.ipe.org.cn/data_ashx/GetAirData.ashx?xx=getindustryzhoubian_map'
    params = {
        'cmd': 'getindustryzhoubian_map',
        'lat_leftdown': 4,
        'lng_leftdown': 70,
        'lat_rightup': 53,
        'lng_rightup': 135,
        'level': level,
        'parentid': 0,
        'province': 0,
        'city': 0,
        'industrytype': industrytype,
        'watertype':watertype,
        'hasvg': hasvg,
        'issearch': 1
    }
    headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer': 'http://www.ipe.org.cn/IndustryMap/IndustryMap.aspx?q=7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        'Cookie': cookie
    }

    try:
        if r.get(cacheKey) is None:
            result = requests.post(url,data=params, headers=headers)
            content = result.content.decode('utf-8')
            r.set(cacheKey,content)
            list = json.loads(content)
        else:
            content = r.get(cacheKey).decode('utf-8')
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
    time.sleep(random.random())  # 太过频繁，ip会被封
    return node['Data']

def deal_result(list=[], file='./result.csv'):
    '''
    :param file: 最后生成的csv文件
    :return:
    '''
    if not list:
        print("get list error")
        r.delete(cacheKey)
        r.delete(setKey)
        return None
    with open(file,'wt',encoding='utf_8_sig') as f:
        f.write("ID,f_name,Year,recordcount,HY,CJId,HaD,gltype,glname,latitude,longitude\n")
        for item in list:
            id = item[0]
            # 如果已经获取了该节点的详情，就不用再请求了
            if r.sismember(setKey,id):
                continue
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
                print(line)
                f.write(line+"\n")
                r.sadd(setKey,id)


def get_list_by_parmas(industrytype=5,watertype=None,hasvg=None):
    '''
    该方法，只能获取大尺度下的数据，不能精确到具体的点，不符合需求,
    不过可以计算总数，可以验证是否有遗漏
    :param industrytype:
    :param watertype:
    :param hasvg:
    :return:
    '''
    url = "http://www.ipe.org.cn/data_ashx/GetAirData.ashx?xx=getindustryzhoubian_map"
    params = {
        'cmd': 'getindustryzhoubian_map',
        'lat_leftdown': 32.170555,
        'lng_leftdown': 104.429676,
        'lat_rightup': 33.374554999999994,
        'lng_rightup': 106.676276,
        'level': 5,
        'parentid': 0,
        'province': 0,
        'city': 0,
        'industrytype': industrytype,  # 5为土壤风险源 4 垃圾焚烧厂 3 污水处理厂 2 近期有新增违规记录或超标排放 1重点排污单位  0,3,4,5,1,2 全部
        'watertype': watertype, # 排放数据 2 企业反馈 1 自行监测  0,1,2,3 全部
        'hasvg': hasvg, # 0,1,2 全部  1 有不良记录  2 无不良记录
        'issearch': 1
    }
    headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer': 'http://www.ipe.org.cn/IndustryMap/IndustryMap.aspx?q=7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        'Cookie': cookie
    }

    result = requests.post(url,data=params,headers=headers)
    content = result.content.decode('utf-8')
    nodes = json.loads(content)['Data']
    if not nodes:
        print("empty result")
        return None
    file = str(industrytype)+"_"+str(watertype)+"_"+str(hasvg)+"_result_by_params.csv"
    with open(file,'wt',encoding='utf_8_sig') as f:
        f.write("ID,latitude,longitude,unknow,number,unknow1,unknow2\n")
        for item in nodes:
            line = ','.join(item)
            f.write(line+"\n")





if __name__ == '__main__':
    try:
        argv = sys.argv[1:]
        opts, args = getopt.getopt(argv,"iwh",["industrytype=","watertype=","hasvg=","help"])
    except getopt.GetoptError:
        print ('python ipe_data.py ')
        sys.exit(2)
    industrytype = 5
    watertype = None
    hasvg = None
    for opt, arg in opts:
        if opt in ( '-h',"--help"):
            print ('''
            --industrytype=  # 5为土壤风险源 4 垃圾焚烧厂 3 污水处理厂 2 近期有新增违规记录或超标排放 1重点排污单位  0,3,4,5,1,2 全部
            --watertype # 排放数据 2 企业反馈 1 自行监测  0,1,2,3 全部
            --hasvg # 0,1,2 全部  1 有不良记录  2 无不良记录
            default:  python ipe_data.py   默认只获取 土壤风险源的
            eg1:  python ipe_data.py  --industrytype=5,2 --watertype=1,2  --hasvg=1
            eg2:  python ipe_data.py  --industrytype=5    只查询 土壤风险源
            ''')
            sys.exit()
        elif opt in ("-i", "--industrytype"):
            industrytype = arg
        elif opt in ("-w", "--watertype"):
            watertype = arg
        elif opt in ("--hasvg"):
            hasvg = arg
    # 默认只获取 土壤风险源的
    list = get_list(industrytype,watertype,hasvg)
    file = './data/'+str(industrytype)+"_"+str(watertype)+"_"+str(hasvg)+"_ipe_data.csv"
    deal_result(list,file)
