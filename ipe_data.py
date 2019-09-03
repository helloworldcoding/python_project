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
cookie="__utmz=105455707.1564723333.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=105455707.2050914565.1564723333.1564723333.1566899238.2; acw_tc=7b39758815674722932206811e6ca46c6ff24bf4134662e70f6572372cbb9e; ASP.NET_SessionId=pac13wwg1c53prvkkgjyrq22; ajaxkey=3BD434C4038E6CE733B87A77531C05E02FCFFAAF5FEC0331; SERVERID=8abfb74b5c7dce7c6fa0fa50eb3d63af|1567474263|1567474256"

# 连接本地redis
r = redis.Redis(host="127.0.0.1",port=6379,db=0)
setKey= "ipe_set"   # 记录已经获取详情的id
cacheKey = "ipe_list_data"
ips = [
    'http://47.93.36.195:8118',
    'http://222.223.203.104:8060',
    'http://117.95.199.186:9999',
    'http://183.129.207.92:11056',
    'http://47.104.172.108:8118',
    'http://123.169.35.16:9999',
    'http://120.236.219.12:8060',
    'http://120.236.219.11:8060',
    'http://114.235.22.180:9000',
    'http://163.204.245.250:9999',
    'http://117.41.38.18:9000',
    'http://112.111.118.201:9000',
    'http://136.228.128.14:61158',
    'http://121.232.148.28:9000',
    'http://114.234.83.227:9000',
]
#request = requests.get(url, proxies={'http': random.choice(pro)}, headers=head) # 让问这个网页 随机生成一个ip
proxies = {'http':random.choice(ips)}
proxies = None


'''
# encoding:utf-8
import requests # 导入requests模块用于访问测试自己的ip
import random
pro = ['1.119.129.2:8080', '115.174.66.148', '113.200.214.164'] 
# 在(http://www.xicidaili.com/wt/)上面收集的ip用于测试
# 没有使用字典的原因是 因为字典中的键是唯一的 http 和https 只能存在一个 所以不建议使用字典

# 你的请求头信息
head = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
url = 'http://www.whatismyip.com.tw/' # 你用于测试自己ip的网站
request = requests.get(url, proxies={'http': random.choice(pro)}, headers=head) # 让问这个网页 随机生成一个ip
request.encoding = request.apparent_encoding # 设置编码 encoding 返回的是请求头编码 apparent_encoding 是从内容网页中分析出的响应内容编码方式
print(request.text) # 输出返回的内容
'''
def split_area(lat_num=30,lng_num=30):
    lat_leftdown = 4    #北纬4度
    lng_leftdown = 70   #东经70度
    lat_rightup = 53    #北纬53度
    lng_rightup = 135   #东经135度
    '''
    按照经纬度，划分为lat_num * lng_num等分
    '''
    lat_diff = (lat_rightup - lat_leftdown) / lat_num
    lng_diff = (lng_rightup - lng_leftdown) / lng_num
    areas = []
    for i in range(lat_num):
        for j in range(lng_num):
            tmp = {
                    "lat_leftdown":lat_leftdown + i * lat_diff,
                    "lng_leftdown":lng_leftdown + j * lng_diff,
                    "lat_rightup":lat_leftdown + (i+1) * lat_diff,
                    "lng_rightup":lng_leftdown + (j +1) * lng_diff,
                    }
            areas.append(tmp)
            
    return areas
            
def query_one_area(params="", areaCacheKey=""):
    '''
    查询某块区域
    '''
    url = 'http://www.ipe.org.cn/data_ashx/GetAirData.ashx?xx=getindustryzhoubian_map'
    headers = {
        'Content-Type':'application/x-www-form-urlencoded',
        'Referer': 'http://www.ipe.org.cn/IndustryMap/IndustryMap.aspx?q=7',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
        'Cookie': cookie
    }

    try:
        if r.get(areaCacheKey) is None:
            result = requests.post(url,data=params, proxies=proxies, headers=headers)
            content = result.content.decode('utf-8')
            r.set(areaCacheKey,content)
            list = json.loads(content)
            time.sleep(random.random())  # 太过频繁，ip会被封
        else:
            content = r.get(areaCacheKey).decode('utf-8')
            list = json.loads(content)
        # [['1334593', '37.204320', '119.953803', '0', '216', '18', '1'], ['309156', '28.980050', '111.688940', '0', '0', '0', '1'] .... ]
        return list['Data']
    except Exception as err:
        print(content,err)
        return None

def get_list(industrytype=5,watertype=None,hasvg=None,level=18):
    '''
    筛选条件：
    区域：全国范围，
    企业类型: 土壤风险源
    违规情况：
    :return:  所有的记录点
    '''
    params = {
        'cmd': 'getindustryzhoubian_map',
        'level': level,
        'parentid': 0,
        'province': 0,
        'city': 0,
        'industrytype': industrytype,
        'watertype':watertype,
        'hasvg': hasvg,
        'issearch': 1
    }

    areas = split_area()
    res = []
    list_total_len = 0
    r.set("list_total_len",0)
    for area in areas:
        params['lat_leftdown'] = area['lat_leftdown']
        params['lng_leftdown'] = area['lng_leftdown']
        params['lat_rightup'] = area['lat_rightup']
        params['lng_rightup'] = area['lng_rightup']
        areaCacheKey = 'area_cache_'+str(area['lat_leftdown'])+str(area['lng_leftdown'])+str(area['lat_rightup'])+str(area['lng_rightup'])
        areaList = query_one_area(params,areaCacheKey)
        if areaList:
            res = res + areaList
            tmp_len = int(r.get("list_total_len").decode('utf-8')) + len(areaList)
            r.set("list_total_len",tmp_len)
    return res


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
    result = requests.post(url,data=params,headers=headers,proxies=proxies)
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
    with open(file,'a',encoding='utf_8_sig') as f:
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
        #f.write("ID,latitude,longitude,unknow,number,unknow1,unknow2\n")
        for item in nodes:
            line = ','.join(item)
            f.write(line+"\n")


def cache_nodeid_from_file(file=''):
    '''
    从文件中，缓存已存在的 节点ID，已经获取的，就不用再次获取了
    :param file:
    :return:
    '''
    i = 0
    with open(file,'r',encoding='utf_8_sig') as f:
        for line in f:
            i = i + 1
            if i == 1:  # 跳过表头
                continue
            id = line.split(',')[0]
            r.sadd(setKey,id)


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
    # 从csv文件中，恢复 已处理的 节点集合
    cache_nodeid_from_file(file)
    # 获取节点信息
    deal_result(list,file)

