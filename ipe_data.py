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
cookie="acw_tc=65c86a0b15646457645586587e3edfa07c6851152f8ff5213f60fbf03fb25e; __utmz=105455707.1564723333.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); ASP.NET_SessionId=b0ppnjjbkp5tsk1bdyd3cis0; ajaxkey=DB1D7B94E280174363D1022350E0E299B4DFD90C87DBA1D2; __utma=105455707.2050914565.1564723333.1564723333.1566899238.2; __utmc=105455707; __utmb=105455707.1.10.1566899238; SERVERID=8abfb74b5c7dce7c6fa0fa50eb3d63af|1566900229|1566897757"

# 连接本地redis
r = redis.Redis(host="127.0.0.1",port=6379,db=0)
setKey= "ipe_set"   # 记录已经获取详情的id
cacheKey = "ipe_list_data"
ips = [
    'http://112.85.169.3:9999',
    'http://112.85.169.97:9999',
    'http://163.125.75.41:9797',
    'http://1.198.73.219:9999',
    'http://182.35.81.44:9999',
    'http://49.73.157.156:9999',
    'http://163.204.243.172:9999',
    'http://171.35.168.230:9999',
    'http://163.204.242.244:9999',
    'http://171.11.178.249:9999',
    #'http://1.199.30.89:9999',
    'http://171.13.137.59:9999',
    'http://1.198.72.98:9999',
    'http://1.198.72.113:9999',
    'http://223.243.255.195:65309',
    'http://112.85.165.195:9999',
    'http://1.198.73.196:9999',
    'http://114.239.42.45:9999',
    'http://58.253.154.137:9999',
    'http://113.124.94.109:9999',
    'http://163.204.241.105:9999',
    'http://60.13.42.42:9999',
    'http://120.83.107.155:9999',
    'http://112.85.166.213:9999',
    'http://110.86.136.104:9999',
    'http://163.204.245.148:9999',
    'http://163.204.240.226:9999',
    'http://123.163.122.110:9999',
    'http://163.204.93.179:9999',
    'http://163.204.242.157:9999',
    'http://163.204.241.252:9999',
    'http://59.57.148.168:9999',
    'http://1.197.204.50:9999',
    'http://58.253.156.197:9999',
    'http://171.11.179.129:9999',
    'http://163.204.247.241:9999',
    'http://125.123.127.253:9999',
    'http://58.253.156.167:9999',
    'http://114.239.250.202:9999',
    'http://163.204.93.28:9999',
    'http://182.34.35.132:9999',
    'http://114.230.69.160:9999',
    'http://42.216.75:9999',
    'http://163.204.244.138:9999',
    'http://182.34.33.120:9999',
    'http://163.204.247.149:9999',
    'http://120.83.105.51:9999',
    'http://163.204.243.193:9999',
    'http://58.253.155.89:9999',
    'http://113.194.29.176:9999',
    'http://182.34.32.7:9999',
    'http://218.91.94.149:9999',
    'http://114.239.172.113:9999',
    'http://1.198.72.235:9999',
    'http://112.85.165.162:9999',
    'http://120.83.97.34:9999',
    'http://123.101.110.16:9999',
    'http://1.197.204.101:9999',
    'http://1.199.31.7:9999',
    'http://1.198.72.4:53128',
    'http://120.83.105.238:9999',
    'http://125.123.143.186:9999',
    'http://1.197.203.140:9999',
    'http://112.87.68.243:9999',
    'http://163.204.244.31:9999',
    'http://60.13.42.168:9999',
    'http://123.55.2.73:9999',
    'http://59.57.38.164:9999',
    'http://182.116.239.72:9999',
    'http://59.57.149.100:9999',
    'http://218.95.37.252:3128',
    'http://1.197.16.172:9999',
    'http://112.87.71.233:9999',
    'http://123.54.253.188:9999',
    'http://123.54.19.29:9999',
    'http://123.54.52.100:9999',
    'http://123.54.52.91:9999',
    'http://123.54.225.31:9999',
    'http://123.54.231.13:9999',
    'http://123.54.40.24:9999',
    'http://114.101.45.138:65309',
    'http://121.69.46.177:9000',
    'http://110.86.136.61:9999',
    'http://114.104.131.163:808',
    'http://110.86.139.212:9999',
    'http://60.167.135.31:61234',
    'http://39.84.113.244:9999',
    'http://39.84.114.73:9999',
    'http://1.197.204.237:9999',
    'http://1.197.204.192:9999',
    'http://110.86.136.126:9999',
    'http://36.248.129.78:9999',
    'http://1.198.73.138:9999',
    'http://1.199.31.243:9999',
    'http://175.42.123.250:9999',
    'http://112.85.166.175:9999',
    'http://122.193.246.129:9999',
    'http://218.64.69.79:8080',
    'http://60.166.75.95:808',
    'http://113.121.44.201:61234',
]
proxies = {'http':random.choice(ips)}
#request = requests.get(url, proxies={'http': random.choice(pro)}, headers=head) # 让问这个网页 随机生成一个ip


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
            result = requests.post(url,data=params, proxies=proxies, headers=headers)
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
        f.write("ID,latitude,longitude,unknow,number,unknow1,unknow2\n")
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
