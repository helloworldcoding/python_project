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
cookie="ASP.NET_SessionId=jy12q3xpxsg3m1lcmzgilata; acw_tc=65c86a0a15735009288403479e8c2f46697196df7f0727330ab9820a586eac; ajaxkey=78E94BADCF004BB149600962FAE5D1235533E47C463EFBF0; SERVERID=63ce6a224eb1e4e64c95f4d7b348be8a|1573503320|1573502982"

# 连接本地redis
r = redis.Redis(host="127.0.0.1",port=6379,db=1)
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
                    "lat_leftdown":round(lat_leftdown + i * lat_diff, 5),
                    "lng_leftdown":round(lng_leftdown + j * lng_diff,5),
                    "lat_rightup":round(lat_leftdown + (i+1) * lat_diff,5),
                    "lng_rightup":round(lng_leftdown + (j +1) * lng_diff,5)
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
        if r.sismember("no_data_area", areaCacheKey):
            return []
        if r.get(areaCacheKey) is None:
            print("request area %s start....\n"%(areaCacheKey))
            result = requests.post(url,data=params, proxies=proxies, headers=headers)
            print("request area %s end \n"%(areaCacheKey))
            content = result.content.decode('utf-8')  # 有可能 不是json
            list1 = json.loads(content)
            if "Data" in list1:
                if list1["Data"]:
                    r.set(areaCacheKey,content)
                else:
                    r.sadd("no_data_area",areaCacheKey)
            else:
                print("get area %s fail, reason: %s \n" % (areaCacheKey,content))
            time.sleep(5+random.random())  # 太过频繁，ip会被封
        else:
            content = r.get(areaCacheKey).decode('utf-8')
            list1 = json.loads(content)
        # [['1334593', '37.204320', '119.953803', '0', '216', '18', '1'], ['309156', '28.980050', '111.688940', '0', '0', '0', '1'] .... ]
        return  list1["Data"] if "Data" in list1 else []
    except Exception as err:
        print(areaCacheKey,err)
        sys.exit(1)

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
        areaCacheKey = 'area_cache_'+str(area['lat_leftdown'])+'_'+str(area['lng_leftdown'])+'_'+str(area['lat_rightup'])+'_'+str(area['lng_rightup'])
        areaList = query_one_area(params,areaCacheKey)
        if areaList:
            time.sleep(2+random.random())  # 太过频繁，ip会被封
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
    try:
        result = requests.post(url,data=params,headers=headers,proxies=proxies)
        content = result.content.decode('utf-8')
        node = json.loads(content)
        # {"IsSuccess":"1","Data":[{"id":"77625","f_name":"广西百色东信化工有限责任公司","Year":"2019","recordcount":"1","HY":"无机酸制造","CJId":"0","HaD":"0","gltype":"0","glname":""}]}
        time.sleep(2+random.random())  # 太过频繁，ip会被封
        return node['Data']
    except Exception as err:
        print("get node failed %d "%(int(id)),err," \n")
        sys.exit(1)

def deal_result(list1=[], file='./result.csv'):
    '''
    :param file: 最后生成的csv文件
    :return:
    '''
    if not list1:
        print("get list error")
        r.delete(cacheKey)
        r.delete(setKey)
        return None
    with open(file,'a',encoding='utf_8_sig') as f:
        f.write("ID,f_name,Year,recordcount,HY,CJId,HaD,gltype,glname,latitude,longitude\n")
        for item in list1:
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
                print(line,"\n")
                f.write(line+"\n")
                r.sadd(setKey,id)


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
    list1 = get_list(industrytype,watertype,hasvg)
    file1 = './data/'+str(industrytype)+"_"+str(watertype)+"_"+str(hasvg)+"_ipe_data.csv"
    # 从csv文件中，恢复 已处理的 节点集合
    cache_nodeid_from_file(file1)
    # 获取节点信息
    deal_result(list1,file1)

