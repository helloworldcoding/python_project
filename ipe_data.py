#!/usr/bin   python3

import requests
import json
import getopt
import sys


'''
从浏览器中复制cookie的值过来
'''
cookie="acw_tc=7b39758815640324801285746ed6be5443ccbe3dd5fdf8e38e0cfcb2297665; __utmz=105455707.1564032650.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _dx_uzZo5y=f940492d5055c1176a822dabb20087f9603810f8ea44b4051cdb5cc9ee540a42ef0b14be; __utma=105455707.1834450751.1564032650.1564032650.1564121234.2; _dx_app_81e16deaf5fee41ed38834363824b3e3=5d393e93LptLKe40OOsJjEMINayzeCAZWWXlcJM1; ASP.NET_SessionId=den53lqafvq4f25xxmzvrq5r; ajaxkey=F2FB35F25190281B2CE6175D38CEB64F16EF760B6290A058; SERVERID=63ce6a224eb1e4e64c95f4d7b348be8a|1564551167|1564551158"


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
        'Cookie': cookie
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


def get_list_by_parmas(industrytype=5,watertype=None,hasvg=None):
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
    #deal_result()
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
    get_list_by_parmas(industrytype,watertype,hasvg)
