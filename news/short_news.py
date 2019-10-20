#!/usr/bin/python3
import requests
from bs4 import BeautifulSoup
import asyncio
import json
from aiohttp import web

def get_news():
    url = "http://china.cnr.cn/news/index.html"
    html = requests.get(url).content
    parsed_html = BeautifulSoup(html,'lxml')
    news_list = parsed_html.body.findAll('div',attrs={'class':'item'})
    res = []
    for item in news_list:
        title = item.find('strong').text
        content = item.find('em').text
        res.append('【'+title+'】'+content)
    return res



async def index(request):
    news = get_news()
    return web.json_response(news)

async def hello(request):
    await asyncio.sleep(0.5)
    text = '<h1>hello, %s!</h1>' % request.match_info['name']
    return web.Response(body=text.encode('utf-8'))

async def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    app.router.add_route('GET', '/hello/{name}', hello)
    srv = await loop.create_server(app.make_handler(), '127.0.0.1', 8000)
    print('Server started at http://127.0.0.1:8000...')
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()


