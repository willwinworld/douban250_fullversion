# -*- coding: utf-8 -*-
# 豆瓣电影250信息完整版
from pyquery import PyQuery as pq
import requests
import re
from utlis.redisq import RedisQueue


REDIS_IP = '127.0.0.1'
REDIS_PASS = ''
res_queue = RedisQueue('result', host=REDIS_IP, db=1, password=REDIS_PASS)


def page_info(url):
    r = requests.get(url)
    r.encoding = 'gbk'
    d = pq(r.text)
    info_list = []  # 主页面的信息
    url_list = []  # 2到10页的URL
    # import ipdb;ipdb.set_trace()
    block_nodes = d('#content .grid_view .item')
    for node in block_nodes:
        dollar = pq(node)
        rank = dollar('em').text()
        movie_name = dollar('.pic a img').attr('alt')
        link = dollar('.pic a').attr('href')
        quote = dollar('.info .quote .inq').text() or 'missing'
        rate = dollar('.info .star .rating_num').text()
        viewnum = dollar('.info .star span').text()
        reviewnum = re.match(r'\d+', viewnum[0]).group(0)
        info_list.append({'rank': rank, 'movie_name': movie_name, 'link': link, 'quote': quote, 'rate': rate, 'reviewnum': reviewnum})
    paginator_nodes = d('#content .paginator a:lt(9)')
    for node in paginator_nodes:
        url_list.append('http://movie.douban.com/top250' + pq(node).attr('href'))  # 第一页进入，获得2到10页的URL
    return url_list, info_list  # 返回元祖


def movie_detail(url):
    r = requests.get(url)
    r.encoding = 'gbk'
    d = pq(r.text)
    detail_dict = {}
    info_nodes = d('#info')
    for node in info_nodes:
        dollar = pq(node)
        director = dollar('.attrs:eq(0) a').text()
        writer = dollar('.attrs:eq(1) a').text()
        maincharacter = dollar('.attrs:eq(2) a').text()
        detail_dict.update({'director': director, 'writer': writer, 'maincharacter': maincharacter})
    summary = d('#link-report span[property]').text()
    detail_dict['summary'] = summary
    # detail_dict['url'] = url  # 对url进行匹配，肯定没错，排名匹配可能出错，其实不用匹配
    return detail_dict


def combine():  # url_list是2-10页的URL，info_list是第1页的页面信息
    url_list, info_list = page_info('http://movie.douban.com/top250')  # url_list(2-10页的url), info_list(主页信息）
    goal = []
    ten_page_info = []
    for link in url_list:  # 遍历2-10页的URL，去读2-10页的页面信息
        url, info = page_info(link)
        ten_page_info.extend(info)  # 获取剩余九页的信息
    ten_page_info.extend(info_list)  # 十页页面信息全部加入完毕
    for item in ten_page_info:  # ten_page_info结构：[{}, {}...]
        movie_url = item['link']  # item是字典
        res = movie_detail(movie_url)  # 一个个电影详细信息的字典
        merge = dict(item, **res)  # 合并两个字典的方法
        goal.append(merge)
    with open('res.json', 'w') as f:  # 存文件
        f.write(json.dumps(goal))
    print len(goal)

    with open('res.json', 'r') as f:
        data = f.read()
        res = json.loads(data)
    for item in res:
        res_queue.put(item)
    print 'load infomation completed!'









