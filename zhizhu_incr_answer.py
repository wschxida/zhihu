#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time    : 2020/6/15 11:56
# @Author  : wcx
# @Email   : 972761574@qq.com
# @File    : zhizhu_incr_answer.py
# @Software: PyCharm

# 用于将标准库中大部分阻塞式调用修改为协作式运行
# 把import gevent，from gevent import monkey，monkey.patch_all()三行语句放在其他所有的import语句之前，可以避免出现警告或者报错信息，导致程序不能正常运行
from gevent import monkey

monkey.patch_all()
import gevent
import time
import redis
import hashlib
import re
import requests
import json
import datetime
import copy
from lxml import etree
import logging

log = logging.getLogger(__name__)

# 携趣代理相关信息
proxy_server = '121.199.42.16:801'
uid = '16657'
ukey = '9B823DAF919CD96B1A520F25D8FB4896'

# 程序出口redis相关信息
params = dict(host='192.168.1.134', port='6380', password='ks_3000', db=0, decode_responses=True)

pool = redis.ConnectionPool(**params)
Rs = redis.Redis(connection_pool=pool)

# 按照天数计数入库信息
today_date = datetime.datetime.now().strftime('%Y%m:%d')


def get_md5(in_str='', coding='UTF-16LE'):
    in_str = in_str.encode(coding)
    md5_str = hashlib.md5(in_str).hexdigest()  # 加密
    return md5_str


def remove_none_printable_char(in_str):
    out_str = re.sub(r'[\x00-\x20]', '', in_str)
    return out_str


def py_md5(in_str, is_file=False, remove_none_printable_chars=False, case_sensitivity=False):
    if is_file:
        return ''
    if remove_none_printable_chars:
        in_str = remove_none_printable_char(in_str)
    if case_sensitivity:
        in_str = in_str.lower()
    return get_md5(in_str, 'UTF-16LE')


def not_proxy_parse_requests(url, headers={}, cookies={}):
    try:
        response = requests.get(url=url, headers=headers, cookies=cookies, timeout=15)
        return response
    except Exception as e:
        pass


def get_proxy():
    # 获取ip
    for i in range(10):
        url = 'http://{}/VAD/GetIp.aspx?act=get&num=1&time=30&plat=0&re=0&type=1&so=1&ow=0&addr='.format(proxy_server)
        response = not_proxy_parse_requests(url=url)
        if not response:
            return None
        response_json = response.json()
        if response_json.get('success') == 'true':
            msg = response_json.get('msg')
            # 如果提示添加白名单，则加入白名单
            if '请先添加' in msg:
                current_ip_regx = re.search('(\d+\.)+\d+', msg)
                current_ip_regx_ip = current_ip_regx.group()

                add_resp = not_proxy_parse_requests(
                    url='http://{0}/VAD/IpWhiteList.aspx?uid={1}&ukey={2}&act=add&ip={3}'.
                        format(proxy_server, uid, ukey, current_ip_regx_ip))
                response = not_proxy_parse_requests(url=url)
                response_json = response.json()
            try:
                proxy_info = response_json.get('data')[0]
                proxy_ip = proxy_info["IP"]
                proxy_port = proxy_info["Port"]
                proxy_address = f"{proxy_ip}:{proxy_port}"
                return proxy_address
                break
            except Exception as e:
                continue




def  get_url():
    """
    判断zhihu自增ID队列中是否有值，如有将rpop10个id,构造长度为10的url列表。如没有将读取zhihu:last_id，调用incr_id(),将last_id+1000个idpush到zhihu:id，再取url列表
    :return: 二维列表[url_list, article_id]
    """
    url_list = []
    id_count = int(Rs.llen("zhihu:answer:id"))
    if id_count >= 100:
        print(int(id_count))
        for i in range(100):
            article_id_a = Rs.rpop("zhihu:answer:id")
            if article_id_a is not None:
                article_id = article_id_a
                url = f"https://www.zhihu.com/answer/{article_id}"
                url_list.append(url)
    elif id_count>0:
        print(int(id_count))
        for i in range(int(id_count)):
            article_id_a = Rs.rpop("zhihu:answer:id")
            if article_id_a is not None:
                article_id = article_id_a
                url = f"https://www.zhihu.com/answer/{article_id}"
                url_list.append(url)
    else:
        last_id = Rs.get('zhihu:answer:last_id')
        incr_id(int(last_id))
        for i in range(100):
            article_id = Rs.rpop("zhihu:answer:id")
            url = f"https://www.zhihu.com/answer/{article_id}"
            url_list.append(url)
    return [url_list, article_id]


def incr_id(value):
    id = value
    for i in range(10000):
        Rs.lpush("zhihu:answer:id", id)
        id += 1


def get_html(url, proxy_address):
    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.87 "
                      "Safari/537.36"
    }
    proxies = {'http': proxy_address,
               'https': proxy_address}
    try:
        response = requests.get(url, headers=headers, proxies=proxies, timeout=10)
        return response
    except Exception as e:
        pass


def get_data(url, proxy_address):
    """
    获取重定向后的url，获取其html, 解析html,入库redis
    :param url:
    :param proxy_address:
    :return:
    """
    try:
        #print(url)
        page_content = get_html(url, proxy_address)
        #print(page_content)
        if page_content:
            if page_content.status_code==200:
                redirect_url = page_content.url
                if "signin?next=" in redirect_url:
                    redirect_url = redirect_url.split('=')[-1]
                    redirect_url = redirect_url.replace('%3A', ':').replace('%2F', '/')
                if "answer_deleted" in redirect_url:
                    return None
                if "unhuman" in redirect_url:
                    return None
                if "question" in redirect_url:
                    #print(redirect_url)
                    answer_id = redirect_url.split('/')[-1]
                    ret = page_content.text
                    data = parse_html(ret, answer_id)
                    save_data(data, redirect_url)
    except Exception as e:
        print("error message:" + str(e))


def list_iter(name):
    """
    自定义redis列表增量迭代
    :param name: redis中的name，即：迭代name对应的列表
    :return: yield 返回 列表元素
    """
    list_count = Rs.llen(name)
    for index in range(list_count):
        yield Rs.lindex(name, index)


def parse_html(response, answer_id):
    '''
    解析html,获取各个字段的值
    :param response: 页面html
    :param answer_id: url中id参数
    :return:data字典，包含article_detail表需要的各个字段
    '''
    try:
        data = {}
        html = etree.HTML(response)
        data["article_title"] = html.xpath("//h1[@class='QuestionHeader-title']/text()")[0]
        article_content = html.xpath("//span[@class='RichText ztext CopyrightRichText-richText']//text()")
        data["article_content"] = ''.join(article_content)
        article_author = html.xpath("//a[@class='UserLink-link']/text()")
        data['article_author'] = article_author[0] if article_author else None
        content_html = html.xpath("//script[@id='js-initialData']")[0]
        content_html = etree.tostring(content_html, encoding='utf-8').decode('utf-8').replace(
            '<script id="js-initialData" type="text/json">',
            '').replace('}</script>', '}')
        content_html = json.loads(content_html)
        time_stamp = content_html["initialState"]["entities"]["answers"][answer_id]["createdTime"]
        data["article_pubtime_str"] = datetime.datetime.fromtimestamp(int(time_stamp)).strftime(
            "%Y-%m-%d %H:%M:%S")
        #print(data)
    except Exception as e:
        print("error message2:" + str(e))
    return data


def save_data(data, url):
    """
    补充字段，rpush到这两个键article_detail_data、article_content_data
    :param data:
    :param url:
    :return:
    """
    article_detail_data = copy.deepcopy(data)
    article_content_data = copy.deepcopy(data)

    article_detail_data['article_url'] = url
    article_detail_data['website_no'] = "S18453"
    article_detail_data['article_pubtime'] = article_detail_data['article_pubtime_str']
    article_url_md5_id = py_md5(url, False, True, True)
    article_detail_data["article_url_md5_id"] = article_url_md5_id
    article_detail_data["media_type_code"] = "A"
    article_detail_data["domain_code"] = "www.zhihu.com"
    article_detail_data["is_with_content"] = 1
    article_detail_data["is_extract_after_detail"] = 0
    article_detail_data["language_code"] = "CN"
    article_detail_data["refpage_type"] = "K"

    article_detail_data['record_md5_id'] = article_url_md5_id
    article_content_data['article_record_md5_id'] = article_url_md5_id

    article_detail_data['table_name'] = 'article_detail'
    article_content_data['table_name'] = 'article_content'
    print(article_detail_data)

    Rs.rpush('zhihu:items', json.dumps(article_detail_data))
    Rs.rpush('zhihu:items', json.dumps(article_content_data))
    Rs.incr('counter:zhihu:answer:%s' % today_date)


def main():
    """
    while无限循环，其中包括了一个10次的for循环，意思是每循环1000词将判断result_count是否增加了，如没有将等待5min再次运行
    循环内容：取一个携趣代理地址(30s有效), 取url_list，作为参数传入get_data()协程
    :return:
    """

    logging.basicConfig(filename="debug.log", filemode="w", format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
                        datefmt="%d-%M-%Y %H:%M:%S", level=logging.DEBUG)
    while True:
        try:
            result_count_1 = int(Rs.get('counter:zhihu:answer:%s' % today_date))
        except:
            Rs.set('counter:zhihu:answer:%s' % today_date, 0)
            result_count_1 = int(Rs.get('counter:zhihu:answer:%s' % today_date))
        # for循环开始，取1000个ID
        for i in range(10):
            start_time = time.time()
            g_l = []
            url_info = get_url()
            try:
                proxy_address = get_proxy()
            except Exception as e:
                proxy_address = get_proxy()
            url_list = url_info[0]
            last_article_id = url_info[1]
            try:
                Rs.set('zhihu:answer:last_id', last_article_id)
            except Exception as e:
                print(url_info)
            for url_item in url_list:
                # 创建一个新的greenlet协程对象,并运行
                g = gevent.spawn(get_data, url_item, proxy_address)
                g_l.append(g)
                # 参数是一个协程对象列表，它会等待所有的协程都执行完毕后再退出
            gevent.joinall(g_l)
            end_time = time.time()
            print(end_time - start_time)
        # for循环结束，开始判断result_count有没有增加
        result_count_2 = int(Rs.get('counter:zhihu:answer:%s' % today_date))
        if result_count_2 - result_count_1 == 0:
            print("进程暂时挂起！")
            for i in range(int(last_article_id), int(last_article_id)-1000, -1):
                Rs.rpush("zhihu:answer:id", i)
            time.sleep(600)
            print("继续执行")


if __name__ == '__main__':
    main()
