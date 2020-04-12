import requests, re
from lxml import etree
from queue import Queue
import time
import threading
from functools import wraps
from redis import Redis

class Tool:
    def fn_timer(self,function):
        @wraps(function)
        def function_timer(*args, **kwargs):
            t0 = time.time() * 1000
            result = function(*args, **kwargs)
            t1 = time.time() * 1000
            print('running %s : %sms ' % (function.__name__, str(int(t1 - t0))))
            return result

        return function_timer


def async_pool(max_thread):
    def start_async(f):
        def wrapper(*args, **kwargs):
            while 1:
                func_thread_active_count = len([i for i in threading.enumerate() if i.name == f.__name__])
                if func_thread_active_count <= max_thread:
                    thr = threading.Thread(target=f, args=args, kwargs=kwargs, name=f.__name__)
                    thr.start()
                    break
                else:
                    time.sleep(0.01)

        return wrapper

    return start_async

class Spider:
    def run(self):

        spider.get_game_base_info_list()
        self.receive()

    def get_game_base_info_list(self):

        def get_html():
            return session.get(url, timeout=3).content.decode('utf-8')

        url = 'https://www.3839.com/top/hot.html'
        html = get_html_by_func(get_html)
        if not html:
            print('请求错误，请检查网络')
            return False
        selector = etree.HTML(html)
        rankli_list = get_res_by_xpath(selector, "//li[@class='rankli']")
        for rankli_selector in rankli_list:
            game_url = get_res_by_xpath(rankli_selector, "./div[@class='gameInfo']/em[@class='name']/a/@href", False)
            game_name = get_res_by_xpath(rankli_selector, "./div[@class='gameInfo']/em[@class='name']/a/text()", False)
            introduce = get_res_by_xpath(rankli_selector, "./div[@class='gameInfo']/p[@class='desc']/text()", False)
            logo_url = get_res_by_xpath(rankli_selector, "./a/img[@class='gameLogo']/@src", False)
            score = get_res_by_xpath(rankli_selector, "./div[@class='gameInfo']/div[@class='info']/div[@class='gameScore']/span[@class='score']/text()", False).replace('分',"")

            game_url = "https:%s"%game_url
            logo_url = "https:%s"%logo_url
            try:
                score = float(score)
            except:
                score = -1
            game_base_info = {
                "game_name":game_name,
                "game_url":game_url,
                "introduce":introduce,
                "logo_url":logo_url,
                "score":score
            }
            game_base_info_queue.put(game_base_info)
            print("信息推送成功：%s"%game_base_info)

    def receive(self):
        while 1:
            game_base_info = game_base_info_queue.get()
            print('信息接收成功：%s'%game_base_info)
            self.get_game_info(game_base_info)

    @async_pool(1)
    def get_game_info(self, game_base_info):
        def get_html():
            html = session.get(game_base_info['game_url'], timeout=3).content.decode('utf-8')
            return html

        html = get_html_by_func(get_html)
        if not html:
            print('请求错误，请检查网络')
            return False
        selector = etree.HTML(html)
        comment_count = get_res_by_xpath(selector,"//div[@class='gameDesc']/div[@class='grade']/div[@class='card']/p[@class='num']/text()",is_list=False).replace("人","")
        try:
            comment_count = int(comment_count)
        except:
            comment_count = -1

        game_base_info['game_id'] = get_res_by_re(game_base_info['game_url'], "a/(\d+)\.htm",is_list=False)
        game_base_info['comment_count'] = comment_count
        print("爬取成功%s\n%s"%(game_base_info, "="*50))

def get_html_by_func(func, retry_count=5):
    for i in range(retry_count):
        try:
            html = func()
            return html
        except Exception as e:
            print(e)
            pass
    else:
        return False


def get_res_by_xpath(selector, xpath, is_list=True):
    res = selector.xpath(xpath)
    if not is_list:
        if not res:
            res = ''
        else:
            res = res[0]
    return res


def get_res_by_re(text, regex, is_list=True):
    res = re.findall(re.compile(regex, re.S), text)  # list
    if not is_list:
        if not res:
            res = ''
        else:
            res = res[0]
    return res



if __name__ == '__main__':
    spider = Spider()
    tool = Tool()
    redis = Redis(host='127.0.0.1', port=6379, decode_responses=True)
    session = requests.session()
    session.headers = {
        "Referer": "https://www.3839.com/top/hot.html",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.25 Safari/537.36 Core/1.70.3676.400 QQBrowser/10.5.3738.400"
    }
    game_base_info_queue = Queue()
    spider.run()
