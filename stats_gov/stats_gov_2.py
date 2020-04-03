import functools, os, time, aiohttp, asyncio, random, re, csv, sys, json, pickle
import urllib.parse, urllib.request
import datetime as dt
from bs4 import BeautifulSoup
# from fake_useragent import UserAgent
from my_fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import numpy as np

DEBUG = False

HTML_ENCODEING = 'gb18030'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
BASE_PATH = '../China_Province_2018/'
if not os.path.isdir(BASE_PATH):
    os.mkdir(BASE_PATH)

chromedriver_path = r'C:\E\chromedriver_win32\chromedriver.exe'


class session_pool(object):
    sp = []


class get_sess(session_pool):
    async def __aenter__(self):
        if session_pool.sp:
            self.sess = session_pool.sp.pop(random.randint(0, len(session_pool.sp) - 1))
        else:
            self.sess = aiohttp.ClientSession()
        return self.sess

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        session_pool.sp.append(self.sess)


class selenium_pool(object):
    sp = []
    size = 0


class get_sele(selenium_pool):

    async def __aenter__(self):
        while not selenium_pool.sp and selenium_pool.size >= 10:
            await asyncio.sleep(1)
        if selenium_pool.sp:
            self.driver = selenium_pool.sp.pop(random.randint(0, len(selenium_pool.sp) - 1))
        else:
            chrome_opt = Options()  # 创建参数设置对象.
            chrome_opt.add_argument('--headless')  # 无界面化.
            chrome_opt.add_argument('--disable-gpu')  # 配合上面的无界面化.
            chrome_opt.add_argument('--window-size=1366,768')  # 设置窗口大小, 窗口大小会有影响.
            chrome_opt.add_argument('–user-agent=' + ua.random())  # 设置窗口大小, 窗口大小会有影响.
            prefs = {
                'profile.default_content_setting_values': {
                    'images': 2
                }
            }
            chrome_opt.add_experimental_option('prefs', prefs)
            self.driver = webdriver.Chrome(executable_path=chromedriver_path, chrome_options=chrome_opt)
            selenium_pool.size += 1
        return self.driver

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        selenium_pool.sp.append(self.driver)


class daili_pool(object):
    p = []
    acounts = []
    ok = []
    fail = []
    disable = []
    ok_stack = []
    lock = asyncio.Lock()


class get_daili(daili_pool):
    base_path = 'kuai'

    def __init__(self, use=True):
        self.use = use

    async def __aenter__(self):
        # def __enter__(self):
        if not self.use:
            return None
        with await daili_pool.lock:
            if not daili_pool.p or sum(daili_pool.disable) <= 15:
                # init
                print('@@@ get daili into pool')
                tmp_path = f'./{self.base_path}_tmp'
                t_os = lambda: dt.datetime(*time.localtime(os.path.getmtime(tmp_path))[:7])
                serverlist = []
                if True:
                    async with get_sess() as session:
                        async with session.get(f"http://dps.kdlapi.com/api/getdps?orderid=998583745967093&num=1",
                                               timeout=20, ) as resp:
                            if resp.status != 200:
                                status = resp.status
                                raise Exception('!!!fail on init daili \tstatus:{}'.format(status))
                            else:
                                resp = await resp.text()

                    # request = urllib.request.Request(f"http://dps.kdlapi.com/api/getdps?orderid=998583745967093&num=1")
                    # with urllib.request.urlopen(request) as page:
                    #     resp = page.read().decode()
                    for addr in resp.split('\n'):
                        daili_pool.p.append(['http://' + addr, dt.datetime.now()])
                        daili_pool.acounts.append(2)
                        daili_pool.disable.append(1)
                        daili_pool.ok.append(0)
                        daili_pool.fail.append(0)
                        daili_pool.ok_stack.append([1])
            #
            #     if os.path.isfile(tmp_path) and os.path.getsize(tmp_path) and (dt.datetime.now() - t_os()) < dt.timedelta(
            #             days=1):
            #         with open(tmp_path, 'rb') as f:
            #             serverlist = pickle.loads(f.read())
            #     else:
                    if False:
                        for p in range(1, 3):
                            request = urllib.request.Request(f"https://www.kuaidaili.com/free/inha/{p}/",
                                                             headers={'User-Agent': ua.random()})
                            with urllib.request.urlopen(request) as page:
                                html = page.read().decode()
                            soup = BeautifulSoup(html, 'lxml')
                            trs = soup('table', class_=['table', 'table-bordered', 'table-striped'])[0]('tr')
                            trs = trs[1:]
                            for i in trs:
                                serverlist.append(
                                    ['{}://{}:{}'.format(i('td')[3].string.lower(), i('td')[0].string, i('td')[1].string),
                                     str(i('td')[5].string), dt.datetime.strptime(i('td')[6].string, '%Y-%m-%d %H:%M:%S')])
            #         if False:
            #             for p in range(1, 3):
            #                 request = urllib.request.Request(f"https://www.xicidaili.com/wt/{p}",
            #                                                  headers={'User-Agent': ua.random()})
            #                 with urllib.request.urlopen(request) as page:
            #                     html = page.read().decode()
            #                 soup = BeautifulSoup(html, 'lxml')
            #                 trs = soup('table', id='ip_list')[0]('tr')
            #                 trs = trs[1:]
            #                 for i in trs:
            #                     serverlist.append(
            #                         ['{}://{}:{}'.format(i('td')[5].string.lower(), i('td')[1].string, i('td')[2].string),
            #                          str(i('td')[8].string), dt.datetime.strptime(i('td')[9].string, '%y-%m-%d %H:%M')])
            #         with open(tmp_path, 'wb') as f:
            #             f.write(pickle.dumps(serverlist))
            #     if False:
            #         serverlist = [
            #             ['http://114.231.109.138:15290'],
            #             # ['http://111.76.128.53:18595'],
            #             # ['http://114.101.253.52:20229'],
            #             # ['http://122.241.12.249:22742'],
            #             # ['http://49.75.36.134:19807'],
            #             # ['http://117.30.113.190:22315'],
            #             # ['http://122.246.90.229:23286'],
            #             # ['http://220.179.211.246:19186'],
            #             # ['http://116.8.114.134:23789'],
            #             # ['http://14.21.242.43:17762'],
            #         ]
            #     sl_http = [i for i in serverlist if i[0].startswith('http:')]
            #     daili_pool.p = sl_http
            # if not daili_pool.acounts:
            #     tmp_path = f'./{self.base_path}_tmp2.json'
            #     if os.path.isfile(tmp_path) and os.path.getsize(tmp_path):
            #         with open(tmp_path, 'r') as f:
            #             p = json.loads(f.read())
            #         daili_pool.acounts = p[0]
            #         daili_pool.disable = p[1]
            #         daili_pool.ok = p[2]
            #         daili_pool.fail = p[3]
            #     else:
            #         daili_pool.acounts = [1 for _ in range(len(sl_http))]
            #         daili_pool.disable = [1 for _ in range(len(sl_http))]
            #         daili_pool.ok = [0 for _ in range(len(sl_http))]
            #         daili_pool.fail = [0 for _ in range(len(sl_http))]

        w = lambda: np.multiply(daili_pool.acounts, daili_pool.disable)
        while sum(w()) == 0:
            await asyncio.sleep(1)
        self.select = random.choices(range(len(daili_pool.p)), w(), k=1)[0]
        daili_pool.acounts[self.select] -= 1
        self.daili = daili_pool.p[self.select]
        return self.daili

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.use:
            if exc_type:
                daili_pool.fail[self.select] += 1
                daili_pool.ok_stack[self.select].append(0)
                await asyncio.sleep(1)
                # print(self.daili, daili_pool.ok[self.select], daili_pool.fail[self.select])
            else:
                daili_pool.ok[self.select] += 1
                daili_pool.ok_stack[self.select].append(1)

            if sum(daili_pool.ok_stack[self.select][-40:]) == 0:
                daili_pool.disable[self.select] = 0
            if daili_pool.fail[self.select] > 10:
                if daili_pool.ok[self.select] == 0 or daili_pool.ok[self.select] / daili_pool.fail[self.select] < 0.5:
                    daili_pool.disable[self.select] = 0
                    # print('disable', self.daili, daili_pool.ok[self.select], daili_pool.fail[self.select])
            # if dt.datetime.now() - self.daili[1] > dt.timedelta(minutes=5):
            #     daili_pool.disable[self.select] = 0

            daili_pool.acounts[self.select] += 1

            total = sum(daili_pool.ok) + sum(daili_pool.fail)
            if total % 100 == 0:
                tmp_path = f'./{self.base_path}_tmp2.json'
                with open(tmp_path, 'w') as f:
                    f.write(json.dumps([daili_pool.acounts, daili_pool.disable, daili_pool.ok, daili_pool.fail]))
                print('ok/total: {}/{}'.format(sum(daili_pool.ok), total),
                      'ava/total: {}/{}'.format(sum(daili_pool.disable), len(daili_pool.disable)),
                      'ava-ok/total:{}/{}'.format(sum(np.multiply(daili_pool.ok, daili_pool.disable)),
                                                  sum(np.multiply(np.add(daili_pool.ok, daili_pool.fail),
                                                                  daili_pool.disable))),
                      'dis-ok/total:{}/{}'.format(sum(np.multiply(daili_pool.ok, np.subtract(1, daili_pool.disable))),
                                                  sum(np.multiply(np.add(daili_pool.ok, daili_pool.fail),
                                                                  np.subtract(1, daili_pool.disable))))
                      )


async def get_html(sem, url, handle, result, counter=None):
    if not isinstance(result, list):
        raise TypeError('result must be a list')
    if not isinstance(url, str):
        raise TypeError('url must be a string')
    if not isinstance(counter, dict):
        counter = {'all': 0, 'done': -1, 'now': dt.datetime.now()}
    async with sem:
        async with get_sess() as session:
            while True:
                status = -1
                try:
                    async with get_daili(use=False) as d:
                        async with session.get(url, headers={
                            'user-agent': ua.random()}, timeout=20) as resp:
                            if resp.status != 200:
                                status = resp.status
                                raise Exception(str(status))
                            else:
                                success_reson = ''
                                # response = await resp.content.read()
                                try:
                                    response = await resp.text(HTML_ENCODEING)
                                    success_reson += ' > ' + HTML_ENCODEING[:5].ljust(5)
                                except TimeoutError as e:
                                    raise e
                                except aiohttp.ClientPayloadError as e:
                                    raise e
                                except (UnicodeDecodeError, UnicodeEncodeError) as e:
                                    try:
                                        t = await resp.text('utf-8', errors='ignore')
                                        success_reson += ' > utf-8'
                                    except Exception as e:
                                        print(repr(e))
                                        exit(0)
                                    else:
                                        sa = re.search('请开启JavaScript并刷新该页', t)
                                        sa = sa.group() if sa else ''
                                        sb = re.search('<title>访问验证<title>', t)
                                        sb = sb.group() if sb else ''
                                        s_reson = sa + sb
                                        if s_reson:
                                            try:
                                                async with get_sele() as driver:
                                                    tn = dt.datetime.now()
                                                    driver.get(url)
                                                    response = driver.page_source
                                                    if driver.title not in ['2019年统计用区划代码', '关于更新全国统计用区划代码和城乡划分代码的公告']:
                                                        raise Exception('selenium获取错误', driver.title)
                                                    success_reson += ' > {}s'.format(
                                                        str((dt.datetime.now() - tn).total_seconds())[:4].ljust(4))
                                            except Exception as e:
                                                raise Exception(s_reson, repr(e))
                                        else:
                                            print(t)
                                            print('解码页面时出错，请查看提示信息。url({})'.format(url))
                                            exit(0)
                                except Exception as e:
                                    print('@' * 100)
                                    raise e
                                result.extend(handle(response, url))
                                counter['done'] += 1
                                dt.timedelta().total_seconds()
                                success_reson = 'success!' + success_reson
                                success_reson = success_reson[:32] + '...' if len(success_reson) > 35 else success_reson
                                print('{} \t{} {} {}'.format(
                                    url, success_reson.ljust(35),
                                    '({}/{})'.format(counter['done'], counter['all']).ljust(11),
                                    str(dt.datetime.now() - counter['now'])[:-7]))
                                break
                except Exception as e:
                    print('{} \tretry due to status:{}\t{}'.format(url, status, repr(e)))
                    # await asyncio.sleep(3 + random.random() * 7)


def get_htmls_and_handle(url_list, handle, handle_kwargs_list, count=False):
    sem = asyncio.Semaphore(200)
    tasks = []
    result = []
    counter = {'all': len(url_list), 'done': 0, 'now': dt.datetime.now()} if count else None
    for url, kwargs in zip(url_list, handle_kwargs_list):
        tasks.append(get_html(sem, url, functools.partial(handle, **kwargs), result, counter))
    if not tasks:
        raise ValueError('tasks is empty')
    loop = asyncio.get_event_loop()
    start_time = dt.datetime.now()
    loop.run_until_complete(asyncio.wait(tasks))
    print('#' * 100)
    print('time cost :{}, \t({}) tasks all done!'.format(str(dt.datetime.now() - start_time)[:-7], len(tasks)))
    print('#' * 100)
    return result


def fun1(response, req_url):
    # pattern = re.compile("<a href='(.*?)'>(.*?)<")  # 正则表达式
    # result = list(set(re.findall(pattern, response)))  # 从主页面获取子页面的html
    soup = BeautifulSoup(response, 'lxml')
    a_list = soup.select('table.provincetable tr.provincetr > td > a')
    result = [[i['href'], i.text] for i in a_list]
    return [{'url': urllib.parse.urljoin(req_url, url),
             'kwargs': {
                 'address_list': [address]
             }} for url, address in result]


def current_level(soup):
    if soup.select('table.citytable tr.citytr'):
        return 2
    elif soup.select('table.countytable tr.countytr'):
        return 3
    elif soup.select('table.towntable tr.towntr'):
        return 4
    else:
        raise Exception('can not recognition current level')


def fun2(response, req_url, address_list):
    # pattern = re.compile("<a href='(.*?)'>(.*?)<")
    # result = list(set(re.findall(pattern, response)))
    soup = BeautifulSoup(response, 'lxml')
    a_list = soup.select('table tr > td:nth-of-type(2) > a')
    if a_list:
        delta_level = current_level(soup) - len(address_list) - 1
        assert delta_level >= 0, 'delta_level can not < 0'
        result = [[i['href'], i.text] for i in a_list]
        return [{'url': urllib.parse.urljoin(req_url, url),
                 'kwargs': {
                     'address_list': address_list + [''] * delta_level + [address]
                 }} for url, address in result]
    else:
        tr_list = soup.select('table.villagetable tr.villagetr')
        result = [[i('td')[0].text, i('td')[1].text, i('td')[2].text] for i in tr_list]
        return [address_list + [code1,
                                code2,
                                address,
                                '1' if code2[0] == '1' else '0',
                                ] for code1, code2, address in result]


def main():
    handle_dict = {
        '一级': fun1,
        '二级': fun2,
        '三级': fun2,
        '四级': fun2,
        '五级': fun2,
    }
    temp = []
    result = []
    for idx, (name, handle) in enumerate(handle_dict.items()):
        if idx == 0:
            # url_list = ['http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index.html']
            url_list = ['http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2019/index.html']
            handle_kwargs_list = [{}]
        else:
            # if idx == 1:
            #     temp = [i for i in temp if '广东省' in i['kwargs']['address_list']]
            url_list = []
            handle_kwargs_list = []
            for row in temp:
                if isinstance(row, dict):
                    url_list.append(row['url'])
                    handle_kwargs_list.append(row['kwargs'])
                else:
                    result.append(row)
            if DEBUG:
                url_list = url_list[:30]
                handle_kwargs_list = handle_kwargs_list[:30]

        temp = get_htmls_and_handle(url_list, handle, handle_kwargs_list, count=True)

    result.extend(temp)
    result.sort()
    with open(os.path.join(BASE_PATH, 'csv_{}.csv'.format(dt.datetime.now().strftime("%Y%m%d%H%M"))), 'w',
              newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        for row in result:
            writer.writerow(row)


if __name__ == '__main__':
    # ua = UserAgent(use_cache_server=False)
    if True:
        n = dt.datetime.now()
        request = urllib.request.Request(f"http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2014/11/01/12/110112105.html")
        with urllib.request.urlopen(request) as page:
            resp = page.read().decode(HTML_ENCODEING)
        print(dt.datetime.now() - n)
        print(resp)
        exit(0)

    ua = UserAgent(family='chrome')
    main()
