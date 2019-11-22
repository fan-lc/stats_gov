import functools, os, time, aiohttp, asyncio, random, re, csv, urllib.parse
import datetime as dt
from bs4 import BeautifulSoup
from fake_useragent import UserAgent


DEBUG = False

HTML_ENCODEING = 'gb18030'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
BASE_PATH = '../China_Province_2018/'
if not os.path.isdir(BASE_PATH):
    os.mkdir(BASE_PATH)


async def get_html(sem, url, handle, result, counter=None):
    if not isinstance(result, list):
        raise TypeError('result must be a list')
    if not isinstance(url, str):
        raise TypeError('url must be a string')
    if not isinstance(counter, dict):
        counter = {'all': 0, 'done': -1, 'now': dt.datetime.now()}
    async with sem:
        async with aiohttp.ClientSession() as session:
            while True:
                status = -1
                try:
                    async with session.get(url, headers={'user-agent': ua.random}, timeout=20) as resp:
                        if resp.status != 200:
                            status = resp.status
                            raise Exception(str(status))
                        else:
                            # response = await resp.content.read()
                            try:
                                response = await resp.text(HTML_ENCODEING)
                            except TimeoutError as e:
                                raise e
                            except aiohttp.ClientPayloadError as e:
                                raise e
                            except (UnicodeDecodeError, UnicodeEncodeError) as e:
                                try:
                                    t = await resp.text('utf-8',errors='ignore')
                                except Exception as e:
                                    print(repr(e))
                                    exit(0)
                                else:
                                    if '请开启JavaScript并刷新该页' in t:
                                        raise Exception('请开启JavaScript并刷新该页')
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
                            print('{} \tsuccess! \t({}/{}) \t{}'.format(url, counter['done'], counter['all'],
                                                                        str(dt.datetime.now() - counter['now'])[:-7]))
                            break
                except Exception as e:
                    await asyncio.sleep(3 + random.random() * 7)
                    print('{} \tretry due to status:{}\t{}'.format(url, status, repr(e)))


def get_htmls_and_handle(url_list, handle, handle_kwargs_list, count=False):
    sem = asyncio.Semaphore(60)
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
    a_list = soup.select('table.provincetable > tr.provincetr > td > a')
    result = [[i['href'], i.text] for i in a_list]
    return [{'url': urllib.parse.urljoin(req_url, url),
             'kwargs': {
                 'address_list': [address]
             }} for url, address in result]


def current_level(soup):
    if soup.select('table.citytable > tr.citytr'):
        return 2
    elif soup.select('table.countytable > tr.countytr'):
        return 3
    elif soup.select('table.towntable > tr.towntr'):
        return 4
    else:
        raise Exception('can not recognition current level')


def fun2(response, req_url, address_list):
    # pattern = re.compile("<a href='(.*?)'>(.*?)<")
    # result = list(set(re.findall(pattern, response)))
    soup = BeautifulSoup(response, 'lxml')
    a_list = soup.select('table > tr > td:nth-of-type(2) > a')
    if a_list:
        delta_level = current_level(soup) - len(address_list) - 1
        assert delta_level >= 0, 'delta_level can not < 0'
        result = [[i['href'], i.text] for i in a_list]
        return [{'url': urllib.parse.urljoin(req_url, url),
                 'kwargs': {
                     'address_list': address_list + [''] * delta_level + [address]
                 }} for url, address in result]
    else:
        tr_list = soup.select('table.villagetable > tr.villagetr')
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
            url_list = ['http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/index.html']
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
    ua = UserAgent(use_cache_server=False)
    main()
