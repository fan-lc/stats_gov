# stats_gov
统计局网站的数据汇总(http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/)。
细粒度，到最后一级(一般为5级，网站上少部分地区为4级)。数据编码格式为utf8，以便显示名称中的生僻字，请使用合适的文本工具打开。

另附python爬虫代码和所需库，使用**命令.txt**中命令安装即可。
``` bash
pip install --no-index --find-links=.\pack -r requirements.txt
```
爬取速度快，网速较好时10分钟左右。

asyncio.Semaphore()值最好设置为较小量(不超过100)，以防网站崩溃。
``` python
sem = asyncio.Semaphore(60)
```
