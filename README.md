# stats_gov
统计局网站的数据汇总(http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2018/)。
细粒度，到最后一级(一般为5级，网站上少部分地区为4级)。数据编码格式为utf8，以便显示名称中的生僻字，请使用合适的文本工具打开。</br>
这里有python爬虫代码和所需库。爬取速度快，网速较好时10分钟左右。

## Results
|province|city|county|town|code1|code2|village|根据code2第一位|
| ------ | ------ | ------ | ------ | ------ | ------ | ------ | ------ |
|上海市|市辖区|嘉定区|华亭镇|310114111001|220|袁家桥社区居委会|0|
|上海市|市辖区|嘉定区|华亭镇|310114111002|121|沁园社区居委会|1|
|上海市|市辖区|嘉定区|华亭镇|310114111003|220|华旺社区居委会|0|
|上海市|市辖区|嘉定区|华亭镇|310114111201|220|联一村村委会|0|
|上海市|市辖区|嘉定区|华亭镇|310114111203|220|联三村村委会|0|

## Prerequisites
代码基于python3.6
- [**python3.6.6**](https://www.python.org/downloads/release/python-366/) ：python 官网下载，选择适合的版本；

如果平台为windows 64位，依赖库使用**命令.txt**中命令安装即可。
``` bash
pip install --no-index --find-links=.\pack -r requirements.txt
```
其他平台，使用命令自行下载安装依赖库
``` bash
pip install -r requirements.txt
```

asyncio.Semaphore()值最好设置为较小量(不超过100)，以防网站崩溃。
``` python
sem = asyncio.Semaphore(60)
```

## run
``` bash
python stats_gov_2.py
```
