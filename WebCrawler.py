from bs4 import BeautifulSoup
from urllib import request
import requests
import re
import pandas as pd
import pymysql
import os


articleCnt = 0		# total article number
imageCnt = 0		# total image number
failPageUrl = []	# failed page urls (where article titles are in) when crawling article data
failArticleUrl = []		# failed article urls when crawling article data
failImagePageUrl = []	# failed page urls (where images are in) when crawling image data
failImageUrl = []		# failed image urls when crawling image data
allImageUrl = []		# all image urls, we do not need to store redundant image urls
failStockUrl = []		# failed urls when crawling stock data


# get article urls from the whole page
def getArticleUrl(url, hrefs):
    global failPageUrl
    # if timeout, retry up to 3 times
    i = 0
    while i < 3:
        try:
            html = requests.get(url, timeout=5)
            soup = BeautifulSoup(html.content, 'lxml')
            for item in soup.select("p.title a"):
                href = item.get("href")
                hrefs.append(href)
            return
        except requests.exceptions.RequestException as e:
            print("Connection timeout, retry...")
            i += 1
    # if still timeout
    if i >= 3:
        print("Cannot connect to " + url)
        failPageUrl.append(url)


# get article's title, time and content, and write them to file
def getArticleContent(url):
    global articleCnt
    global failArticleUrl
    hasTime = False
    hasDiscuss = False
    i = 0
    while i < 3:
        try:
            html = requests.get(url, timeout=5)
            soup = BeautifulSoup(html.content, 'lxml')
            # if there is no content in this page, just return and skip this url
            if len(soup.select("#ContentBody > p")) == 0:
                return
            title = soup.title.string
            # if there is update-time in this page, crawl and write it to file
            if len(soup.select(".time")) != 0:
                time = soup.select(".time")[0].get_text()
                hasTime = True
            # if there is discuss number in this page, crawl and write it to file
            if len(soup.select("span.num.ml5")) != 0:
                discussNum = soup.select("span.num.ml5")[0].get_text()
                hasDiscuss = True
            # get the file name from the url
            fileName = "CrawlData//ArticleData//" + url[-23:-5] + ".txt"
            articleCnt += 1
            with open(fileName, 'w+', encoding="utf-8") as f:
            	# write title to file
                f.write(title + '\n')
                # if exists, write time to file
                if hasTime == True:
                    f.write(time + '\n')
                # write article content to file
                for item in soup.select("#ContentBody > p"):
                    f.write(item.get_text() + '\n')
                # write discuss number to file
                if hasDiscuss == True:
                    f.write(discussNum + "人参与讨论" + '\n')
                print("Write: " + title)
            # store article url to a file
            with open("CrawlData//ArticleUrl.txt", 'a+', encoding="utf-8") as f:
                f.write(url + '\n')
            return
        except requests.exceptions.RequestException as e:
            print("Connection timeout, retry...")
            i += 1
    if i >= 3:
        print("Cannot connect to " + url)
        failArticleUrl.append(url)


# main function of writing article data
def writeArticleData():
    rootUrls = [
        "http://finance.eastmoney.com/news/cjjsp_",
        "http://finance.eastmoney.com/a/ccjdd_",
        "http://finance.eastmoney.com/news/cgspl_",
        "http://finance.eastmoney.com/news/cgnjj_",
        "http://finance.eastmoney.com/a/czqyw_",
        "http://finance.eastmoney.com/news/cgjjj_",
        "http://finance.eastmoney.com/a/chgyj_",
        "http://finance.eastmoney.com/a/cssgs_",
        "http://hk.eastmoney.com/a/cgsbd_",
        "http://stock.eastmoney.com/a/czggng_",
        "http://stock.eastmoney.com/a/cmgpj_",
        "http://finance.eastmoney.com/a/ccjxw_",
        "http://finance.eastmoney.com/a/ccyts_",
        "http://biz.eastmoney.com/a/csyzx_",
        "http://finance.eastmoney.com/news/csygc_",
        "http://stock.eastmoney.com/news/chyyj_"
    ]
    # for each root url
    for rootUrl in rootUrls:
        # for each page (25 pages in total)
        for i in range(25):
            articleHrefs = []	# used to store article urls
            url = rootUrl + str(i + 1) + ".html"
            getArticleUrl(url, articleHrefs)
            if len(articleHrefs) == 0:
                print("Connection failed, skip this page.")
                continue
            # for each article
            for articleHref in articleHrefs:
                getArticleContent(articleHref)
            print("Write page: " + str(i + 1) + " for " + rootUrl)
    print("Write %d articles in total." % articleCnt)
    # write failed page and article urls to files
    with open("CrawlData//FailPageUrl.txt", 'a+', encoding="utf-8") as f:
        for line in failPageUrl:
            f.write(line + '\n')
    with open("CrawlData//FailArticleUrl.txt", 'a+', encoding="utf-8") as f:
        for line in failArticleUrl:
            f.write(line + '\n')
    # eliminate redundant urls in ArticleUrl.txt
    with open("CrawlData//ArticleUrl.txt", 'r+', encoding="utf-8") as f:
        tmpList = f.readlines()
        f.seek(0)
        f.truncate()
        tmpList = list(set(tmpList))
        for line in tmpList:
            f.write(line)


# get image urls from each article's page
def getImageUrl(url, imageUrls):
    global failImagePageUrl
    global allImageUrl
    # only crawl three types of images for simplicity
    imageType = [".jpg", ".png", ".gif"]
    i = 0
    while i < 3:
        try:
            html = requests.get(url, timeout=5)
            soup = BeautifulSoup(html.content, 'lxml')
            images = soup.find_all('img')
            for image in images:
                imageUrl = image.get('src')
                # many images may be redundant, so check if imageUrl is already in imageUrls or allImageUrl
                if (imageUrl[-4:] in imageType) and (imageUrl not in imageUrls) and (imageUrl not in allImageUrl):
                    if imageUrl[0:4] == "http":
                        imageUrls.append(imageUrl)
                        allImageUrl.append(imageUrl)
                    # some urls may not have host names, so add it
                    else:
                        imageUrls.append("https:" + imageUrl)
                        allImageUrl.append("https:" + imageUrl)
            return
        except requests.exceptions.RequestException as e:
            print("Connection timeout, retry...")
            i += 1
    if i >= 3:
        print("Cannot connect to " + url)
        failImagePageUrl.append(url)


# get each image's content and write it to file
def getImageContent(url):
    global imageCnt
    global failImageUrl
    i = 0
    while i < 3:
        try:
        	# get image file's name from its url
            fileName = "CrawlData//ImageData//" + url[-10:]
            request.urlretrieve(url, fileName)
            imageCnt += 1
            # write image url to a file
            with open("CrawlData//ImageUrl.txt", 'a+', encoding="utf-8") as f:
                f.write(url + '\n')
            return
        except:
            print("Connection timeout, retry...")
            i += 1
    if i >= 3:
        print("Cannot connect to " + url)
        failImageUrl.append(url)


# main function of writing image data
def writeImageData():
	# we crawl the images in each article's page, so use those urls
    with open("CrawlData//ArticleUrl.txt", 'r', encoding="utf-8") as f:
        articleUrls = f.readlines()
    cnt = 1
    for i in range(len(articleUrls)):
        url = articleUrls[i]
        imageUrls = []
        getImageUrl(url, imageUrls)
        if len(imageUrls) == 0:
            print("Connection failed, skip this url.")
            continue
        imageUrls = list(set(imageUrls))
        for imageUrl in imageUrls:
            getImageContent(imageUrl)
        print("Download images on %s, %d" % (url, cnt))
        cnt += 1
    print("Download %d images in total." % imageCnt)
    with open("CrawlData//FailImagePageUrl.txt", 'a+', encoding="utf-8") as f:
        for line in failImagePageUrl:
            f.write(line + '\n')
    with open("CrawlData//FailImageUrl.txt", 'a+', encoding="utf-8") as f:
        for line in failImageUrl:
            f.write(line + '\n')
    # eliminate redundant urls in ImageUrl.txt
    with open("CrawlData//ImageUrl.txt", 'r+', encoding="utf-8") as f:
        tmpList = f.readlines()
        f.seek(0)
        f.truncate()
        tmpList = list(set(tmpList))
        for line in tmpList:
            f.write(line)


# get raw data from the url
def getHtml(paraStr1, paraStr2, pageNum):
    global failStockUrl
    url = "http://50.push2.eastmoney.com/api/qt/clist/get?cb=jQuery11240024537486996756952_1576655935866&pn=" + str(pageNum) + \
          "&pz=20&po=1&np=1&ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&invt=2&fid=f3&fs=" + paraStr1 + \
          "&fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f22,f11,f62,f128,f136,f115,f152&_=" + str(paraStr2)
    # if timeout, retry up to 3 times
    i = 0
    while i < 3:
        try:
            r = requests.get(url, timeout=5)   # GET method
            regex1 = "\[(.*?)\]"
            regex2 = "{(.*?)}"
            allData = re.compile(regex1, re.S).findall(r.text)
            if len(allData) == 0:
                return None
            data = re.compile(regex2, re.S).findall(allData[0])
            return data
        except requests.exceptions.RequestException as e:
            print("Connection timeout, retry...")
            i += 1
    if i >= 3:
        print("Cannot connect to " + url)
        failStockUrl.append(url)


# get one page's stock data
def getOnePageData(paraStr1, paraStr2, pageNum):
    data = getHtml(paraStr1, paraStr2, pageNum)
    indexNames = ["f12", "f14", "f2", "f3", "f4", "f5", "f6", "f7", "f15", "f16", "f17", "f18", "f10", "f8", "f9", "f23"]
    results = []
    for i in range(len(data)):
        result = []
        for j in range(len(indexNames)):
            beginPos = data[i].find(indexNames[j])
            endPos = data[i].find(',', beginPos)
            if j == 0 or j == 1:
                result.append(data[i][(beginPos + len(indexNames[j]) + 3):(endPos - 1)])
            else:
                result.append(data[i][(beginPos + len(indexNames[j]) + 2):endPos])
        results.append(result)
    return results


# main function of writing stock data
def writeStockData():
    paraStrs1 = {
        "上证A股": "m:1+t:2,m:1+t:23",
        "沪深A股": "m:0+t:6,m:0+t:13,m:0+t:80,m:1+t:2,m:1+t:23",
        "深证A股": "m:0+t:6,m:0+t:13,m:0+t:80",
        "新股": "m:0+f:8,m:1+f:8",
        "中小版": "m:0+t:13",
        "创业版": "m:0+t:80",
        "科创版": "m:1+t:23",
        "沪股通": "b:BK0707",
        "深股通": "b:BK0804",
        "B股": "m:0+t:7,m:1+t:3"
    }
    paraStrs2 = {
        "上证A股": "1576655936001",
        "沪深A股": "1576655936006",
        "深证A股": "1576655935963",
        "新股": "1576655935965",
        "中小版": "1576655935976",
        "创业版": "1576655935981",
        "科创版": "1576655935985",
        "沪股通": "1576655935988",
        "深股通": "1576655935992",
        "B股": "1576655935996"
    }
    columns = [
        "代码",
        "名称",
        "最新价",
        "涨跌幅",
        "涨跌额",
        "成交量（手）",
        "成交额",
        "振幅",
        "最高",
        "最低",
        "今开",
        "昨收",
        "量比",
        "换手率",
        "市盈率（动态）",
        "市净率"
    ]
    for i in paraStrs1.keys():
        pageNum = 1
        datas = getOnePageData(paraStrs1[i], paraStrs2[i], pageNum)
        # crawl until there is no more pages
        while True:
            pageNum += 1
            if getHtml(paraStrs1[i], paraStrs2[i], pageNum):
                datas.extend(getOnePageData(paraStrs1[i], paraStrs2[i], pageNum))
                print("Finished " + i + " Page " + str(pageNum))
            else:
                break
        df = pd.DataFrame(datas)
        df.columns = columns
        # write stock data to a csv file
        df.to_csv("CrawlData//StockData//" + i + ".csv", encoding="utf_8_sig")
        print("Finished " + i)
    with open("CrawlData//FailStockUrl.txt", 'a+', encoding="utf-8") as f:
        for line in failStockUrl:
            f.write(line + '\n')

# store stock data to mysql database (not necessary)
def storeStockData():
	# mysql username and password should be changed to the user's own
    name = "root"
    password = "WLX643204"
    try:
        pymysql.connect("localhost", name, password)
    except:
        print("Database connect error!")
    db = pymysql.connect("localhost", name, password, charset="utf8")
    print("Database connect succeed!")
    cursor = db.cursor()
    # create database
    sqlStr1 = "create database if not exists stockDatabase"
    cursor.execute(sqlStr1)
    sqlStr2 = "use stockDatabase"
    cursor.execute(sqlStr2)
    # get files
    filePath = "CrawlData//StockData//"
    fileList = os.listdir(filePath)
    cnt = 1
    for fileName in fileList:
        data = pd.read_csv(filePath + fileName, encoding="utf8")
        # create table
        try:
            sqlStr3 = "create table stockData_" + str(cnt) + \
                      "(代码 VARCHAR(10), 名称 VARCHAR(50), 最新价 float, 涨跌幅 float, " + \
                      "涨跌额 float, 成交量（手） float, 成交额 float, 振幅 float, 最高 float, 最低 float, " + \
                      "今开 float, 昨收 float, 量比 float, 换手率 float, 市盈率（动态） float, 市净率 float)"
            cursor.execute(sqlStr3)
            print("Creating table stockData_" + str(cnt))
        except:
            print("Table stockData_" + str(cnt) + "already exists!")
        print("Begin storing " + fileName + ":")
        for i in range(len(data)):
            record = list(data.loc[i])
            del record[0]
            record = tuple(record)
            # insert each record
            try:
                sqlStr4 = "insert into stockData_" + str(cnt) + \
                          "(代码, 名称, 最新价, 涨跌幅, 涨跌额, 成交量（手）, 成交额, 振幅, 最高, 最低, 今开, 昨收, " + \
                          "量比, 换手率, 市盈率（动态）, 市净率) values ('%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" % record
                sqlStr4 = sqlStr4.replace('"-"', "null")
                cursor.execute(sqlStr4)
                print("Insert record %d" % i)
            except:
                print("Error happens when inserting record %d" % i)
                traceback.print_exc()
                break
        print("Finished storing " + fileName)
        cnt += 1
    cursor.close()
    db.commit()
    db.close()


writeStockData()
storeStockData()
writeArticleData()
writeImageData()
