from bs4 import BeautifulSoup
from urllib import request
import requests
import re
import pandas as pd
import cv2
import numpy as np


checkStockNum = 0       # the number of stock records checked
checkArticleNum = 0     # the number of articles checked
checkImageNum = 0       # the number of images checked
totalStockError = 0     # stock data errors
totalArticleError = 0   # article data errors 
totalImageError = 0     # image data errors
failStockUrl = []       # failed stock urls
failImageUrl = []       # failed image urls
failArticleUrl = []     # failed article urls


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
    # since most stock datas change every day, we only check codes and names
    indexNames = ["f12", "f14"]
    results = []
    for i in range(len(data)):
        result = []
        for j in range(len(indexNames)):
            beginPos = data[i].find(indexNames[j])
            endPos = data[i].find(',', beginPos)
            result.append(data[i][(beginPos + len(indexNames[j]) + 3):(endPos - 1)])
        results.append(result)
    return results


# main function of verifying stock data
def verifyStockData():
    global totalStockError
    global checkStockNum
    # missed [Code, Name] matches
    missCodeName = []
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
    # create empty lists, e.g. code_name1, code_name2, ...
    listNames = locals()
    for i in range(10):
        listNames["code_name" + str(i + 1)] = []
    # read expected results
    cnt = 0
    for i in paraStrs1.keys():
        df = pd.read_csv("CrawlData//StockData//" + i + ".csv", converters={"代码": str}, \
                         usecols=["代码", "名称"], encoding="utf_8_sig")
        tmpList1 = list(df["代码"])
        tmpList2 = list(df["名称"])
        for j in range(len(tmpList1)):
            tmp = []
            tmp.append(tmpList1[j])
            tmp.append(tmpList2[j])
            listNames["code_name" + str(cnt + 1)].append(tmp)
        cnt += 1
    # write verification results to a log file
    file = open("VerifyData//StockErrorLog.txt", "a+", encoding="utf-8")
    cnt = 0
    tmpStockError = 0
    for i in paraStrs1.keys():
        pageNum = 1
        datas = getOnePageData(paraStrs1[i], paraStrs2[i], pageNum)
        # crawl until there is no more pages
        while True:
            pageNum += 1
            if getHtml(paraStrs1[i], paraStrs2[i], pageNum):
                datas.extend(getOnePageData(paraStrs1[i], paraStrs2[i], pageNum))
                print("Got " + i + " Page " + str(pageNum))
            else:
                break
        print("Start validating " + i + ":")
        file.write("Start validating " + i + ":" + '\n')
        for code_name in datas:
            # if cannot find this [Code, Name] match, we see it as an error
            if code_name not in listNames["code_name" + str(cnt + 1)]:
                print("Missing [Code, Name] = ", end = "")
                print(code_name)
                file.write("Missing [Code, Name] = [" + ", ".join(code_name) + "]\n")
                if code_name not in missCodeName:
                    missCodeName.append((code_name))
                tmpStockError += 1
        print("Finished validating %s." % i)
        file.write("Finished validating %s\n" % i)
        print("Found %d errors in %s." % (tmpStockError, i))
        file.write("Found %d error(s) in %s.\n" % (tmpStockError, i))
        checkStockNum += len(datas)
        totalStockError += tmpStockError
        tmpStockError = 0
        cnt += 1
        file.flush()
    file.write("Verified %d records in StockData.\n" % checkStockNum)
    file.write("Found %d error(s) in total in StockData.\n" % totalStockError)
    if len(missCodeName) != 0:
        file.write("Missing [Code, Name] matches are:\n")
        for code_name in missCodeName:
            file.write("Missing [Code, Name] = [" + ", ".join(code_name) + "]\n")
    file.close()
    # if there are failed urls
    if len(failStockUrl) != 0:
        with open("VerifyData//FailStockUrl.txt", "a+", encoding="utf-8") as f:
            for line in failStockUrl:
                f.write(line + '\n')


# main function of verifying article data
def verifyArticleData():
    global failArticleUrl
    global checkArticleNum
    global totalArticleError
    failTitle = []  # error article urls, title mismatch
    failTime = []   # error article urls, time mismatch
    failSource = [] # error article urls, source mismatch
    failEditor = [] # error article urls, editor mismatch
    with open("CrawlData//ArticleUrl.txt", "r", encoding="utf-8") as f:
        articleUrls = f.readlines()
    file = open("VerifyData//ArticleErrorLog.txt", "a+", encoding="utf-8")
    for i in range(len(articleUrls)):
        url = articleUrls[i]
        fileName = "CrawlData//ArticleData//" + url[-24:-6] + ".txt"
        hasTime = False
        hasSource = False
        hasEditor = False
        hasDiscuss = False
        hasTip = False
        flag = True
        # if timeout, retry up to 3 times
        j = 0
        while j < 3:
            try:
                html = requests.get(url, timeout=5)
                soup = BeautifulSoup(html.content, 'lxml')
                title = soup.title.string
                if len(soup.select("p.em_media")) != 0:
                    source = soup.select("p.em_media")[0].get_text()
                    hasSource = True
                if len(soup.select("p.res-edit")) != 0:
                    editor = soup.select("p.res-edit")[0].get_text()
                    hasEditor = True
                if len(soup.select(".time")) != 0:
                    time = soup.select(".time")[0].get_text()
                    hasTime = True
                if len(soup.select("span.num.ml5")) != 0:
                    hasDiscuss = True
                if len(soup.select("p.tip")) != 0:
                    hasTip = True
                # read expected result
                # since article's content may be changed easily, we only consider some factors
                with open(fileName, "r", encoding="utf-8") as f:
                    fileContent = f.readlines()
                    expectTitle = fileContent[0]
                    if (title.strip()) != (expectTitle.strip()):
                        flag = False
                        failTitle.append(url)
                    if hasTime == True:
                        expectTime = fileContent[1]
                        if (time.strip()) != (expectTime.strip()):
                            flag = False
                            failTime.append(url)
                    if hasSource == True:
                        if hasDiscuss == True and hasTip == True:
                            expectSource = fileContent[-8]
                        elif hasDiscuss == True and hasTip == False:
                            expectSource = fileContent[-7]
                        elif hasDiscuss == False and hasTip == True:
                            expectSource = fileContent[-7]
                        elif hasDiscuss == False and hasTip == False:
                            expectSource = fileContent[-6]
                        if (source.strip()) != (expectSource.strip()):
                            flag = False
                            failSource.append(url)
                    if hasEditor == True:
                        if hasDiscuss == True and hasTip == True:
                            expectEditor = fileContent[-5]
                        elif hasDiscuss == True and hasTip == False:
                            expectEditor = fileContent[-4]
                        elif hasDiscuss == False and hasTip == True:
                            expectEditor = fileContent[-4]
                        elif hasDiscuss == False and hasTip == False:
                            expectEditor = fileContent[-3]
                        if (editor.strip()) != (expectEditor.strip()):
                            flag = False
                            failEditor.append(url)
                if flag == True:
                    print("Pass %s" % title)
                    file.write("Pass %s\n" % title)
                else:
                    print("Failed %s" % title)
                    file.write("Failed %s\n" % title)
                    totalArticleError += 1
                checkArticleNum += 1
                break;
            except requests.RequestException:
                print("Connection timeout, retry...")
                j += 1
        if j >= 3:
            print("Cannot connect to " + url)
            failArticleUrl.append(url)
        file.flush()
    file.write("Verified %d articles in ArticleData.\n" % checkArticleNum)
    file.write("Found %d error(s) in total in ArticleData.\n" % totalArticleError)
    if len(failTitle) != 0:
        file.write("Title error(s) are:\n")
        for line in failTitle:
            file.write(line + '\n')
    if len(failTime) != 0:
        file.write("Time error(s) are:\n")
        for line in failTime:
            file.write(line + '\n')
    if len(failSource) != 0:
        file.write("Source error(s) are:\n")
        for line in failSource:
            file.write(line + '\n')
    if len(failEditor) != 0:
        file.write("Editor error(s) are:\n")
        for line in failEditor:
            file.write(line + '\n')
    file.close()
    # if there are failed urls
    if len(failArticleUrl) != 0:
        with open("VerifyData//FailArticleUrl.txt", "a+", encoding="utf-8") as f:
            for line in failArticleUrl:
                f.write(line + '\n')


# main function of verifying image data
def verifyImageData():
    global failImageUrl
    global checkImageNum
    global totalImageError
    with open("CrawlData//ImageUrl.txt", "r", encoding="utf-8") as f:
        imageUrls = f.readlines()
    file = open("VerifyData//ImageErrorLog.txt", "a+", encoding="utf-8")
    for i in range(len(imageUrls)):
        url = imageUrls[i]
        actualImageName = "VerifyData//ActualImageData//" + url[-11:-1]
        expectImageName = "CrawlData//ImageData//" + url[-11:-1]
        j = 0
        while j < 3:
            try:
                request.urlretrieve(url, actualImageName)
                # use openCV to check whether two images are the same
                expectImage = cv2.imread(expectImageName)
                actualImage = cv2.imread(actualImageName)
                flag = not np.any(cv2.subtract(expectImage, actualImage))
                if flag == True:
                    print("Pass %s" % url[-11:-1])
                    file.write("Pass %s\n" % url[-11:-1])
                else:
                    totalImageError += 1
                    print("Failed %s, url is %s" % (url[-11:-1], url))
                    file.write("Failed %s, url is %s\n" % (url[-11:-1], url))
                checkImageNum += 1
                break;
            except requests.RequestException:
                print("Connection timeout, retry...")
                j += 1
        if j >= 3:
            print("Cannot connect to " + url)
            failImageUrl.append(url)
        file.flush()
    file.write("Verified %d images in ImageData.\n" % checkImageNum)
    file.write("Found %d error(s) in total in ImageData." % totalImageError)
    file.close()
    # if there are failed urls
    if len(failImageUrl) != 0:
        with open("VerifyData//FailImageUrl.txt", "a+", encoding="utf-8") as f:
            for line in failImageUrl:
                f.write(line + '\n')


verifyStockData()
verifyArticleData()
verifyImageData()
print("Verified %d records in StockData." % checkStockNum)
print("Found %d error(s) in total in StockData." % totalStockError)
print("Verified %d articles in ArticleData." % checkArticleNum)
print("Found %d error(s) in total in ArticleData." % totalArticleError)
print("Verified %d images in ImageData." % checkImageNum)
print("Found %d error(s) in total in ImageData." % totalImageError)
