import csv
import json
from multiprocessing.dummy import Pool as ThreadPool
import pandas as pd
from queue import Queue
import requests
import sys
import threading
import time
import urllib.request

start = time.time()
pool = ThreadPool(10)
lock = threading.Lock()

def getidx(year, quarter):
    idx = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR{}/master.idx'.format(year,quarter)
    return idx

def getyidx(year):
    quarters = [i for i in range(1,5)]
    yidx = []
    for quarter in quarters:
        yidx.append(getidx(year, quarter))
    return yidx

def checklink(Year, Type ,CIK):
    FILE = "10-K"
    CIK = str(CIK)
    urlmid = 'edgar/data/'
    urlfront = 'https://www.sec.gov/Archives/'
    quarters = [i for i in range(1,5)]
    
    Klinks = []
    KAlinks = []
    NTlinks = []
    links = Klinks, KAlinks, NTlinks

    for quarter in quarters:
        idx = 'https://www.sec.gov/Archives/edgar/full-index/{}/QTR{}/master.idx'.format(Year,quarter)
        
        response = urllib.request.urlopen(idx)    
        element2 = None
        element3 = None
        element4 = None

        for line in response:
            line = str(line)
            if FILE in line and CIK in line:
                for element in line.split(' '):
                    if urlmid in element:
                        element2 = element.split('|')
                        for element3 in element2:
                            if urlmid in element3:
                                urlback = element3[:-3]
                                url = urlfront + urlback
                                if not "10-k/a" in line.lower() and not "10-K" in line.lower():
                                    Klinks.append(url)
                                    print(line)
                                if "10-k/a" in line.lower():
                                    KAlinks.append(url)
                                    print(line)
                                if "NT 10-K" in line:
                                    NTlinks.append(url)
                                    print(line)
    if Type.lower() == "k":
        return Klinks
    elif Type.lower() == "ka":
        return KAlinks
    elif Type.lower() == "nt":
        return KAlinks

    return links

def getKlist(idx):
    FILE = "10-K"
    urlmid = 'edgar/data/'
    urlfront = 'https://www.sec.gov/Archives/'
    year = idx.split("/")[6]
    quarter = idx.split("/")[7]
    with lock:
        print("{} {} ".format(year,quarter))
    time.sleep(1)
    
    response = urllib.request.urlopen(idx)    
    element2 = None
    element3 = None
    element4 = None

    Kurls = []
    for line in response:
        line = str(line)
        if FILE in line:
            for element in line.split(' '):
                if urlmid in element:
                    element2 = element.split('|')
                    for element3 in element2:
                        if urlmid in element3:
                            urlback = element3[:-3]
                            url = urlfront + urlback
                            if not "10-K/A" in line and not "NT 10-K" in line:
                                Kurls.append(url)
    return Kurls

def getKAlist(idx):
    FILE = "10-K"
    urlmid = 'edgar/data/'
    urlfront = 'https://www.sec.gov/Archives/'
    year = idx.split("/")[6]
    quarter = idx.split("/")[7]
    with lock:
        print("{} {} ".format(year,quarter))
    time.sleep(1)
    
    response = urllib.request.urlopen(idx)
    element2 = None
    element3 = None
    element4 = None

    KAurls = []    
    for line in response:
        line = str(line)
        if FILE in line:
            for element in line.split(' '):
                if urlmid in element:
                    element2 = element.split('|')
                    for element3 in element2:
                        if urlmid in element3:
                            urlback = element3[:-3]
                            url = urlfront + urlback
                            if "10-K/A" in line:
                                KAurls.append(url)
    return KAurls

def getNTlist(idx):
    FILE = "10-K"
    urlmid = 'edgar/data/'
    urlfront = 'https://www.sec.gov/Archives/'
    year = idx.split("/")[6]
    quarter = idx.split("/")[7]

    with lock:
        print("{} {} ".format(year,quarter))
    time.sleep(1)
    
    response = urllib.request.urlopen(idx)
    element2 = None
    element3 = None
    element4 = None

    NTurls = []    
    for line in response:
        line = str(line)
        if FILE in line:
            for element in line.split(' '):
                if urlmid in element:
                    element2 = element.split('|')
                    for element3 in element2:
                        if urlmid in element3:
                            urlback = element3[:-3]
                            url = urlfront + urlback
                            if "NT 10-K" in line:
                                if not "10-K/A" in line:
                                    NTurls.append(url)
    return NTurls

def getyearK(year):
    yidx = getyidx(year)
    yearKlist = []
    i = pool.map(getKlist, yidx)
    for j in i:
        for k in j:
            with lock:
                yearKlist.append(k)
    return yearKlist

def getyearKA(year):
    yidx = getyidx(year)
    yearKAlist = []
    i = pool.map(getKAlist, yidx)
    for j in i:
        for k in j:
            with lock:
                yearKAlist.append(k)
    return yearKAlist

def getyearNT(year):
    yidx = getyidx(year)
    yearNTlist = []
    i = pool.map(getNTlist, yidx)
    for j in i:
        for k in j:
            with lock:
                yearNTlist.append(k)
    return yearNTlist
    
def getinfo(url):
    try:
        CIK = int(url.split("/")[6])
    except:
        print(url)
        
    time.sleep(1)
    
    with urllib.request.urlopen(url) as response:
       response3 = str(response.read())
    words = response3.lower().split()

    usi = [] #index for "unresolved staff comments" with "1b"
    rfi = [] #index for "risk factors" with same formatting as second urc occurrence
    usB = [] #"unresolved staff comments" + back
    
    dbi = [] #index for "data breach"
    sbi = [] #index for "security breach"
    hki = [] #index for "hack"
    ini = [] #index for "intrusion" and "intrude"
    
    for i,j in enumerate(words):
        if "unresolved" in j:
            if "staff" in words[i+1]:
                string = " ".join(words[i-10:i+5])
                if "1b" in string:
                    usi.append(i)
                plusminus = " ".join(words[i:i+4])
                usB.append(plusminus)

        if "breach" in j:
            if "data" in words[i-1]:
                dbi.append(i)
            if "security" in words[i-1]:
                sbi.append(i)
        if "hack" in j and "hackett" not in j:
            hki.append(i)
        if "intrusion" in j or "intrude" in j:
            ini.append(i)
    for i,j in enumerate(words):
        if "factors" in j and len(usi) == 2:
            if "risk" in words[i-1]:
                string = " ".join(words[i-1:i+2])
                if string[-3:] in usB[1][-3:]:
                    rfi.append(i)
    
    db_t = len(dbi)
    sb_t = len(sbi)
    hk_t = len(hki)
    in_t = len(ini)
    
    db_s1 = 0
    sb_s1 = 0
    hk_s1 = 0
    in_s1 = 0
    
    db_s2 = 0
    sb_s2 = 0
    hk_s2 = 0
    in_s2 = 0
    
    if len(usi) != 2:
        db_s1 = db_t
        sb_s1 = sb_t
        hk_s1 = hk_t
        in_s1 = in_t

        db_s2 = db_t
        sb_s2 = sb_t
        hk_s2 = hk_t
        in_s2 = in_t
    else:
        Upper = usi[1]
        Lower1 = usi[0]
        Lower2 = Lower1
        
        if len(rfi) == 1:
            Lower2 = rfi[0]
            
        for i in dbi:
            if i < Upper:
                if i > Lower1:
                    db_s1 += 1
                if i > Lower2:
                    db_s2 +=1
        for i in sbi:
            if i < Upper:
                if i > Lower1:
                    sb_s1 += 1
                if i > Lower2:
                    sb_s2 += 1
        for i in hki:
            if i < Upper:
                if i > Lower1:
                    hk_s1 += 1
                if i > Lower2:
                    hk_s2 += 1
        for i in ini:
            if i < Upper:
                if i > Lower1:
                    in_s1 += 1
                if i > Lower2:
                    in_s2 += 1
                    
    out = {"Year":None,
           "Type":None,
           "CIK":[CIK],
           
           "DB_T":[db_t],
           "SB_T":[sb_t],
           "HK_T":[hk_t],
           "IN_T":[in_t],
           
           "DB_S1":[db_s1],
           "SB_S1":[sb_s1],
           "HK_S1":[hk_s1],
           "IN_S1":[in_s1],
           
           "DB_S2":[db_s2],
           "SB_S2":[sb_s2],
           "HK_S2":[hk_s2],
           "IN_S2":[in_s2],
           }
    out = pd.DataFrame.from_dict(out)
    return out

def exportlinks(start_year, end_year):
    for year in range(start_year,end_year+1):
        K = getyearK(year)
        KA = getyearKA(year)
        NT = getyearNT(year)
        
        with open("{}_K.json".format(year),"w") as f:
            json.dump(K,f)
            print("{} 10-K done".format(year))        
        with open("{}_KA.json".format(year),"w") as f:
            json.dump(KA,f)
            print("{} 10-K/A done".format(year))        
        with open("{}_NT.json".format(year),"w") as f:
            json.dump(NT,f)
            print("{} NT 10-K done".format(year))
        
def checklinks(start_year, end_year):
    for year in range(start_year,end_year+1):
        K = getyearK(year)
        KA = getyearKA(year)
        NT = getyearNT(year)

        kdf = pd.read_json("{}_K.json".format(year))
        kadf = pd.read_json("{}_KA.json".format(year))
        ntdf = pd.read_json("{}_NT.json".format(year))

        kdiff = len(K) - len(kdf)
        kadiff = len(KA) - len(kadf)
        ntdiff = len(NT) - len(ntdf)

        if kdiff == 0:
            print("K list correct")
        else:
            print("Expected : {}, Actual : {}".format(len(K),len(kdf)))
        if kadiff == 0:
            print("KA list correct")
        else:
            print("Expected : {}, Actual : {}".format(len(KA),len(kadf)))
        if ntdiff == 0:
            print("NT list correct")
        else:
            print("Expected : {}, Actual : {}".format(len(NT),len(ntdf)))

start_year = 2005
end_year = 2017
types = ["K","KA","NT"]
years = [str(i) for i in range(start_year,end_year + 1)]

df = pd.DataFrame()
df05 = df
df06 = df
df07 = df
df08 = df
df09 = df
df10 = df
df11 = df
df12 = df
df13 = df
df14 = df
df15 = df
df16 = df
df17 = df

yrdf = {"2005" : df05, "2006" : df06, "2007" : df07, 
        "2008" : df08, "2009" : df09, "2010" : df10,
        "2011" : df11, "2012" : df12, "2013" : df13,
        "2014" : df14, "2015" : df15, "2016" : df16,
        "2017" : df17
        }

# multithreading starts here #

##def worker():
##    global yrdf, lock
##    while True:
##        [Year, Type, URL, count, total] = q.get()
##        if Year == None:
##            break
##        info = getinfo(URL)
##        info["Year"] = Year
##        info["Type"] = Type
##        with lock:
##            yrdf[Year] = yrdf[Year].append(info,1)
##            print("{}{} {}/{} ".format(Year,Type,count,total))
##            q.task_done()
##            
##q = Queue()
##threads = []
##ThreadCount = 10
##
##for i in range(ThreadCount):
##    t = threading.Thread(target=worker)
##    t.daemon = True
##    t.start()
##    threads.append(t)
##
##for Year in years:
##    for Type in types:
##        linklist = pd.read_json("{}_{}.json".format(Year,Type))[0]
##        count = 0
##        for URL in linklist:
##            total = len(linklist)
##            count += 1
##            done = False
##            q.put([Year, Type, URL, count, total])
##    q.join()
##    yrdf[Year].to_csv("{}_counts.csv".format(Year))
##
##for i in range(ThreadCount):
##    q.put([None,None,None,None,None])
##for t in threads:
##    t.join()
    
# multithreading ends here #

def checkoutput(Year):
    i = str(Year)
    df = pd.read_csv("{}_counts.csv".format(i), index_col=0)
    dfk = len(df [df["Type"] == "K" ])
    dfka = len(df [df["Type"] == "KA" ])
    dfnt = len(df [df["Type"] == "NT" ])
    dftotal = len(df.index)

    try:
        k  = len( pd.read_json("{}_K.json".format(i)).index )
        ka = len( pd.read_json("{}_KA.json".format(i)).index )
        nt = len( pd.read_json("{}_NT.json".format(i)).index )
        total = k + ka + nt
    except:
        k = ka = nt = total = 0

    kdiff = k - dfk
    kadiff = ka - dfka
    ntdiff = nt - dfnt
    totaldiff = total - dftotal
    diff = [kdiff, kadiff, ntdiff, totaldiff]
    return diff

def checkoutputs(first_year, last_year):
    years = [str(i) for i in range(first_year, last_year +1)]
    results = pd.DataFrame(years, columns = ["Year"])
    results["Difference"] = results["Year"].apply(lambda i: checkoutput(i))
    results.set_index(["Year"],inplace = True)
    return results

def combineoutputs(first_year, last_year):
    years = [str(i) for i in range(first_year, last_year +1)]
    outdf = pd.DataFrame()
    totallength = 0
    for year in years:
        yeardf = pd.read_csv("{}_counts.csv".format(year))
        outdf = outdf.append(yeardf)
        totallength += len(yeardf)
    print("Expected: {}, Actual: {} ".format(totallength, len(outdf)))
    outdf.reset_index(drop=True).drop(columns=["Unnamed: 0"]).to_csv("final_output.csv")
    return None

combineoutputs(2005,2017)
print("Entire job took:",time.time()-start)
