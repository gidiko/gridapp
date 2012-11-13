from org.apache.nutch.tools import *
import os


def __urlsToFile(urls, urlfile):
    f = open(urlfile, 'w')
    for url in urls:
        print >> f, url
    f.close()
    

def sitecrawl(urls, depth, crawldir='crawldata', configfile='crawl-tool.xml', filterfile='crawl-urlfilter.txt', urlfile='urls.txt'):
    for f in [configfile, filterfile]:
        cmd = 'cp %s nutch' % f
        os.system(cmd)
    __urlsToFile(urls, urlfile)
    CrawlTool.main([urlfile, '-dir', crawldir, '-depth', str(depth)])


def crawl(urls, depth, crawldir='crawldata', configfile='nutch-site.xml', urlfile='urls.txt'):
     cmd = 'cp %s nutch' % configfile
     os.system(cmd)
     __urlsToFile(urls, urlfile)
     CrawlTool.main([urlfile, '-dir', crawldir, '-depth', str(depth)])


                                   

