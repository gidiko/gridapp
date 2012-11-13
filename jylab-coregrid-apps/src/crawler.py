# (setq py-indent-offset 2)

import urllib
import re
import os

def crawl(site, depth, linksfile):
  pattern = re.compile(r'href="(http://.*?)"')
  f = open(linksfile, 'a+')
  try: 
    if depth < MAX_DEPTH:
      print 'crawling [%s]...' % site, 
      print >> f, '[%s]' % site
      url = urllib.urlopen(site)
      content = url.read()
      hits = pattern.findall(content)
      for hit in hits:
        print >> f, hit
      print 'done.'
      print >> f, ''
      for hit in hits:
        crawl(hit, depth + 1, linksfile)
  except:
    pass
  f.close()

MAX_DEPTH=3
base = r'http://www.ceid.upatras.gr'
linksfile = r'links.txt'

if os.path.isfile(linksfile):
  os.remove(linksfile)
crawl(base, 0, linksfile)



