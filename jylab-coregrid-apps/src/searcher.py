from java.io import *

from org.apache.nutch.searcher import *


dirname = r'/home/giorgos/crawls/jycrawl'
dbdirname =  r'/home/giorgos/crawls/jycrawl/db'
term = r'ceid' 
nhits = 1000
htmlfile = r'/home/giorgos/search.html'

bean = NutchBean(File(dirname))
query = Query()
query.addRequiredTerm(term)
hits = bean.search(query, nhits)
ntotal = hits.getTotal()

num = nhits
if ntotal < nhits:
    num = ntotal

s = ''
for i in range(num):
    hit = hits.getHit(i)
    details = bean.getDetails(hit)
    s = s + details.toHtml()

f = open(htmlfile, 'w')
f.write(s)
f.close()


from org.apache.nutch.db import *
from org.apache.nutch.fs import *

nfs = NutchFileSystem.get()
fdir = File(dbdirname)
reader = WebDBReader(nfs, fdir) 
print reader.numLinks()
print reader.numPages()

pages = reader.pages()
for page in pages:
    # page.getNumOutlinks()
    print page.getURL().toString()
    
links = reader.links()
for link in links:
    pass
    # print link.toString()



from org.apache.nutch.parse import *
import urllib

f = urllib.urlopen(r'http://www.python.org', 'r')
s = f.read()
f.close()
outlinks = OutlinkExtractor.getOutlinks(s)


from org.apache.nutch.plugin import *
descriptors = PluginManifestParser.parsePluginFolder()
for d in descriptors:
    print d.getName()
    
