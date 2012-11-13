import crawlme


crawlme.crawl(['http://www.uoa.gr', 'http://www.ceid.upatras.gr', 'http://www.ntua.gr'], 2)


import os
import gridme
jarfile = 'crawldata.jar'
cmd = 'jar -cMf %s %s' % (jarfile, 'crawldata')
os.system(cmd)
cwd = os.getcwd()
jarpath = os.path.join(cwd, jarfile)
gridme.upload(jarpath, 'lfn:/grid/see/jylab/%s' % jarfile)

