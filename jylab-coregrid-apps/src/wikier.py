from org.apache.xmlrpc import *
from java.util import *
import os
import urllib

def ostype():
    '''Prints a string identifying host operating system.
    '''
    pathsep = os.pathsep
    if pathsep  == ':':
        return 'linux'
    else:
        return 'windows'


def rmtree(rootdir, ask=1):
    if ostype() == 'linux':
        if ask == 1:
            ans = raw_input('rmtree? [y/n]: ')
            if ans == 'n':
                return 0
        cmd = r'rm -rf %s' % (rootdir,)
    else:
        pass
    retval = os.system(cmd)    
    return retval
    
def copytree(srcdir, destdir):
    '''Copies directory tree rooted at srcdir under (existing) destdir or to (non existing) destdir.
    '''
    if ostype() == 'linux':
        cmd = r'cp -r %s %s' % (srcdir, destdir)
    else:
        pass
    retval = os.system(cmd)
    return retval
    

def copy(src, dest):
    '''Copies src file to dest.
    '''
    if ostype() == 'linux':
        cmd = r'cp %s %s' % (src, dest)
    else:
        pass
    retval = os.system(cmd)
    return retval

def compress(srcdir, dest):
    if ostype() == 'linux':
        cmd = r'zip -r %s %s' % (dest, srcdir)
    else:
        pass
    retval = os.system(cmd)
    return retval


class WikiInstance:
    def __init__(self, installdir, interface='localhost', port=8000, name='wiki', sitename=u'Untitled Wiki'):
        self.installdir = installdir
        self.interface = interface
        self.port = port
        self.name = name
        self.sitename = sitename
        
    def create(self, rootdir='/usr/share/moin'):
        if not os.path.exists(self.installdir):
            os.mkdir(self.installdir)
        self.__copyFiles(rootdir)
        self.__patchFiles(rootdir)
        self.rootdir = rootdir

    def __copyFiles(self, rootdir):
        dirs = ['data', 'underlay']
        for d in dirs:
            srcdir = os.path.join(rootdir, d)
            copytree(srcdir, self.installdir)
        moind = apply(os.path.join, [rootdir, 'server', 'moin'])
        moin = apply(os.path.join, [rootdir, 'server', 'moin.py'])
        wikiconfig = apply(os.path.join, [rootdir, 'config', 'wikiconfig.py'])
        copy(moind, self.installdir)
        copy(moin, self.installdir)
        copy(wikiconfig, self.installdir)        
        self.moin= moin
        self.wikiconfig = wikiconfig
        
    def __patchFiles(self, rootdir):
        self.__patchMoin(rootdir)
        self.__patchWikiconfig()
        
    def __patchMoin(self, rootdir):
        fromto = {"#sys.path.insert(0, '/etc/moin')":"sys.path.insert(0, '%s')" % (self.installdir,),
                  "sys.path.insert(0, '/etc/moin')" : "#sys.path.insert(0, '/etc/moin')",
                  "name = 'moin'" : "name = '%s'" % (self.name,),
                  "docs = '/usr/share/moin/htdocs'" : "docs = '%s'" % (os.path.join(rootdir, 'htdocs'),),
                  "port = 8000" : "port = %d" % (self.port,),
                  "interface = 'localhost'" : "interface = '%s'" % (self.interface,)
                  }
        f = open(self.moin, 'r')
        s = f.read()
        f.close()
        for item in fromto.keys():
            s = s.replace(item, fromto[item])
        mymoin = os.path.join(self.installdir, 'moin.py')
        f = open(mymoin, 'w')
        f.write(s)
        f.close()

    def __patchWikiconfig(self):
        fromto = {"sitename = u'Untitled Wiki'" : "sitename = u'%s'" % (self.sitename,),
                  "data_dir = './data/'" : "data_dir = '%s'" % (os.path.join(self.installdir, 'data'),),
                  "data_underlay_dir = './underlay/'" : "data_underlay_dir = '%s'" % (os.path.join(self.installdir, 'underlay'),),
                  "# Security ----------------------------------------------------------" : "xmlrpc_putpage_enabled = True",
                  "# Mail --------------------------------------------------------------" : "xmlrpc_putpage_trusted_only = False"
                  }        
        f = open(self.wikiconfig, 'r')
        s = f.read()
        f.close()
        for item in fromto.keys():
            s = s.replace(item, fromto[item])        
        mywikiconfig = os.path.join(self.installdir, 'wikiconfig.py')
        f = open(mywikiconfig, 'w')
        f.write(s)
        f.close()        
    
    def start(self):
        daemon = apply(os.path.join, [self.installdir, 'moin'])
        cmd = '%s start' % (daemon,)
        retval = os.system(cmd)
        return retval

    def restart(self):
        daemon = apply(os.path.join, [self.installdir, 'moin'])
        cmd = '%s restart' % (daemon,)
        retval = os.system(cmd)
        return retval
    
    def stop(self):
        daemon = apply(os.path.join, [self.installdir, 'moin'])
        cmd = '%s stop' % (daemon,)
        retval = os.system(cmd)
        return retval
        
    def destroy(self):
        retval = rmtree(self.installdir, ask=0)
        return retval


    
    def backup(self, dest='data.zip'):
        srcdir = os.path.join(self.installdir, 'data')
        retval = compress(srcdir, dest)
        return retval


class WikiProxy:
    def __init__(self, hostname='localhost', port=8000):
        url = r'http://%s:%d/?action=xmlrpc2' % (hostname, port)
        proxy = XmlRpcClient(url)
        self.hostname = hostname
        self.port = port
        self.url = url
        self.proxy = proxy
        
    def getPage(self, pagename):
        args = Vector()
        args.add(pagename)
        s = self.proxy.execute('getPage', args)
        return s

    def getPageHTML(self, pagename):
        args = Vector()
        args.add(pagename)
        s = self.proxy.execute('getPageHTML', args)
        return s        

    def getAllPages(self):
        args = Vector()
        pages = self.proxy.execute('getAllPages', args)
        return pages

    def getPageInfo(self, pagename):
        args = Vector()
        args.add(pagename)
        info = self.proxy.execute('getPageInfo', args)
        return info        

    def listLinks(self, pagename):
        args = Vector()
        args.add(pagename)
        links = self.proxy.execute('listLinks', args)
        return links        

    def getBackLinks(self, pagename):
        args = Vector()
        args.add(pagename)
        links = self.proxy.execute('getBackLinks', args)
        return links

    def putPage(self, pagename, content):
        args = Vector()
        args.add(pagename)
        args.add(content)
        retval = self.proxy.execute('putPage', args)
        return retval



# testing    
def setup():
    installdir = '/home/giorgos/wiki'
    interface = 'localhost'
    hostname = 'localhost'
    port = 8000

    wiki = WikiInstance(installdir, interface, port)
    wiki.create()
    wiki.start()
    
    proxy = WikiProxy(hostname, port)
    pages = proxy.getAllPages()
    print pages

    proxy.putPage('HelloWorld',
                  "This is '''wiki markup''', see [http://moinmoin.wikiwikiweb.de/ MoinMoin wiki]")
    return wiki
    
def cleanup(wiki):
    wiki.stop()
    wiki.destroy()


# wiki = setup()
# cleanup(wiki)

    
    




