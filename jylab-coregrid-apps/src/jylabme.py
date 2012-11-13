#!/usr/bin/python

import os
import glob

class Package:
    def __init__(self):
        pass

    def wget(self):
        pass

    def adapt(self):
        pass

    def upload(self):
        pass

    def download(self):
        pass

    def install(self):
        pass


class NutchPackage(Package):
    def __init__(self):
        name = 'nutch'
        version = '0.7.2'

        dirs = {}
        dirs['jylab'] = os.getcwd()
        dirs['package'] = os.path.join(dirs['jylab'], name)
        dirs['web'] = os.path.join(dirs['jylab'], 'web')
        dirs['grid'] = dirs['jylab']
        dirs['extract'] = os.path.join(dirs['jylab'], '%s-%s' % (name, version))
               
        weburls = ['http://apache.forthnet.gr/lucene/nutch/nutch-0.7.2.tar.gz']
        gridlfns = ['lfn:/grid/see/jylab/nutch-0.7.2.jar']

        webfiles = []
        for url in weburls:
            fname = os.path.split(url)[-1]
            webfiles.append(fname)

        files = []
        for lfn in gridlfns:
            fname = os.path.split(lfn)[-1]
            files.append(fname)
            
        self.name = name
        self.version = version
        self.dirs = dirs
        self.weburls = weburls
        self.gridlfns = gridlfns
        self.webfiles = webfiles
        self.files = files

        self.classpaths = [] 

        
    def wget(self):
        dirname = self.dirs['web'] 
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        os.chdir(dirname)
        for url in self.weburls:
            cmd = 'wget %s' % url
            os.system(cmd)
        os.chdir(self.dirs['jylab'])

        
    def adapt(self):
        # extract web files
        os.chdir(self.dirs['web'])
        for fname in self.webfiles:
            cmd = 'tar -xzf %s -C %s' % (fname, self.dirs['jylab'])
            os.system(cmd)
            
        # make a jar only with necessary components
        os.chdir(self.dirs['extract'])
        cmd = 'jar -xf %s-%s.jar' % (self.name, self.version)
        os.system(cmd)
        os.remove('%s-%s.jar' % (self.name, self.version))       
        cmd = 'jar -cf %s-%s.jar lib org plugins -C conf .' % (self.name, self.version)
        os.system(cmd)
        cmd = 'cp %s-%s.jar %s' % (self.name, self.version, self.dirs['jylab'])
        os.system(cmd)
        
        # clean the rest
        os.chdir(self.dirs['jylab'])
        cmd = 'rm -rf %s' % self.dirs['extract']
        os.system(cmd)
        

    def upload(self):
        import gridme
        for lfn in self.gridarchives:
            base = os.path.split(lfn)[-1]
            fname = os.path.join(os.getcwd(), base)
            fnames.append(fname)
            gridme.upload(fname, lfn)       
            

    def download(self):
        import gridme
        # download
        num = len(self.gridlfns)
        if not os.path.exists(self.dirs['grid']):
            os.mkdir(self.dirs['grid'])
        for i in range(num):
            lfn = self.gridlfns[i]
            fname = os.path.join(self.dirs['grid'], self.files[i])
            gridme.download(lfn, fname)
            

    def install(self):
        dirname = self.dirs['package']
        if not os.path.exists(dirname):
            os.mkdir(dirname)

        os.chdir(self.dirs['grid'])
        for f in self.files:
            cmd = 'cp %s %s' % (f, dirname)
            os.system(cmd)

        os.chdir(dirname)
        for f in self.files:
            cmd = 'jar -xf %s' % f
            os.system(cmd)
            
        # create classpaths
        self.classpaths.append(self.name)
          
        os.chdir(os.path.join(dirname, 'lib'))
        jars = glob.glob('*.jar')
        for jar in jars:
            jarpath = os.path.join('nutch/lib', jar)
            self.classpaths.append(jarpath)
            
        # dive out to top level
        os.chdir(self.dirs['jylab'])



class JythonPackage(Package):
    def __init__(self):
        name = 'jython'
        version = '2.1'

        dirs = {}
        dirs['jylab'] = os.getcwd()
        dirs['package'] = os.path.join(dirs['jylab'], name)
        dirs['web'] = os.path.join(dirs['jylab'], 'web')
        dirs['grid'] = dirs['jylab']
        dirs['extract'] = os.path.join(dirs['jylab'], '%s-%s' % (name, version))
        
        weburls = ['http://heanet.dl.sourceforge.net/sourceforge/jython/jython_21.class']
        gridlfns = ['lfn:/grid/see/jylab/jython-2.1.jar']

        webfiles = []
        for url in weburls:
            fname = os.path.split(url)[-1]
            webfiles.append(fname)

        files = []
        for lfn in gridlfns:
            fname = os.path.split(lfn)[-1]
            files.append(fname)
        
        self.name = name
        self.version = version
        self.dirs = dirs
        self.weburls = weburls
        self.gridlfns = gridlfns
        self.webfiles = webfiles
        self.files = files

        self.classpaths = [] 

        
        
    def wget(self):
        dirname = self.dirs['web'] 
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        os.chdir(dirname)
        for url in self.weburls:
            cmd = 'wget %s' % url
            os.system(cmd)
        os.chdir(self.dirs['jylab'])


    def adapt(self):
        # extract web files
        os.chdir(self.dirs['web'])
        for fname in self.webfiles:
            classname = os.path.split(fname)[-1]
            classname = classname.split('.')[0]
            cmd = 'java %s -o %s' % (classname, self.dirs['extract'])
            os.system(cmd)
         
        # make a jar only with necessary components
        os.chdir(self.dirs['extract'])
        cmd = 'jar -xf %s.jar' % self.name
        os.system(cmd)
        os.remove('%s.jar' % self.name)       
        cmd = 'jar -cf %s-%s.jar com org jxxload_help Lib registry' % (self.name, self.version)
        os.system(cmd)
        cmd = 'cp %s-%s.jar %s' % (self.name, self.version, self.dirs['jylab'])
        os.system(cmd)

        # clean the rest
        os.chdir(self.dirs['jylab'])
        cmd = 'rm -rf %s' % self.dirs['extract']
        os.system(cmd)


    def upload(self):
        import gridme
        for lfn in self.gridarchives:
            base = os.path.split(lfn)[-1]
            fname = os.path.join(os.getcwd(), base)
            fnames.append(fname)
            gridme.upload(fname, lfn)
        

    def download(self):
        import gridme
        # download
        num = len(self.gridlfns)
        if not os.path.exists(self.dirs['grid']):
            os.mkdir(self.dirs['grid'])
        for i in range(num):
            lfn = self.gridlfns[i]
            fname = os.path.join(self.dirs['grid'], self.files[i])
            gridme.download(lfn, fname)
           

    def install(self):
        dirname = self.dirs['package']
        if not os.path.exists(dirname):
            os.mkdir(dirname)

        os.chdir(self.dirs['grid'])
        for f in self.files:
            cmd = 'cp %s %s' % (f, dirname)
            os.system(cmd)

        os.chdir(dirname)
        for f in self.files:
            cmd = 'jar -xf %s' % f
            os.system(cmd)        

        # create classpaths
        self.classpaths.append(self.name)
            
        # dive out to top level
        os.chdir(self.dirs['jylab'])

     
        

packages = {'nutch': NutchPackage(),
            'jython': JythonPackage()
            }

def clean():
    keys = packages.keys()
    for key in keys:
        pkg = packages[key]
        dirname = pkg.dirs['package']
        if os.path.exists(dirname):
            cmd = 'rm -rf %s' % dirname
            os.system(cmd)
        

def execute(scriptfile):
    classpaths = []
    keys = packages.keys()
    for key in keys:
        pkg = packages[key]
        pkg.download()
        pkg.install()
        classpaths.extend(pkg.classpaths)
    classpath = os.pathsep.join(classpaths)
    pythonhome = packages['jython'].name
    cmd = 'java -cp %s -Dpython.home=%s org.python.util.jython %s' % (classpath, pythonhome, scriptfile)
    os.system(cmd)



if __name__ == '__main__':
    execute('nutching.py')
    

