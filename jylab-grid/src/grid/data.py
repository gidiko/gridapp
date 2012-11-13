##########################################
# Jylab Grid Data Managemnent
#
##########################################
#
#
##########################################

from ssh import RemoteExecutor, FileTransfer, RemoteHost
from sysprocess import ProcExecutor
from util import *
import os


class Data:
    def __init__(self, ex = None, fs = None):

        self.__executor = ex
        if ex is None:
            self.__executor = ProcExecutor()
        self.__filer = fs

    def upload(self, localfile, gridfile, vo, se):
        x = self.__executor
        if isinstance(x, RemoteExecutor):
            uidir =  getuitemp(x.getEnv("HOME"))
            self.__filer.upload(localfile,uidir)
            uifile = uidir+os.path.basename(gridfile)
            x.exec(['mv',uidir+os.path.basename(localfile),uifile])
            x.input()
            x.close()
        else:
            uifile = localfile
        x.exec(['lcg-cr','--vo',vo,'-d',se,'-l','lfn:'+gridfile,'file:'+uifile])
        input = x.input()
        print input
        x.close()
        x.exec(['rm',uifile])
        x.input()
        x.close()

    def download(self, gridfile, localfile,vo):
        x = self.__executor
        if isinstance(x, RemoteExecutor):
            uifile =  getuitemp(x.getEnv("HOME")) + os.path.basename(gridfile)
        else:
            uifile = localfile
        x.exec(['lcg-cp','--vo',vo,'-v','lfn:'+gridfile,'file:'+uifile])
        input = x.input()
        print input
        x.close()
        if isinstance(self.__executor, RemoteExecutor):
            localdir = os.path.dirname(localfile)
            self.__filer.download(uifile,localdir)


    def replicate(self, gridfile, se, vo):
        x = self.__executor
        x.exec(['lcg-rep','-v','--vo',vo,'-d',se,gridfile])
        input = x.input()
        print input
        x.close()

    def remove(self, gridfile, vo, se = None):
        x = self.__executor
        command = ['lcg-del','--vo',vo]
        if se is not None:
            command = command + ['-s',se]
        command.append(gridfile)
        x.exec(command)
        input = x.input()
        print input
        x.close()

    def lfcrm(self, gridfile=""):
        x = self.__executor
        x.exec("lfc-rm " + gridfile)
        input = x.input()
        print input
        x.close()

    def lsdir(self, _dir = None):
        x = self.__executor
        if _dir is None:
            _dir = x.getEnv("LFC_HOME")
        x.exec('lfc-ls ' + _dir)
        input = x.input()
        #print input
        dirs = input.split('\n')
        dirs.pop()
        x.close()
        return dirs

    def rmdir(self,_dir = ""):
        x = self.__executor
        x.exec("lfc-rm -r " + _dir)
        input = x.input()
        print input
        x.close()

    def mkdir(self,_dir):
        x = self.__executor
        x.exec("lfc-mkdir " + _dir)
        input = x.input()
        print input
        x.close()

    def chdir(self, _dir):
        x = self.__executor
        x.exec("lfc-ls -d " + _dir)
        input = x.input()
        if input == _dir+'\n':
            x.setEnv("LFC_HOME",_dir)
        else:
            print "Directory " + _dir + " not found."
        x.close()

    def wget(self,webfile):
        x = self.__executor
        x.exec("wget " + webfile)
        input = x.input()
        print input
        x.close()

    def ui2se(self, uifile, lfn):
        x = self.__executor
        x.exec()

    def se2ui(self):
        pass

class Sandbox:
    def __init__(self, filelist):
        self.filelist = filelist

    def toString(self):
        num = len(self.filelist)
        if num == 0:
            return '{}'
        result = '{'
        for i in range(num-1):
            result = result + '"%s",' % (self.filelist[i],)
        result = result + '"%s"}' % (self.filelist[num-1],)
        return result


