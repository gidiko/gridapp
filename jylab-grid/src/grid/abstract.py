
from proxy import *
from info import *
from data import *
from job import Job
import re
import copy

class Grid:
    """ Abstraction Class of Grid Concept
        Singleton Class emulation
    """
    __instance = None
    __shared_state = {}

    def __init__(self, rhost = None, vo = None ):
        """ Initializing a Grid Object. First check if jylab runs on a Grid
        UI or not """
        self.__dict__ = self.__shared_state
        if self.__instance is None:
            self.__instance = self
            self.__host = rhost
            self.__vo = vo
            self.__proxy = None
            self.__data = None
            if is_ui() == 1:
                self.__executor = ProcExecutor()
                self.__data = Data()
            else:
                if self.__host is None:
                    print "Error : No Remote Host Connection established"
                    return None
                self.__executor = RemoteExecutor(self.__host)
                self.__data = Data(self.__executor, FileTransfer(self.__host))
                Job._Job__filer = FileTransfer(self.__host)

            Job._Job__executor = self.__executor
            self.__info = GridInfo(vo,self.__executor)
        else:
            self = self.__instance

    def setVo(self,vo):
        self.__vo = vo
        self.__info.vo = vo
        Job._Job__vo  = vo

    def getVo(self):
        return self.__vo

    def setHost(self, remhost):
        self.__host = remhost

    def getInfo(self):
        return self.__info

    def getExecutor(self):
        return self.__executor

    def getData(self):
        return self.__data

    def newProxy(self):
        self.__proxy = Proxy(self.__executor)

    def getProxy(self):
        return self.__proxy

    def terminate(self):
        if self.__proxy is not None:
            self.__proxy.destroy()
        if self.__host is not None:
            self.__host.destroy()
        self.__instance = None


class Jobs:
    """ Singleton Class of the Jobs present on grid.
    It consists of a list of job objects
    """
    __instance = None
    __shared_state = {}

    def __init__( self, ex = None ):
        """ Initializing the Jobs Class """
        self.__dict__ = self.__shared_state
        if self.__instance is None:
            self.__instance = self
            self.__jobs = []
            if ex == None:
                ex = ProcExecutor()
            self.__executor = ex

        else:
            self = self.__instance

    def __repr__(self):
        output = []
        output.append(' Statistics: %d jobs' % len(self.__jobs))
        output.append('------------------------------------')
        output.append('# Index\tstatus\t\tname\t\t\t\texecutable')
        for i in range(len(self.__jobs)):
            j = self.__jobs[i]
            output.append('# %-5s %-15s %-31s %-30s'% (str(i+1),j._Job__status,j.name,j.executable))
        return "\n".join(output)

    def getJob(self, index):
        return self.__jobs[index-1]

    def addJob(self, job):
        self.__jobs.append(copy.copy(job))

    def removeJob(self, index):
        j = self.__jobs.pop(index-1)
        j = None

    def getStatuses(self):
        x = self.__executor
        jobsids = " ".join([job._Job__id for job in self.__jobs])
        x.exec(['edg-job-status','-v','2',jobsids," | egrep 'Current Status: | Submitted[[:space:]]*: |  Running[[:space:]]*: | Done[[:space:]]*'"])
        inp = x.input()
        x.close()
        # could be a regex iterator

        jobsst = re.findall('Current Status\W+(.*)',inp)
        jobssubmittime = re.findall('Submitted\s*:\s(.*)',inp)
        jobsstarttime = re.findall('Running\s*:\s(.*)',inp)
        jobsendtime = re.findall('Done\s*:\s(.*)',inp)

        for j,s in zip(self.__jobs, jobsst):
            j._Job__status = s  # violate private variable status.It's ok
            if s.startswith("Done (Success)"):
                print "Job Done"
                #j._Job__timesubmit = jsb
                #j._Job__timestart = jst
                #j._Job__timeended = jen





def is_ui():
    try:
        pr = ProcExecutor()
        pr.exec(['globus-hostname'])
        host = pr.input()
        return 1
    except IOException:
        return 0

