#############################################################################
#                                                                           #
#                                                                           #
#############################################################################


# java imports
from java.lang import Runtime,System
from java.io import BufferedReader,InputStreamReader,DataOutputStream
from ch.ethz.ssh2 import StreamGobbler
from java.lang import *
from java.io import *


class CEInfo:
    def __init__(self, infostring):
        info = self._scrape(infostring)
        self.totalCPUs = info[0]
        self.freeCPUs = info[1]
        self.totalJobs = info[3]
        self.runningJobs = info[3]
        self.waitingJobs = info[4]
        self.CE = info[5]

    def _scrape(self, infostring):
        info = infostring.split('\t')
        return [int(info[0]),
                int(info[1]),
                int(info[2]),
                int(info[3]),
                int(info[4]),
                info[5]]

class SEInfo:
    def __init__(self, infostring):
        info = self._scrape(infostring)
        self.SE = info[0]

    def _scrape(self, infostring):
        info = infostring.split('\t')
        return [info[-1]]

class GridInfo:
    def __init__(self, myvo="see", ex = None):
        self.vo = myvo
        self.cert = None
        self.executor = ex
        self.CEs = []
        self.SEs = []

    def collectCEInfo(self):
        # computing elements
        ex = self.executor
        ex.exec(['lcg-infosites', '--vo', '%s'%self.vo, 'ce'])
        input = ex.input().splitlines()
        for infostring in input[3:len(input)]:
            ce = CEInfo(infostring)
            self.CEs.append(ce)
        ex.close()

    def collectSEInfo(self):
        # storage elements
        ex = self.executor
        ex.exec(" ".join(['lcg-infosites', '--vo', '%s'%self.vo, 'se']))
        inpt = ex.input()
        input = inpt.splitlines()
        for infostring in input[3:len(input)]:
            se = SEInfo(infostring)
            self.SEs.append(se)
        ex.close()

    def getSEs(self):
        self.SEs = []
        self.collectSEInfo()
        print "# ID\tStorage Element"
        print "-----------------------------------"
        for i in range(len(self.SEs)):
            print "# " + str(i+1) + "\t" + self.SEs[i].SE

    def getCEs(self):
        self.CEs = []
        self.collectCEInfo()
        print "# ID\t#CPUs\tFree Total Jobs\tRunning\tWaiting\tComputing Element"
        print "--------------------------------------------------\
--------------------------------------------------------"
        for i in range(len(self.CEs)):
            print "# "+str(i+1) + \
            "\t"+str(self.CEs[i].totalCPUs) + \
            "\t"+str(self.CEs[i].freeCPUs)+\
            "\t"+str(self.CEs[i].totalJobs)+\
            "\t"+str(self.CEs[i].runningJobs)+\
            "\t"+str(self.CEs[i].waitingJobs)+\
            "\t"+self.CEs[i].CE

    def certificate(self):
        ex = self.executor
        ex.exec(['grid-cert-info'])
        input = ex.input()
        print input
        ex.close()

