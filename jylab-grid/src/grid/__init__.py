from util import *
from ssh import RemoteHost, RemoteExecutor
from sysprocess import ProcExecutor

from job import Job
from abstract import Grid,Jobs,is_ui
from test import *


# functions for initialize, login, logout to grid
def login(vo, addr = None, keypath = " "):
    """ Connect to Grid and create a new proxy."""
    if is_ui() == 1:
        gr = Grid()
    else:
        remhost = RemoteHost(addr, keypath)
        gr = Grid(rhost = remhost)

    creategridtemp(gr.getExecutor())
    gr.setVo(vo)
    gr.newProxy()
    gr.getData().chdir("/grid/"+vo)
    Jobs(gr.getExecutor())

def logout():
    """ Logout from Grid """
    gr = Grid()
    gr.terminate()

def info(attr):
    """ Information about the certificate and the proxy"""
    gr = Grid()
    if attr == "cert":
        gr.getInfo().certificate()
    elif attr == "proxy":
        gr.getProxy().info()

def ses(index = None):
    """ Print SEs or return the name of the SE on index"""
    gr = Grid()
    if index is None:
        gr.getInfo().getSEs()
    else:
        if gr.getInfo().SEs == []:
            gr.getInfo().collectSEInfo()
        return gr.getInfo().SEs[index-1].SE

def ces(index = None):
    """ Print CEs or return the name of the CE on index"""
    gr = Grid()
    if index is None:
        gr.getInfo().getCEs()
    else:
        if gr.getInfo().CEs == []:
            gr.getInfo().collectCEInfo();
        return gr.getInfo().CEs[index-1].CE

# functions for managing jobs
def jobs(index = None):
    """ See the status of the jobs or return the job on index """
    jbs = Jobs()
    if index is None:
        jbs.getStatuses()
        print jbs
    else:
        return jbs.getJob(index)


# functions for managing files
def upload(localfile, gridfile, se):
    """ Upload a file to a storage element """
    gr = Grid()
    return gr.getData().upload(localfile, gridfile, gr.getVo(), se)

def download(gridfile, localfile):
    """ Doenload a file from a storage element """
    gr = Grid()
    return gr.getData().download(gridfile, localfile, gr.getVo())

def replicate(gridfile, se):
    """ Replicate (Copy) a file to the storage element """
    gr = Grid()
    return gr.getData().replicate(gridfile, se, gr.getVo())

def remove(gridfile, se = None):
    """ Remove a file from a storage element """
    gr = Grid()
    return gr.getData().remove(gridfile, gr.getVo(), se)

def lsdir(_dir = None):
    """ List files of a directory """
    gr = Grid()
    return gr.getData().lsdir(_dir)

def mkdir(_dir):
    """ Create a new directory """
    gr = Grid()
    return gr.getData().mkdir(_dir)

def chdir(_dir):
    """ Change default lfn directory """
    gr = Grid()
    return gr.getData().chdir(_dir)

def rmdir(_dir):
    """ Remove a directory """
    gr = Grid()
    return gr.getData().rmdir(_dir)

#def monitor(jobindeces, int = 10000):
#    from java.lang import Thread
#    if type(jobindeces) != list or jobindeces == []:
#        print "Input Error. Must be a non-Empty list!"
#        return
#
#    jobsPending = jobindeces[:]
#
#    jobs = Jobs()
#    def checkStatus():
#        print "Checking Status"
#        for i in jobsPending:
#            jb = jobs.getJob(i)
#            stat = jb.status()
#            if stat.startswith("Cancelled") or stat.startswith("Aborted"):
#                jobsPending.remove(i)
#                print "Job " + str(i)+ " interrupted"
#
#            if stat.startswith("Done"):
#                jobsPending.remove(i)
#                print "Job " + str(i)+ " ended"
#
#    checkStatus()
#    while jobsPending != []:
#        Thread.sleep(int)
#        checkStatus()









