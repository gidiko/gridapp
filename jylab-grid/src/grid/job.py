"""
Grid Job API verion XX

Provides the standar class for Job. You can create a new job,
submit it to grid,cancel it, see the status of it,etc

Typical use:

    # submitting a job
    j = Job()
    j.vo = <vo>
    j.inputsandbox = ['/path/to/file1','/path/to/file2','/path/to/file3']
    j.outputsandbox = ['outfile1'.'outfile2']
    j.arguments = "file1 10"
    j.submit()
    j.status()
    j.getOupout()
    j.readFile("std.out")

"""

# java imports
from java.lang import Runtime
from java.io import BufferedReader,InputStreamReader,DataOutputStream
from sysprocess import ProcExecutor
from ssh import RemoteExecutor,FileTransfer
from java.text import SimpleDateFormat
from java.util import Locale

# jyhton imports
import os
import re
from shutil import copy
import zipfile
from util import raw_input
# other imports


class GridLauncher:
    def __init__(self, myvo, inpData = None):

        self.inpData = inpData
        self.templateEnv = '''#!/bin/sh

echo "[Jylab Grid Runtime Environment Preparing]" > time.log
date1=`date`
echo $date1 >> time.log
virtorg=%(thevo)s
mkdir gre
touch gridification.log

#download and untar jre
lcg-cp --vo $virtorg -t 100 -v  lfn:/grid/$virtorg/jylab/jre.tar.gz file:`pwd`/jre.tar.gz >> gridification.log
tar xzvf jre.tar.gz -C gre >> gridification.log

#download jylab packages and untar them
lcg-cp --vo $virtorg -t 100 -v  lfn:/grid/$virtorg/jylab/packages3.tar.gz file:`pwd`/packages.tar.gz >> gridification.log
tar xzvf packages.tar.gz -C gre >> gridification.log
'''

        self.templateExec = '''
ls -la >> gridification.log

# create classpath
classpath=.
for i in *.jar
do
    classpath=$classpath:$i
done

for i in gre/packages/*
do
    classpath=$classpath:$i
done

# create pythonpath
pythonpath=.
for i in gre/packages/*
do
    pythonpath=$pythonpath:$i/Lib
done

# find exact version of jre
javare=`find gre -name jre-*`
javare=`basename $javare`

date2=`date`
echo "[Executing Scipt]" >> time.log
echo $date2 >> time.log

# execute the script
./gre/$javare/bin/java -cp $classpath -Dpython.path=$pythonpath -Dpython.cachedir.skip=false org.python.util.jython $*

date3=`date`
echo "[Scipt Finished]" >> time.log
echo $date3 >> time.log

echo "[Script Execution Duration]" >> time.log
expr `date -d "$date3" +%s` - `date -d "$date2" +%s` >> time.log

echo "[Job Execution Duration]" >> time.log
expr `date -d "$date3" +%s` - `date -d "$date1" +%s` >> time.log

'''

        self.launcherstr = self.templateEnv % {'thevo' : myvo}

    def getString(self):
        self.launcherstr += self.getTemplateData() + self.templateExec
        return self.launcherstr

    def getTemplateData(self):
        self.templateData = ''

        if self.inpData != None:
            for i in self.inpData:
                inpFilename = os.path.basename(i)
                self.templateData += '''
lcg-cp --vo $virtorg -t 100 -v ''' + i + ''' file:`pwd`/''' + inpFilename + ''' >> gridification.log
'''
        return self.templateData



class Job:
    __vo = ''
    __executor = None
    __filer = None

    """ Job Class """
    def __init__(self):
        """ Initialise a new job  """

        if self.__executor is None:
            self.__executor = ProcExecutor()

        self.__id = None
        self.__actualoutdir = None
        self.__status = 'New'
        self.__stdoutfile = 'std.out'
        self.__stderrfile = 'std.err'
        self._Job__actualce = ''
        self.ce = ''
        self.vo = self.__vo
        self.name = ''
        self.executable = ''
        self.arguments = ''
        self.inputsandbox = []
        self.outputsandbox = []
        self.outputdir = ''
        self.inputdata = []
        self._Job__timesubmitted = ''
        self._Job__timestarted = ''
        self._Job__timeended = ''
        self._Job__executiontime = ''
        self._Job__totaltime = ''

    def __repr__(self):
        """ Formatted output of a job.Could be used to create a job """
        return 'Job (  name = %s, vo = %s,\n'\
                '      executable = %s,\n'\
                '      arguments = %s,\n'\
	            '      inputSandbox = %s,\n'\
                '      outputSandbox = %s\n\n'\
                '      id = %s,\n'\
                '      status = %s,\n '\
                '      ce = %s )'% \
                (self.name, self.vo,self.executable,
                self.arguments,self.inputsandbox,self.outputsandbox,
                self.__id, self.__status, self.__actualce)

    def submit(self):
        """ submit a new job"""
        def uisubmit(self):
            from abstract import Jobs
            self.checkjob()     # check job's attributes
            command = self.prepare_command(".autogenerated_jdl")    # create the command string
            ex = self.__executor
            jdl = self.create_description_string()  # create the jdl string
            f = open(".autogenerated.jdl", 'w')
            f.write(jdl)
            f.close()
            print "Submitting job ..."
            ex.exec(command)
            infostring = ex.input()
            ex.close()
            os.remove('.autogenerated.jdl')
            pattern = re.compile('(https://.*$)', re.MULTILINE)
            match = pattern.search(infostring)
            self.__id = match.group()
            if self.name == '':
                self.name = "Job-"+os.path.basename(self.__id)
            jbs = Jobs()
            jbs.addJob(self)

        def sshsubmit(self):
            from abstract import Jobs
            #TODO:Check job before submitting and messages after for err. or warn.
            ex = self.__executor
            fl = self.__filer
            self.checkjob()     # check job's attributes
            # create the command string
            command = self.prepare_command("~/.tempjob/.autogenerated.jdl")

            jdlpath = os.path.gethome() + os.path.sep + ".autogenerated.jdl"
            self.create_zipped_job()
            ex.exec("mkdir -p ~/.tempjob")
            ex.close()
            fl.upload(os.path.gethome() + os.path.sep + ".job.zip",ex.getEnv("HOME")+"/.tempjob")
            ex.exec("unzip -u ~/.tempjob/.job.zip -d ~/.tempjob")
            inp = ex.input()
            ex.close()

            jdl = self.create_description_string()  # create the jdl string
            f = open(jdlpath, 'w')
            f.write(jdl)
            f.close()
            fl.upload(jdlpath, ex.getEnv("HOME")+"/.tempjob")
            print "Submitting job ..."
            ex.exec(command)
            infostring = ex.input()
            #print infostring
            ex.close()
            os.remove(jdlpath)
            ex.exec("rm -R .tempjob")
            ex.input()
            ex.close()
            pattern = re.compile('(https://.*$)', re.MULTILINE)
            match = pattern.search(infostring)
            self.__id = match.group()
            if self.name == '':
                self.name = "Job-"+os.path.basename(self.__id)
            jbs = Jobs()
            jbs.addJob(self)
            zf = zipfile.ZipFile(os.path.gethome() + os.path.sep + ".job.zip")
            zf.close()
            os.remove(os.path.gethome() + os.path.sep + ".job.zip")

        if isinstance(self.__executor,RemoteExecutor) == 1:
            sshsubmit(self)
        else:
            uisubmit(self)

        print "Job " + self.name + " submitted"


    def create_zipped_job(self):

        # open the zip file for writing, and write stuff to it
        zf = zipfile.ZipFile(os.path.gethome() + os.path.sep + ".job.zip", "w")
        ex = self.__executor
        for name,i in zip(self.inputsandbox,range(len(self.inputsandbox))):
            if name.startswith("lfn") != 1:
                zf.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)
                self.inputsandbox[i] = ex.getEnv("HOME")+"/.tempjob/" + os.path.basename(name)
        zf.close()

        # open the file again, to see what's in it
        #zf = zipfile.ZipFile(os.path.gethome() + os.path.sep + ".job.zip", "r")
        #for info in zf.infolist():
        #    print info.filename, info.date_time, info.file_size, info.compress_size
        #zf.close()

    def prepare_command(self, jdlpath):
        """ Prepare the command to be executed"""
        cmd = []
        cmd.append('edg-job-submit')
        if self.ce != '':
            cmd += ['-r',self.ce]
        cmd.append(jdlpath)
        return " ".join(cmd)


    def cancel(self):
        """ Cancel a job """
        input = raw_input('Are you sure you want to cancel the job with id: '\
                + self.__id + ' [n]:  ')
        if (len(input) == 1) & (input[0] == 'y'):
            ex = self.__executor
            ex.exec(['edg-job-cancel' , self.__id])
            ex.writeBytes(input[0]+"\n")
            print "Job " + self.name + " is being cancelled... Check the status of the job."

    def status(self):
        """ get job status """
        ex = self.__executor
        ex.exec(['edg-job-status','-v','2',self.__id," | egrep 'Current Status: | Submitted[[:space:]]*: |  Running[[:space:]]*: | Done[[:space:]]*'"])
        inp = ex.input()
        ex.close()
        match = re.search('Current Status\W+(.*)',inp,re.MULTILINE)

        jobst = match.group(1)
        self.__status = jobst
        if jobst.startswith("Done (Success)") or jobst.startswith("Cleared"):
            match = re.search('Submitted\s*:\s(.*)',inp,re.MULTILINE)
            self._Job__timesubmitted = match.group(1)
            match = re.search('Running\s*:\s(.*)',inp,re.MULTILINE)
            self._Job__timestarted = match.group(1)
            match = re.search('Done\s*:\s(.*)',inp,re.MULTILINE)
            self._Job__timeended = match.group(1)

            #sdf = SimpleDateFormat("EEE MMM dd HH:mm:ss yyyy", Locale.US)
            #self._Job__executiontime = sdf.parse(self._Job__timeended).getTime() - sdf.parse(self._Job__timestarted).getTime()
            #self._Job__totaltime = sdf.parse(self._Job__timeended).getTime() - sdf.parse(self._Job__timesubmitted).getTime()

    	return jobst

    def getOutput(self):
        """ Retrieve the output files at UI """
        if self.__id == '':
            print "Job not submitted yet"
            return
        stat = self.status()
        ex = self.__executor
        ex.exec(['edg-job-get-output', self.__id])
        input = ex.input()
        match = re.search('(NS_JOB_OUTPUT_\w+)|(\/tmp\/[\w\/-]+)',input,re.MULTILINE)
        ex.close()
        if match.group() == 'NS_JOB_OUTPUT_NOT_READY':
            print "Job hasn't finished yet.\nStatus:",
            return self.status()
        elif match.group() == 'NS_JOB_OUTPUT_RETRIEVED':
            print "Job's output already retrieved"
            return
        else:
            self.__actualoutdir = match.group()
            if isinstance(self.__executor,RemoteExecutor):
                if os.path.isdir(self.outputdir) == 0:
                    self.outputdir = os.path.gethome() + os.sep + os.path.basename(self.__id)
                    os.mkdir(self.outputdir)
                ex = self.__executor
                ex.exec("zip -jD ~/output.zip "+self.__actualoutdir+"/*")
                #print ex.input()
                ex.close()
                self.__filer.download("~/output.zip",self.outputdir)

                zf = zipfile.ZipFile(self.outputdir + os.sep + "output.zip", "r")
                for i,name in zip(range(len(zf.namelist())),zf.namelist()):
                    outfile = open(os.path.join(self.outputdir, name), 'wb')
                    outfile.write(zf.read(name))
                    outfile.flush()
                    outfile.close()
                zf.close()
                os.remove(os.path.join(self.outputdir,"output.zip"))
            else:
                if os.path.isdir(self.outputdir):
                    for i in self.outputsandbox:
                        copy(self.__actualoutdir+os.sep+i,self.outputdir+os.sep+i)
                else:
                    self.outputdir = self.__actualoutdir
            return "Output retrieved"


    def readFile(self, file):
        """ Read a file from output directory"""
        stdout = self.outputdir + os.sep + file
        if os.path.isfile(stdout):
            f = open(stdout,'r')
            out = f.read()
            f.close()
            return out
        else:
            print "ERROR: File " + stdout +" does not exist"

    def create_description_string(self):
        """ Create and save the jdl file  """
        if self.executable == '':
            ex = self.__executor
            fl = self.__filer
            greLauncher = GridLauncher(self.vo, self.inputdata)
            launcherpath = os.path.gethome() + os.path.sep + "jylab-gre.sh"
            if os.path.exists(launcherpath):
                os.remove(launcherpath)
            f = open(launcherpath, 'w')
            f.write(greLauncher.getString())
            f.close()
            if isinstance(ex,RemoteExecutor) == 1:
                fl.upload(launcherpath,ex.getEnv("HOME")+"/.tempjob/")

            ex.exec(["dos2unix","~/.tempjob/jylab-gre.sh"])
            ex.input()
            ex.close()
            ex.exec(["chmod","755","~/.tempjob/jylab-gre.sh"])
            ex.input()
            ex.close()

            self.executable = "jylab-gre.sh"
            self.inputsandbox.insert(0, ex.getEnv("HOME") + "/.tempjob/" + self.executable)
            self.outputsandbox.append("gridification.log")
            self.outputsandbox.append("time.log")

        self.outputsandbox = self.outputsandbox + [self.__stdoutfile,self.__stderrfile]

        template = '''VirtualOrganisation="%(vo)s";
Executable="%(executable)s";
Arguments="%(arguments)s";
InputSandbox={%(inputSandbox)s};
InputData={%(inputData)s};
DataAccessProtocol={"file","gridftp"};
OutputSandbox={%(outputSandbox)s};
StdOutput="%(stdOutput)s";
StdError="%(stdError)s";'''

        result = template \
                % {'executable' : self.executable,
                'arguments': self.arguments,
                'inputSandbox' : ','.join(['"'+f+'"' for f in self.inputsandbox]),
                'inputData' : ','.join(['"'+f+'"' for f in self.inputdata]),
                'outputSandbox' : ','.join(['"'+f+'"' for f in self.outputsandbox]),
                'stdOutput' : self.__stdoutfile,
                'stdError' : self.__stderrfile,
                'vo' : self.vo}

    	return result

    def checkjob(self):
        """ a primary check of the job's attributes """
        pass

    def executionTime(self):

        if self._Job__executiontime == '':
            stat = self.status()
            if stat.startswith("Done (Success)"):
                output = self.getOutput()
                inp = self.readFile("time.log")
            elif stat.startswith("Cleared"):
                inp = self.readFile("time.log")
            else:
                return "Job not done yet"

            match = re.search("\[Script Execution Duration\].(.*).\[",inp,re.MULTILINE | re.DOTALL)
            self._Job__executiontime = match.group(1)
            match = re.search("\[Job Execution Duration\].(.*).",inp,re.MULTILINE | re.DOTALL)
            self._Job__totaltime = match.group(1)

        return self._Job__executiontime

    def totalTime(self):

        if self._Job__totaltime == '':
            stat = self.status()
            if stat.startswith("Done (Success)"):
                output = self.getOutput()
                inp = self.readFile("time.log")
            elif stat.startswith("Cleared"):
                inp = self.readFile("time.log")
            else:
                return "Job not done yet"

            match = re.search("\[Script Execution Duration\].(.*).\[",inp,re.MULTILINE | re.DOTALL)
            self._Job__executiontime = match.group(1)
            match = re.search("\[Job Execution Duration\].(.*).",inp,re.MULTILINE | re.DOTALL)
            self._Job__totaltime = match.group(1)

        return self._Job__totaltime

    def rbTimeStatistics(self):
        print "Time Submitted\t:\t" + str(self._Job__timesubmitted)
        print "Time Running\t:\t" + str(self._Job__timestarted)
        print "Time Ended\t:\t" + str(self._Job__timeended)
