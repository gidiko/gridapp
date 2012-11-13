import os
import sys

commands = {}

class Command:
    def __init__(self):
        pass  

    def execute(self):
        pass
  
class SimpleCommand:
    def __init__(self, cmd):
        self.cmd = cmd

    def execute(self):
        return os.system(self.cmd)
        
class TemplatedCommand(SimpleCommand):
    def __init__(self, template, keyvalues):
        cmd = template % keyvalues
        self.template = template
        SimpleCommand.__init__(self, cmd)

# First the keys(pnames) are inserted into the string template and a new template (with names) is produced.
# During command execution these named keys are substituted with values from the dictionary of arguments
# passed to command itself.
# cmd is generated during runtime (not inheriting TemplatedCommand
# Note: you have to use the same names for keys and command arguments.
# Some ideas taken from http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/52315.
#

class EmbeddedTemplatedCommand:
    def __init__(self, template, keys):
        template = template % keys
        self.template = template
        
        
    def execute(self):
        keyvalues = sys._getframe(1).f_locals
        self.cmd = self.template % keyvalues
        return os.system(self.cmd)

commands['proxy'] = {'login' : SimpleCommand('grid-proxy-init'),
                     'logout' : SimpleCommand('grid-proxy-destroy')
                     }

def login():
    '''
    '''
    return commands['proxy']['login'].execute()


def logout():
    '''
    '''
    return commands['proxy']['logout'].execute()
    

commands['info'] = {'proxyInfo' : SimpleCommand('grid-proxy-info'),
                    'certificateInfo' : SimpleCommand('grid-cert-info'),
                    'sitesInfo': EmbeddedTemplatedCommand('lcg-infosites --vo %%(%s)s %%(%s)s', ('vo', 'about')),
                    'info': Command()
                    }

def __proxyInfo():
    '''
    '''
    return commands['info']['proxyInfo'].execute()


def __certificateInfo():
    '''
    '''
    return commands['info']['certificateInfo'].execute()

def __sitesInfo(about, vo='see'):
    '''
    '''
    return commands['info']['sitesInfo'].execute()

def info(about, vo='see'):
    if about == 'proxy':
        return __proxyInfo()
    elif about == 'certificate':
        return __certificateInfo()
    else:
        return __sitesInfo(about, vo)

    
class Job:
    def __init__(self):
        pass


############################################################
# trying to use names from standard python os module
# lfc- commands
# lfc- commands need LFC_HOST environment variable

commands['data'] = {'mkdir' : EmbeddedTemplatedCommand('lfc-mkdir %%(%s)s', ('dname')),
                    'rmdir' : EmbeddedTemplatedCommand('lfc-rm -r %%(%s)s', ('dname')),
                    'listdir' : EmbeddedTemplatedCommand('lfc-ls -l %%(%s)s', ('dname')),
                    'mklink': EmbeddedTemplatedCommand('lfc-ln -s %%(%s)s', ('lfname')),
                    'upload' : EmbeddedTemplatedCommand('lcg-cr --vo %%(%s)s -d %%(%s)s -l %%(%s)s file://%%(%s)s', ('vo', 'se', 'lfname', 'fname')),
                    'download' : EmbeddedTemplatedCommand('lcg-cp --vo %%(%s)s %%(%s)s file://%%(%s)s', ('vo', 'lfname', 'fname')),
                    'replicate' : EmbeddedTemplatedCommand('lcg-rep --vo %%(%s)s -d %%(%s)s %%(%s)s', ('vo', 'se', 'fname')),
                    'remove' : EmbeddedTemplatedCommand('lcg-del --vo %%(%s)s -s %%(%s)s %%(%s)s', ('vo', 'se', 'fname')),
                    'surls' : EmbeddedTemplatedCommand('lcg-lr --vo %%(%s)s %%(%s)s', ('vo', 'fname')),
                    'guid' : EmbeddedTemplatedCommand('lcg-lg --vo %%(%s)s %%(%s)s', ('vo', 'fname')),
                    'lfns' : EmbeddedTemplatedCommand('lcg-la --vo %%(%s)s %%(%s)s', ('vo', 'fname')),
                    'turl' : EmbeddedTemplatedCommand('lcg-gt --vo %%(%s)s %%(%s)s %%(%s)s', ('vo', 'surlname', 'protocol'))
                    }


def mkdir(dname):
    '''
    '''
    return commands['data']['mkdir'].execute()


def rmdir(dname):
    '''
    '''
    return commands['data']['rmdir'].execute()


def listdir(dname):
    '''
    '''
    return commands['data']['listdir'].execute()


def mklink(lfname):
    '''
    '''
    return commands['data']['mklink'].execute()


def upload(fname, lfname, vo='see', se='se01.isabella.grnet.gr'):
    '''
    '''
    return commands['data']['upload'].execute()


def download(lfname, fname, vo='see'):
    '''
    '''
    return commands['data']['download'].execute()


def replicate(fname, vo='see', se='se01.isabella.grnet.gr'):
    '''
    '''
    return commands['data']['replicate'].execute()


def remove(fname, vo='see', se='se01.isabella.grnet.gr'):
    '''
    '''
    return commands['data']['remove'].execute()


def surls(fname, vo='see'):
    '''
    '''
    return commands['data']['surls'].execute()


def guid(fname, vo='see'):
    '''
    '''
    return commands['data']['guid'].execute()


def lfns(fname, vo='see'):
    '''
    '''
    return commands['data']['lfns'].execute()


def turl(surlname, protocol):
    '''
    '''
    return commands['data']['turl'].execute()



# def funcname():
#     return sys._getframe(1).f_code.co_name


# def mkdir(dname, vo='see', ):
#     caller = funcname()
#     return commands['data'][caller].execute()






