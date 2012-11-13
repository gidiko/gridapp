import re



def toSuccessors(fpath):
    f = open(fpath, 'r')
    s = f.read()
    f.close()
    successors = {}
    pat1 = re.compile(r'\[(http://.*)\]\s((?:http://.*\s)*)\s')
    matches =  pat1.findall(s)
    pat2 = re.compile(r'(http://.*)\s')
    for m in matches:
        parent = m[0]
        children = pat2.findall(m[1])
        successors[parent] = children
    return successors



def toListing(fpath):
    listing = []
    pool = {}
    dummy = 1
    succ = toSuccessors(fpath)
    keys = succ.keys()
    for key in keys:
        pool[key] = 1
        outlinks = succ[key]
        for link in outlinks:
            pool[link] = 1
    urls = pool.keys()
    urls.sort()
    return urls
    


links = r'/home/giorgos/coregrid/gridjob/links.txt'
successors = toSuccessors(links)
listing = toListing(links)
