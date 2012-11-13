import urllib

from javax.swing import *
from java.io import *
from java.awt import *
from java.lang import *

from edu.uci.ics.jung.graph.impl import *
from edu.uci.ics.jung.visualization import *
from edu.uci.ics.jung.algorithms import *
from edu.uci.ics.jung.algorithms.importance import *
from edu.uci.ics.jung.algorithms.transformation import *
from edu.uci.ics.jung.utils import *

from org.apache.nutch.searcher import *
from org.apache.nutch.db import *
from org.apache.nutch.fs import *
from org.apache.nutch.parse import *
from org.apache.nutch.plugin import *



def search(dbdirname, term, nhits=1000, htmlfile=None):
    bean = NutchBean(File(dirname))
    query = Query()
    query.addRequiredTerm(term)
    hits = bean.search(query, nhits)
    ntotal = hits.getTotal()
    
    num = nhits
    if ntotal < nhits:
        num = ntotal

    s = ''
    for i in range(num):
        hit = hits.getHit(i)
        details = bean.getDetails(hit)
        s = s + details.toHtml()

    if htmlfile is not None:
        f = open(htmlfile, 'w')
        f.write(s)
        f.close()

def statistics(dbdirname):
    nfs = NutchFileSystem.get()
    fdir = File(dbdirname)
    reader = WebDBReader(nfs, fdir) 
    #print reader.numLinks()
    #print reader.numPages()

    pages = reader.pages()
    for page in pages:
        outlinks = page.getNumOutlinks()
        url = page.getURL()
        score = page.getScore()
        nextscore = page.getNextScore()
        strurl = url.toString()
        inlinks = len(reader.getLinks(url))
        print '[%s:  outlinks=%d inlinks=%d score=%f nextscore=%f]' % (strurl, outlinks, inlinks, score, nextscore)
    
    links = reader.links()
    for link in links:
        pass
    # print link.toString()

def pages(dbdirname):
    nfs = NutchFileSystem.get()
    fdir = File(dbdirname)
    reader = WebDBReader(nfs, fdir) 
    pages = reader.pages()
    return pages

def links(dbdirname):
    nfs = NutchFileSystem.get()
    fdir = File(dbdirname)
    reader = WebDBReader(nfs, fdir) 
    links = reader.links()
    return links    


def outlinks(url=r'http://www.python.org'):
    f = urllib.urlopen(url, 'r')
    s = f.read()
    f.close()
    outlinks = OutlinkExtractor.getOutlinks(s)
    print outlinks
    
def plugins():
    descriptors = PluginManifestParser.parsePluginFolder()
    for d in descriptors:
        print d.getName()


def digraph(dbdirname):
    nfs = NutchFileSystem.get()
    fdir = File(dbdirname)
    reader = WebDBReader(nfs, fdir) 
    pages = reader.pagesByMD5()
    graph = DirectedSparseGraph()

    vertices = {}
    edges = {}

    for p in pages:
        url = p.getURL()
        vertex = DirectedSparseVertex()
        vertex.setUserDatum('url', url.toString(), UserData.SHARED)
        vertices[url] = vertex 
        links = reader.getLinks(p.getMD5())
        lurls = []
        for l in links:
            lurl = l.getURL()
            lurls.append(lurl)
            vertex = DirectedSparseVertex()
            vertex.setUserDatum('url', url.toString(), UserData.SHARED)
            vertices[lurl] = vertex
        edges[url] = lurls

    keys = vertices.keys()
    for key in keys:
        graph.addVertex(vertices[key])

    keys = edges.keys()
    for key in keys:
        urls = edges[key]
        for url in urls:
            edge = DirectedSparseEdge(vertices[key], vertices[url])
            graph.addEdge(edge)

    return graph
    
 

def pagerank(graph, alpha=0.85):
    bias = 1.0 - alpha
    ranker = PageRank(graph, bias)
    ranker.setRemoveRankScoresOnFinalize(0)
    ranker.evaluate()
    return ranker
    # top = ranker.getRankScores(topN)
    # return top

# use authority = 0 for hub ranking
def hits(graph, authority=1):
    ranker = HITS(graph, authority)
    ranker.setRemoveRankScoresOnFinalize(0)
    ranker.evaluate()
    return ranker



class LinkExplorer:
    def __init__(self, graph, rankers):
        # scrollable text
        text = JTextArea()
        scroll = JScrollPane(text)
        
        self.graph = graph
        self.rankers = rankers
        self.text = text

        # viewer
        layout = FRLayout(graph)
        renderer = PluggableRenderer()
        viewer = VisualizationViewer(layout, renderer)
        viewer.addGraphMouseListener(WebGraphMouseListener(self))        

        # frame
        frame = JFrame()
        grid = GridLayout(1,2)
        frame.setLayout(grid)
        pane = frame.getContentPane()

        # add components
        pane.add(viewer)
        pane.add(scroll)
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
        frame.pack()
        frame.show()
         


class WebGraphMouseListener(GraphMouseListener):
    def __init__(self, explorer):
        self.rankers = explorer.rankers
        self.text = explorer.text
        marker = '-' * 60
        self.text.append('%s\n' % marker)
        self.text.append(' Interactive Link Explorer in Jylab (CEID, University of Patras)\n')
        self.text.append('%s\n' % marker)
        
    def graphClicked(self, vertex, mevent):
        url = vertex.getUserDatum('url')
        predecessors = vertex.getPredecessors()
        successors = vertex.getSuccessors()

        purls = []
        for pred in predecessors:
            purls.append(pred.getUserDatum('url'))

        surls = []
        for succ in successors:
            surls.append(succ.getUserDatum('url'))

        indegree = vertex.inDegree()
        outdegree = vertex.outDegree()
        pagerank = self.rankers['pagerank'].getRankScore(vertex)
        authority = self.rankers['authority'].getRankScore(vertex)

        marker = '-' * 60
        s = '\n%s' % marker
        s = '%s\n  [Selected URL = %s]' % (s, url)

        s = '%s\n    [Indegree = %d]' % (s, indegree)
        for purl in purls:
            s = '%s\n    <<- %s' % (s, purl)

        s = '%s\n    [Outdegree = %d]' % (s, outdegree)
        for surl in surls:
            s = '%s\n    ->> %s' % (s, surl)

        s = '%s\n  [Rankings]' % s
        s = '%s\n    [PageRank value = %f]' % (s, pagerank)
        s = '%s\n    [Hits Authority value = %f]' % (s, authority)
        s = '%s\n' % s
        self.text.append(s)

        
        
        
    def graphPressed(self, vertex, mevent):
        pass
        
    def graphReleased(self, vertex, mevent):
        pass

# XXX more functionality... :


dirname = r'/home/giorgos/crawls/jycrawl'
dbdirname =  r'/home/giorgos/crawls/jycrawl/db'
term = r'ceid' 
nhits = 1000
htmlfile = r'/home/giorgos/search.html'

graph = digraph(dbdirname)
rankers = {}
rankers['pagerank'] = pagerank(graph)
rankers['authority'] = hits(graph)
explorer = LinkExplorer(graph, rankers)
    

