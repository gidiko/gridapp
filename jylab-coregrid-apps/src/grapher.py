from edu.uci.ics.jung.utils import *
from edu.uci.ics.jung.graph.impl import *
from edu.uci.ics.jung.algorithms.importance import *
from edu.uci.ics.jung.random.generators import *
from edu.uci.ics.jung.visualization import *
from samples.graph import *
from javax.swing import *
from converter import *


def saveAsJPEG(component, filename):
    size = component.getSize();
    image = BufferedImage(size.width, size.height, BufferedImage.TYPE_INT_RGB)
    graphics = image.createGraphics()
    component.paintComponent(graphics)
    out = new FileOutputStream(filename)
    encoder = JPEGCodec.createJPEGEncoder(out);
    encoder.encode(image);
    out.close();



successors = toSuccessors(links)
urls = toListing(links)

vertices = {}
for url in urls:
    vertex = DirectedSparseVertex()
    vertex.addUserDatum('url', url, UserData.SHARED)
    vertices[url] = vertex

graph = DirectedSparseGraph()
values = vertices.values()
for value in values:
    graph.addVertex(value)




roots = successors.keys()
for root in roots:
    vparent = vertices[root]
    #graph.addVertex(vparent)
    children = successors[root]
    for child in children:
        vchild = vertices[child]
        #graph.addVertex(vchild)
        graph.addEdge(DirectedSparseEdge(vparent, vchild))


ranker = PageRank(graph, 0.15)
ranker.evaluate();
ranker.printRankings()

visualizer = FRLayout(graph)
renderer = BasicRenderer()
viewer = VisualizationViewer(visualizer, renderer)
frame = JFrame()
frame.contentPane.add(viewer)
frame.setVisible(1)
