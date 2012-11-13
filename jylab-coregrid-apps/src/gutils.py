from javax.swing import *
from java.awt.image import *
from java.io import *
from javax.imageio import *

def saveAsJPEG(component, filename):
    size = component.getSize();
    print size
    image = BufferedImage(size.width, size.height, BufferedImage.TYPE_INT_RGB)
    graphics = image.createGraphics()
    graphics.fillRect(0, 0, size.width, size.height)
    component.paintComponents(graphics)
    ImageIO.write(image, 'jpeg', File(filename))


frame = JFrame('Test', size=(200, 200))
button = JButton('Button 1')
frame.contentPane.add(button)
frame.show()

saveAsJPEG(frame, '/home/giorgos/a.jpeg')



 
