from PIL import Image

import matplotlib.pyplot as plt
import nda

nda.set_api_key('whatistheconnectome')
data = nda.request("stimulus/2/")

frame = data[:,:,1000] # frame 1000

img = Image.fromarray(frame, 'L')
img.show()
