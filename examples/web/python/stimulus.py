from PIL import Image
import numpy as np

from datajoint import blob

import urllib.request
my_blob = urllib.request.urlopen("https://nda.seunglab.org/stimulus/2").read()

data = blob.unpack(my_blob)

frame = data[:,:,1000] # frame 1000

img = Image.fromarray(frame, 'L')
img.show()
