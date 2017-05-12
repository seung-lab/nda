from PIL import Image
import numpy as np

from datajoint import blob

import urllib.request
my_blob = urllib.request.urlopen("https://nda.seunglab.org/mask/5/1/459/").read()

data = blob.unpack(my_blob)

mask = np.zeros(256 * 256, dtype=np.int8)

for idx in data.flatten():
    mask[int(idx)-1] = 255 # 1 indexed

mask = mask.reshape((256, 256))

img = Image.fromarray(mask, 'L')
img.show()
