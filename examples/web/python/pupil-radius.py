import matplotlib.pyplot as plt
import numpy as np

from datajoint import blob

import urllib.request
my_blob = urllib.request.urlopen("https://nda.seunglab.org/pupil_x/5/").read()

data = blob.unpack(my_blob)

plt.plot(data.flatten())
plt.show()
