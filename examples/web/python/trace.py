import matplotlib.pyplot as plt
import nda

nda.set_api_key('whatistheconnectome')
data = nda.request("trace/2/1/11/")
data = blob.unpack(my_blob)

plt.plot(data.flatten())
plt.show()
