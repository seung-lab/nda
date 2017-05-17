import matplotlib.pyplot as plt
import nda

nda.set_api_key('whatistheconnectome')
data = nda.request("treadmill/2/")

plt.plot(data.flatten())
plt.show()
