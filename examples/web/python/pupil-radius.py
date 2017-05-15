import matplotlib.pyplot as plt
import nda

nda.set_api_key('whatistheconnectome')
data = nda.request("pupil_x/5/")

plt.plot(data.flatten())
plt.show()
