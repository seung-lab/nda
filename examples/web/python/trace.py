import matplotlib.pyplot as plt
import nda

nda.set_api_key('whatistheconnectome')
scans_to_slice = nda.request("slices_for_cell/pinky40/v7/watershed_mst_smc_sem5_remap_2/27328840/")

for scan, slices in scans_to_slice.items():
    for slice in slices:
        data = nda.request("trace/pinky40/v7/watershed_mst_smc_sem5_remap_2/" + str(scan) + "/" + str(slice) + "/27328840/")
        fig = plt.figure() 
        fig.canvas.set_window_title(str(scan) + '-' + str(slice)) 
        plt.plot(data.flatten())
        plt.show()
