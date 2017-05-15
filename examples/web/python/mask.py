from PIL import Image
import nda

nda.set_api_key('whatistheconnectome')
scans_to_slice = nda.request("slices_for_cell/pinky40/v7/watershed_mst_smc_sem5_remap_2/27328840/")

for scan, slices in scans_to_slice.items():
    for slice in slices:
        print(scan, slice)
        data = request("mask/pinky40/v7/watershed_mst_smc_sem5_remap_2/" + str(scan) + "/" + str(slice) + "/27328840/")
        mask = np.zeros(256 * 256, dtype=np.int8)

        for idx in data.flatten():
            mask[int(idx)-1] = 255 # 1 indexed
        
        mask = mask.reshape((256, 256))

        img = Image.fromarray(mask, 'L')
        img.show()
