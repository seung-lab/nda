from __future__ import print_function
import csv
import pymysql.cursors
import time
import numpy as np
import datajoint_utils

############################### CONFIGURATION #################################
mysql_host = ''
mysql_user = ''
mysql_password = ''
mysql_db = ''


# this is a space separated file
#   seg_id min_x min_y min_z max_x max_y max_z
filename_bbox = './bbox_volume.in'

# this is a space separated file
#   seg_id mass_x mass_y mass_z size
filename_center_of_mass = './com_volume.in'

# includes a vector of new remapped seg_ids
# the first value is the remapping of segment id 0 
filename_remapping = "mst_smc_sem5_remap.npy"

filename_voxel_set = './voxel_set_smc_sem5.txt'

# this is a vector of remapped ids from our seg ids to the em_functional_id
# [
#   [our_id_1, em_functional_id_1],
#   [our_id_2, em_functional_id_2],
#   ...
# ]
# 
filename_functional_remapping ='remap_functional_smc_sem5.npy'

voxel_set_batch_size = 100000

# channel name for segments
channel_name = "pinky40/v7/watershed_mst_smc_sem5_remap"


#################################### RUN ######################################
connection = pymysql.connect(host=mysql_host,
                             port=3306,
                             user=mysql_user,
                             password=mysql_password,
                             db=mysql_db,
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor,
                             local_infile=True)

# get channel
channel_id = datajoint_utils.get_or_create_channel(
        connection, channel_name)
print('Using segment name:', channel_name, ', channel id:',
        channel_id)

# this class used to aggregate bounding boxes, sizes, keypoints
class VoxelSet(object):
    __slots__ = ('boss_vset_id',
                 'size',
                 'key_point_mass',
                 'min',
                 'max')

    def __init__(self,
                 boss_vset_id,
                 size,
                 key_point_mass,
                 bbox_min,
                 bbox_max):
        self.boss_vset_id = boss_vset_id
        self.size = size
        self.key_point_mass = key_point_mass
        self.min = bbox_min
        self.max = bbox_max

    def merge(self, other):
        self.key_point_mass = self.key_point_mass + other.key_point_mass 
        self.size += other.size
        self.min = np.minimum(self.min, other.min)
        self.max = np.maximum(self.max, other.max)

remap = np.load(filename_remapping)
file_bbox = open(filename_bbox, 'r')
file_center_of_mass = open(filename_center_of_mass, 'r')
reader_bbox = csv.reader(file_bbox, delimiter=' ')
reader_center_of_mass = csv.reader(file_center_of_mass, delimiter=' ')

# aggregate all remapped voxel_sets
segment_count = 0
lookup = dict()
begin_merge_time = time.time()
start_time = time.time()
for bbox in reader_bbox:
    center_of_mass = reader_center_of_mass.next()
    if bbox[0] != center_of_mass[0]:
        print("bbox seg id", bbox[0], " does not match ", center_of_mass[0])
        continue

    bbox_number = [int(i) for i in bbox]
    center_of_mass = [int(i) for i in center_of_mass]
    
    voxel_set_entry = VoxelSet(
            bbox_number[0],
            center_of_mass[3],
            np.array(center_of_mass[1:4], dtype=np.float64),
            np.array(bbox_number[1:4]),
            np.array(bbox_number[4:7]))

    root_id = remap[voxel_set_entry.boss_vset_id]

    if lookup.has_key(root_id):
        lookup[root_id].merge(voxel_set_entry)
    else:
        lookup[root_id] = voxel_set_entry
    
    segment_count += 1
    if segment_count % voxel_set_batch_size == 0:
        elapsed_seconds = time.time() - start_time
        print('imported ', segment_count, ' in ', elapsed_seconds, ' seconds, ',
                elapsed_seconds / voxel_set_batch_size, ' per row. lookup has ',
                len(lookup), ' entries ')
        start_time = time.time()

# save aggregate voxel sets to disk
file_temp = open(filename_voxel_set, 'w')
file_temp.write("boss_vset_id,size,key_point_x,key_point_y,key_point_z,")
file_temp.write("x_min,y_min,z_min,x_max,y_max,z_max,channel\n")
csv_string = "{0},{1.size},{1.key_point_mass[0]},{1.key_point_mass[1]},\
{1.key_point_mass[2]},{1.min[0]},{1.min[1]},{1.min[2]},{1.max[0]},{1.max[1]},\
{1.max[2]},{2}\n"

for root_id, voxel_set in lookup.iteritems():
    voxel_set.key_point_mass = voxel_set.key_point_mass / voxel_set.size
    file_temp.write(csv_string.format(root_id, voxel_set, channel_id))
elapsed_seconds = time.time() - start_time
print('imported ', segment_count, ' in ', elapsed_seconds, ' seconds, ',
        elapsed_seconds / voxel_set_batch_size, ' per row. lookup has ',
        len(lookup), ' entries ')
file_temp.close()

# upload voxel_sets to database
begin_load_data_time = time.time()
load_data_sql = """
    LOAD DATA LOCAL INFILE '{0}' INTO TABLE voxel_set
        CHARACTER SET UTF8
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (boss_vset_id, size, key_point_x, key_point_y, key_point_z, x_min,
         y_min, z_min, x_max, y_max, z_max, channel)
        SET channel = {1};
""".format(filename_voxel_set, channel_id)
load_data_cursor = connection.cursor()
load_data_cursor.execute(load_data_sql)
connection.commit()
print('loaded data in ', time.time() - begin_load_data_time)

# insert neuron table to database
begin_add_neuron_time = time.time()
print('adding neuron data')
insert_neuron_sql = """
    insert into neuron (voxel_set) select id from voxel_set where channel = {0};
""".format(channel_id)
insert_neuron_cursor = connection.cursor()
insert_neuron_cursor.execute(insert_neuron_sql)
connection.commit()
print('added neuron data in ', time.time() - begin_add_neuron_time)

# update existing neurons to have the functional em_id's
remap_functional = np.load(filename_functional_remapping)
begin_remap_functional_time = time.time()
print('adding neuron data')
insert_neuron_sql = """
    update neuron set em_id = {0} where voxel_set = (select id from voxel_set where boss_vset_id = {1} and channel = {2});
"""
insert_neuron_cursor = connection.cursor()
for remap in remap_functional:
    boss_vset_id = remap[0]
    em_functional_id = remap[1]
    sql_update_functional = insert_neuron_sql.format(em_functional_id, boss_vset_id, channel_id)
    print('setting neuron with boss_vset_id {0} to map to functional data {1}'.format(boss_vset_id, em_functional_id))
    insert_neuron_cursor.execute(sql_update_functional)
print('updated ', remap_functional.shape, ' in ', time.time() - begin_remap_functional_time)
connection.commit()
