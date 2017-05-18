from __future__ import print_function
import csv
import pymysql.cursors
import time
import numpy as np # import numpy as np
import datajoint_utils

############################### CONFIGURATION #################################
mysql_host = ''
mysql_user = ''
mysql_password = ''
mysql_db = ''

# csv file with:
# segid, pre_boss_vset_id, post_boss_vset_id, keypoint_x, keypoint_y,
# keypoint_z, bbox_min_x, bbox_min_y, bbox_min_z, bbox_max_x, bbox_max_y,
# bbox_max_z
filename_synapses = 'final_edges_smc_sem5.csv'

channel_name_segment = "pinky40/v7/watershed_mst_smc_sem5_remap"
channel_name_synapse = "pinky40/v7/psdsegs_mst_smc"

update_pre_post_batch_size = 10000

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
channel_id_synapse = datajoint_utils.get_or_create_channel(
        connection, channel_name_synapse)
channel_id_segment = datajoint_utils.get_or_create_channel(
        connection, channel_name_segment)

print('Using segment name:', channel_name_segment, ', channel id:',
        channel_id_segment)
print('Using synapse name:', channel_name_synapse, ', channel id:',
        channel_id_synapse)

print("Load csv into database")
load_data_sql = """
    LOAD DATA LOCAL INFILE '{0}' INTO TABLE voxel_set
        CHARACTER SET UTF8
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
        IGNORE 1 LINES
        (boss_vset_id, @dummy, @dummy, key_point_x, key_point_y, key_point_z,
         size, x_min, y_min, z_min, x_max, y_max, z_max)
        SET channel = {1};
""".format(filename_synapses, channel_id_synapse)

begin_load_voxel_set_time = time.time()
load_file_cursor = connection.cursor()
load_file_cursor.execute(load_data_sql)
connection.commit()
print('uploaded ', filename_synapses, ' in ', time.time() - begin_load_voxel_set_time)

# insert synapse data with pre and post 
print("Insert synapse data with pre and post")
synapse_insert_sql = """
    INSERT INTO synapse (voxel_set, pre, post)
    SELECT voxel_set_v, pre_v, post_v
    FROM (
        SELECT vs.id AS voxel_set_v
        FROM voxel_set AS vs
        WHERE boss_vset_id = {0} AND
            channel = {3}
        ) AS voxel_set
    CROSS JOIN (
        SELECT pren.id as pre_v
        FROM neuron AS pren
        JOIN voxel_set AS prenvs ON pren.voxel_set = prenvs.id
        WHERE prenvs.boss_vset_id = {1} AND
            channel = {4}
        ) AS pre
    CROSS JOIN (
        SELECT postn.id as post_v
        FROM neuron AS postn
        JOIN voxel_set AS postnvs ON postn.voxel_set = postnvs.id
        WHERE postnvs.boss_vset_id = {2} AND
            channel = {4}
        
        ) AS post
                """

synapse_cursor = connection.cursor()
begin_insert_time = time.time()
begin_batch_insert_time = time.time()
with open(filename_synapses) as file_synapses:
    reader_synapses = csv.reader(file_synapses)
    synapse_count = 0
    # skip header
    reader_synapses.next()
    for synapse_row in reader_synapses:
        synapse_row = [int(val) for val in synapse_row]
        synapse_cursor.execute(synapse_insert_sql.format(synapse_row[0],
            synapse_row[1], synapse_row[2], channel_id_synapse,
            channel_id_segment))

        synapse_count += 1
        if synapse_count % update_pre_post_batch_size == 0:
            synapse_cursor = connection.commit()
            print('Processed ', synapse_count, ' in ', time.time() - begin_batch_insert_time)
            begin_batch_insert_time = time.time()
            synapse_cursor = connection.cursor()

# commit the rest of the synapses
synapse_cursor = connection.commit()
print('Processed Total', synapse_count, ' in ', time.time() - begin_insert_time)

connection.commit()
exit()        
