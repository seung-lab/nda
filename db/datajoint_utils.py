#
# get the channel with this name
def get_or_create_channel(connection, channel_name):
    select_channel_id = "SELECT id FROM channel WHERE name = '{0}'".format(
        channel_name)
    insert_channel_sql = "INSERT INTO channel (name) VALUES('{0}');".format(
        channel_name)
    channel_cursor = connection.cursor()
    channel_cursor.execute(select_channel_id)
    channel_row = channel_cursor.fetchone()
    if not channel_row:
        channel_cursor.execute(insert_channel_sql)
        connection.commit()
        channel_cursor.execute(select_channel_id)
        channel_row = channel_cursor.fetchone()
    channel_id = str(channel_row['id'])
    return channel_id


