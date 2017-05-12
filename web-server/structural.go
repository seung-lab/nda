package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/julienschmidt/httprouter"
	"github.com/parnurzeal/gorequest"
)

var structuralDb *sql.DB

func isSynapse(bossID int, channelID int) (bool, error) {
	var exists bool
	err := structuralDb.QueryRow("SELECT 1 FROM synapse, voxel_set WHERE synapse.voxel_set = voxel_set.id AND voxel_set.boss_vset_id=? and voxel_set.channel=?", bossID, channelID).Scan(&exists)

	if err == sql.ErrNoRows {
		err = nil
	}

	return exists, err
}

func isNeuron(bossID int, channelID int) (bool, error) {
	var exists bool
	err := structuralDb.QueryRow("SELECT 1 FROM neuron, voxel_set WHERE neuron.voxel_set = voxel_set.id AND voxel_set.boss_vset_id=? and voxel_set.channel=?", bossID, channelID).Scan(&exists)

	return exists, err
}

func getBBox(bossID int, channelID int) (BBox, error) {
	var res = BBox{
		MIN: Vector3{
			X: 0,
			Y: 0,
			Z: 0},
		MAX: Vector3{
			X: 0,
			Y: 0,
			Z: 0}}
	err := structuralDb.QueryRow("SELECT x_min, y_min, z_min, x_max, y_max, z_max FROM voxel_set WHERE voxel_set.boss_vset_id=? and voxel_set.channel=?", bossID, channelID).Scan(
		&res.MIN.X, &res.MIN.Y, &res.MIN.Z,
		&res.MAX.X, &res.MAX.Y, &res.MAX.Z)

	return res, err
}

func channelString(ps httprouter.Params) string {
	return fmt.Sprintf("%s/%s/%s",
		ps.ByName("collection"),
		ps.ByName("experiment"),
		ps.ByName("layer"))
}

func getChannelFromString(s string) (int, error) {
	var channelID int
	err := structuralDb.QueryRow("SELECT id FROM channel where name=?", s).Scan(&channelID)
	return channelID, err
}

func getChannel(ps httprouter.Params) (int, error) {
	return getChannelFromString(channelString(ps))
}

func getKeypoint(bossID int, channelID int) (Vector3, error) {
	var res = Vector3{}
	err := structuralDb.QueryRow("SELECT key_point_x, key_point_y, key_point_z FROM voxel_set WHERE voxel_set.boss_vset_id=? and voxel_set.channel = ?", bossID, channelID).Scan(&res.X, &res.Y, &res.Z)
	return res, err
}

func getSynapseParents(synapseID int, channelID int) (int, int, error) {
	var pre int
	var post int

	err := structuralDb.QueryRow(`
	SELECT
		(SELECT boss_vset_id from voxel_set, neuron where neuron.id = synapse.pre AND voxel_set.id = neuron.voxel_set) as pre,
		(SELECT boss_vset_id from voxel_set, neuron where neuron.id = synapse.post AND voxel_set.id = neuron.voxel_set) as post
	FROM
		synapse, voxel_set
	WHERE
		synapse.voxel_set = voxel_set.id
		AND voxel_set.boss_vset_id=?
		AND voxel_set.channel=?`, synapseID, channelID).Scan(&pre, &post)

	return pre, post, err
}

func idInRegion(id int, channel string, region BBox, idBbox BBox, resolution uint64) (bool, error) {
	if idBbox.Inside(region) {
		return true, nil
	}

	// otherwise check each voxel in intersection
	intersection, err := idBbox.Intersection(region)

	if err != nil {
		return false, nil // no overlap, skip
	}

	ids, err := getUniqueIdsInRegion(channel, intersection, resolution)

	if err != nil {
		return false, err
	}

	for _, bossIDStr := range ids.Ids {
		bossID, parseError := strconv.Atoi(bossIDStr)

		if parseError != nil {
			return false, parseError
		}

		if bossID == id {
			return true, nil
		}
	}

	return false, nil
}

// Child boop
type child struct {
	Synapse  int
	Polarity int
}

func getNeuronChildren(bossID int, channel string, region BBox, resolution uint64) ([]child, error) {
	channelID, err := getChannelFromString(channel)

	if err != nil {
		return nil, err
	}

	var neuronID int
	err = structuralDb.QueryRow(`
		SELECT
			neuron.id
		FROM
			neuron, voxel_set
		WHERE 
			neuron.voxel_set = voxel_set.id
			AND voxel_set.boss_vset_id=? and voxel_set.channel=?`, bossID, channelID).Scan(&neuronID)

	if err != nil {
		return nil, err
	}

	rows, err := structuralDb.Query(`
	SELECT
		boss_vset_id,
		synapse.pre,
		x_min, y_min, z_min,
		x_max, y_max, z_max,
		channel.name
	FROM
		synapse,
		voxel_set,
		channel
	WHERE
		synapse.voxel_set = voxel_set.id
		AND voxel_set.channel = channel.id
		AND (synapse.pre = ? OR synapse.post = ?)
	`, neuronID, neuronID)
	defer rows.Close()

	res := make([]child, 0) // for output to json to be [] in the empty case, not null

	// for each synapse
	for rows.Next() {
		var synapseID int
		var preID int
		var xMin, yMin, zMin int
		var xMax, yMax, zMax int

		var synapseChannelString string

		err := rows.Scan(&synapseID, &preID, &xMin, &yMin, &zMin, &xMax, &yMax, &zMax, &synapseChannelString)

		if err != nil {
			return nil, err
		}

		var polarity = 1
		if neuronID != preID {
			polarity = 2
		}

		synapseBbox := BBox{
			MIN: Vector3{
				X: xMin,
				Y: yMin,
				Z: zMin}.DownsampleAniso(resolution),
			MAX: Vector3{
				X: xMax,
				Y: yMax,
				Z: zMax}.DownsampleAniso(resolution)}

		inRegion, err := idInRegion(synapseID, synapseChannelString, region, synapseBbox, resolution)

		if err != nil {
			return res, err // network error, break
		} else if inRegion {
			res = append(res, child{synapseID, polarity})
		}
	}

	return res, err
}

// IdsInRegionRes boop
type IdsInRegionRes struct {
	Ids []string `json:"ids"`
}

// we don't convert from string to number because we proxy this result for service 2 and 6 and JSON 64 bit integers need to be strings
func getUniqueIdsInRegion(channel string, bbox BBox, resolution uint64) (IdsInRegionRes, error) {
	// adding 1 to max because boss ranges are inclusive exclusive
	url := fmt.Sprintf(bossInfo.URL+"ids/%s/%d/%d:%d/%d:%d/%d:%d/", channel, resolution, bbox.MIN.X, bbox.MAX.X+1, bbox.MIN.Y, bbox.MAX.Y+1, bbox.MIN.Z, bbox.MAX.Z+1)

	fmt.Println("getUniqueIdsInRegion", url)

	request := gorequest.New()
	resp, body, errArr := request.Get(url).
		Set("Authorization", bossInfo.AuthToken).
		EndBytes()

	var ids IdsInRegionRes

	if resp.StatusCode >= 400 {
		return ids, fmt.Errorf("http error, status code: %d url: %s", resp.StatusCode, url)
	}

	if len(errArr) > 0 {
		return ids, errArr[0]
	}

	err := json.Unmarshal(body, &ids)

	return ids, err
}

func getFunctionalID(bossID int, channelID int) (int, error) {
	var functionalID int
	err := structuralDb.QueryRow(`SELECT neuron.em_id from neuron, voxel_set where neuron.voxel_set = voxel_set.id and boss_vset_id = ? and channel = ?`, bossID, channelID).Scan(&functionalID)

	return functionalID, err
}
