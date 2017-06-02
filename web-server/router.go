package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/go-redis/redis"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"

	"github.com/rs/cors"
)

// Min2 ...
func Min2(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// Max2 ...
func Max2(x, y int) int {
	if x > y {
		return x
	}
	return y
}

var bossInfo bossConfig

var client *redis.Client

type bossConfig struct {
	AuthToken string
	URL       string
}

type dBConfig struct {
	DbUser string
	DbPass string
	DbHost string
	DbName string
}

func loadBossInfo(path string) {
	file, openError := os.Open(path)
	defer file.Close()

	if openError != nil {
		fmt.Println("boss config open error:", openError)
		os.Exit(1)
	}

	res := bossConfig{}

	decoder := json.NewDecoder(file)
	decodeError := decoder.Decode(&res)

	if decodeError != nil {
		fmt.Println("boss config decode error:", decodeError)
		os.Exit(1)
	}

	bossInfo = res
}

func loadDbConfig(path string) (dBConfig, error) {
	res := dBConfig{}

	file, openError := os.Open(path)
	defer file.Close()

	if openError != nil {
		return res, openError
	}

	decoder := json.NewDecoder(file)
	decodeError := decoder.Decode(&res)

	return res, decodeError
}

func connectToDb(path string) *sql.DB {
	config, loadConfigError := loadDbConfig(path)

	if loadConfigError != nil {
		fmt.Println("sql config open error:", loadConfigError)
		os.Exit(1)
	}

	db, sqlOpenError := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", config.DbUser, config.DbPass, config.DbHost, config.DbName))

	if sqlOpenError != nil {
		fmt.Println("sql open error:", sqlOpenError)
		os.Exit(1)
	}

	return db
}

type authCheck struct {
	handler http.Handler
}

func (a *authCheck) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	apiToken := r.Header.Get("authorization")

	_, err := client.Get(apiToken).Result()

	if err == redis.Nil {
		httpError(w, http.StatusUnauthorized, err)
	} else if err != nil {
		internalError(w, err)
	} else {
		a.handler.ServeHTTP(w, r)
	}
}

type boolRes struct {
	Result bool `json:"result"`
}

type KeypointRes struct {
	Keypoint [3]int `json:"keypoint"`
}

type parentRes struct {
	ParentNeurons map[string]int `json:"parent_neurons"`
}

type childrenRes struct {
	ChildrenSynapses map[string]int `json:"child_synapses"`
}

type neighborsRes struct {
	Presynaptic  []int `json:"presynaptic"`
	Postsynaptic []int `json:"postsynaptic"`
}

type voxelListRes struct {
	X []int `json:"x"`
	Y []int `json:"y"`
	Z []int `json:"z"`
}

func pupilHandler(pg pupilGetter) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/octet-stream")

		scanID, err1 := strconv.Atoi(ps.ByName("scanID"))

		if err1 != nil {
			httpError(w, http.StatusBadRequest, err1)
			return
		}

		pupilData, err2 := pg(scanID)

		if err2 != nil {
			internalError(w, err2)
		} else {
			w.Write(pupilData)
		}
	}
}

func functionalCellHandler(cdg cellDataGetter) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/octet-stream")

		scanID, err1 := strconv.Atoi(ps.ByName("scanID"))
		slice, err2 := strconv.Atoi(ps.ByName("slice"))
		cellID, err3 := strconv.Atoi(ps.ByName("cellID"))

		if err1 != nil || err2 != nil || err3 != nil {
			firstError := err3
			if err1 != nil {
				firstError = err1
			} else if err2 != nil {
				firstError = err2
			}

			httpError(w, http.StatusBadRequest, firstError)
			return
		}

		channelID, chanErr := getChannel(ps)

		if chanErr == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, chanErr)
			return
		} else if chanErr != nil {
			internalError(w, chanErr)
			return
		}

		funcID, lookupErr := getFunctionalID(cellID, channelID)

		if lookupErr == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, lookupErr)
			return
		} else if lookupErr != nil {
			internalError(w, lookupErr)
			return
		}

		cellData, err4 := cdg(scanID, slice, funcID)

		if err4 != nil {
			internalError(w, err4)
		} else {
			w.Write(cellData)
		}
	}
}

func idsHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	resolution, parseError := strconv.ParseUint(ps.ByName("resolution"), 10, 0)

	if parseError != nil {
		httpError(w, http.StatusBadRequest, parseError)
		return
	}

	bbox, parseError := parseBBox(ps)

	if parseError != nil {
		httpError(w, http.StatusBadRequest, parseError)
	} else {
		ids, err := getUniqueIdsInRegion(channelString(ps), bbox, resolution)

		if err != nil {
			internalError(w, err)
		} else {
			json.NewEncoder(w).Encode(ids)
		}
	}
}

func main() {
	loadBossInfo("boss.json")

	structuralDb = connectToDb("structural-db-config.json")
	defer structuralDb.Close()
	functionalDb = connectToDb("functional-db-config.json")
	defer functionalDb.Close()

	client = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()

	if err != nil {
		fmt.Println("redis open error:", err)
		os.Exit(1)
	}

	router := httprouter.New()

	// add services
	router.GET("/testtoken", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		json.NewEncoder(w).Encode("success!")
	})

	// s1 is_synapse
	router.GET("/is_synapse/:collection/:experiment/:layer/:id/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		channelID, err := getChannel(ps)

		bossID, _ := strconv.Atoi(ps.ByName("id"))
		answer, err := isSynapse(bossID, channelID)

		if err != nil {
			internalError(w, err)
		} else {
			res := boolRes{Result: answer}
			json.NewEncoder(w).Encode(res)
		}
	})

	// s5 is_neuron
	router.GET("/is_neuron/:collection/:experiment/:layer/:id/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		channelID, err := getChannel(ps)

		bossID, _ := strconv.Atoi(ps.ByName("id"))
		answer, err := isNeuron(bossID, channelID)

		if err == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err)
		} else if err != nil {
			internalError(w, err)
		} else {
			res := boolRes{Result: answer}
			json.NewEncoder(w).Encode(res)
		}
	})

	// s2 synapse_ids
	router.GET("/synapse_ids/:collection/:experiment/:layer/:resolution/:xrange/:yrange/:zrange/", idsHandler)

	// s6 neuron_ids
	router.GET("/neuron_ids/:collection/:experiment/:layer/:resolution/:xrange/:yrange/:zrange/", idsHandler)

	// s3 synapse_keypoint
	router.GET("/synapse_keypoint/:collection/:experiment/:layer/:resolution/:id/", keypointHandler)

	// s7 neuron_keypoint
	router.GET("/neuron_keypoint/:collection/:experiment/:layer/:resolution/:id/", keypointHandler)

	// s4 synapse_parent
	router.GET("/synapse_parent/:collection/:experiment/:layer/:id/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		channelID, err := getChannel(ps)

		synapseID, _ := strconv.Atoi(ps.ByName("id"))

		pre, post, err := getSynapseParents(synapseID, channelID)

		if err == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err)
		} else if err != nil {
			internalError(w, err)
		} else {
			w.Header().Set("Content-Type", "application/json")

			res := parentRes{}
			res.ParentNeurons = map[string]int{
				strconv.Itoa(pre):  1,
				strconv.Itoa(post): 2,
			}

			jsonEncodeError := json.NewEncoder(w).Encode(res)

			if jsonEncodeError != nil {
				internalError(w, jsonEncodeError)
			}
		}
	})

	// s8 neuron_children

	// test /neuron_children/team2_waypoint/pinky10/segmentation/15736:35973/19104:35456/4003:4258/15144/
	router.GET("/neuron_children/:collection/:experiment/:layer/:resolution/:xrange/:yrange/:zrange/:id/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		id, parseError := strconv.Atoi(ps.ByName("id"))

		if parseError != nil {
			httpError(w, http.StatusBadRequest, parseError)
			return
		}

		resolution, parseError := strconv.ParseUint(ps.ByName("resolution"), 10, 0)

		if parseError != nil {
			httpError(w, http.StatusBadRequest, parseError)
			return
		}

		bbox, parseError := parseBBox(ps)

		if parseError != nil {
			httpError(w, http.StatusBadRequest, parseError)
			return
		}

		queryValues := r.URL.Query()
		filterQV := queryValues.Get("filter")

		children, err := getNeuronChildren(id, channelString(ps), bbox, resolution, filterQV == "keypoint")

		if err == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err)
			return
		}

		if err != nil {
			internalError(w, err)
			return
		}

		res := childrenRes{}
		res.ChildrenSynapses = make(map[string]int)

		for _, child := range children {
			res.ChildrenSynapses[strconv.Itoa(child.Synapse)] = child.Polarity
		}

		json.NewEncoder(w).Encode(res)
	})

	router.GET("/neighbors/:collection/:experiment/:layer/:id/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		id, parseError := strconv.Atoi(ps.ByName("id"))

		if parseError != nil {
			httpError(w, http.StatusBadRequest, parseError)
			return
		}

		queryValues := r.URL.Query()

		functionalQV := queryValues.Get("functional")

		channelID, err := getChannel(ps)

		if err == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err)
			return
		}

		neuronID, neuronErr := getNeuronID(id, channelID)

		if neuronErr != nil {
			httpError(w, http.StatusNotFound, err)
			return
		}

		presynaptic, err1 := getNeighbors(neuronID, true, functionalQV == "true")
		postsynaptic, err2 := getNeighbors(neuronID, false, functionalQV == "true")

		if err1 != nil {
			internalError(w, err1)
			return
		}

		if err2 != nil {
			internalError(w, err2)
			return
		}

		res := neighborsRes{}
		res.Presynaptic = presynaptic
		res.Postsynaptic = postsynaptic

		json.NewEncoder(w).Encode(res)
	})

	// se1
	router.GET("/bbox/:collection/:experiment/:layer/:id/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		channelID, err := getChannel(ps)

		id, _ := strconv.Atoi(ps.ByName("id"))
		res, err := getBBox(id, channelID)

		if err == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err)
		} else if err != nil {
			internalError(w, err)
		} else {
			json.NewEncoder(w).Encode(res)
		}
	})

	// functional
	router.GET("/scans/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		scans, err2 := getScans()

		if err2 == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err2)
		} else if err2 != nil {
			internalError(w, err2)
		} else {
			json.NewEncoder(w).Encode(scans)
		}
	})

	router.GET("/scans/:scanID/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		scanID, err1 := strconv.Atoi(ps.ByName("scanID"))

		if err1 != nil {
			httpError(w, http.StatusBadRequest, err1)
			return
		}

		scanMetadata, err2 := getScanMetadata(scanID)

		if err2 == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err2)
		} else if err2 != nil {
			internalError(w, err2)
		} else {
			json.NewEncoder(w).Encode(scanMetadata)
		}
	})

	router.GET("/stimulus/:scanID/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/octet-stream")

		scanID, err1 := strconv.Atoi(ps.ByName("scanID"))

		if err1 != nil {
			httpError(w, http.StatusBadRequest, err1)
			return
		}

		cachedFilename := fmt.Sprintf("./cache/stimulus/%d", scanID)

		if _, err2 := os.Stat(cachedFilename); err2 == nil {
			http.ServeFile(w, r, cachedFilename)
		} else {
			stimulus, err3 := getStimulus(scanID)

			if err3 != nil {
				internalError(w, err3)
			} else {
				w.Write(stimulus)
			}
		}
	})

	router.GET("/stimulus_conditions/:scanID/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/octet-stream")

		scanID, err1 := strconv.Atoi(ps.ByName("scanID"))

		if err1 != nil {
			httpError(w, http.StatusBadRequest, err1)
			return
		}

		cachedFilename := fmt.Sprintf("./cache/stimulus/%d", scanID)

		if _, err2 := os.Stat(cachedFilename); err2 == nil {
			http.ServeFile(w, r, cachedFilename)
		} else {
			stimulusConditions, err3 := getStimulusConditions(scanID)

			if err3 != nil {
				internalError(w, err3)
			} else {
				w.Write(stimulusConditions)
			}
		}
	})

	router.GET("/treadmill/:scanID/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/octet-stream")

		scanID, err1 := strconv.Atoi(ps.ByName("scanID"))

		if err1 != nil {
			httpError(w, http.StatusBadRequest, err1)
			return
		}

		treadmill, err2 := getTreadmill(scanID)

		if err2 != nil {
			internalError(w, err2)
		} else {
			w.Write(treadmill)
		}
	})

	router.GET("/pupil_r/:scanID/", pupilHandler(getPupilR))
	router.GET("/pupil_x/:scanID/", pupilHandler(getPupilX))
	router.GET("/pupil_y/:scanID/", pupilHandler(getPupilY))

	router.GET("/slices_for_cell/:collection/:experiment/:layer/:cellID/", func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		w.Header().Set("Content-Type", "application/json")

		cellID, err1 := strconv.Atoi(ps.ByName("cellID"))

		if err1 != nil {
			httpError(w, http.StatusBadRequest, err1)
			return
		}

		channelID, chanErr := getChannel(ps)

		if chanErr == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, chanErr)
			return
		} else if chanErr != nil {
			internalError(w, chanErr)
			return
		}

		funcID, lookupErr := getFunctionalID(cellID, channelID)

		if lookupErr == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, lookupErr)
			return
		} else if lookupErr == errNoCellFunctionalId {
			httpError(w, http.StatusNotFound, lookupErr)
			return
		} else if lookupErr != nil {
			internalError(w, lookupErr)
			return
		}

		slicesPerScan, err2 := getSlicesForCell(funcID)

		fmt.Println(slicesPerScan)

		if err2 == sql.ErrNoRows {
			httpError(w, http.StatusNotFound, err2)
		} else if err2 != nil {
			internalError(w, err2)
		} else {
			json.NewEncoder(w).Encode(slicesPerScan)
		}
	})

	router.GET("/mask/:collection/:experiment/:layer/:scanID/:slice/:cellID/", functionalCellHandler(getMask))
	router.GET("/trace/:collection/:experiment/:layer/:scanID/:slice/:cellID/", functionalCellHandler(getTrace))
	router.GET("/spike/:collection/:experiment/:layer/:scanID/:slice/:cellID/", functionalCellHandler(getSpike))

	fmt.Printf("started  on port %s\n", os.Getenv("PORT"))

	addAuth := &authCheck{handler: router}

	addCors := cors.Default().Handler(addAuth)

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", os.Getenv("PORT")), addCors))
}

func internalError(w http.ResponseWriter, err error) {
	fmt.Println("internal error")
	httpError(w, http.StatusInternalServerError, err)
}

func httpError(w http.ResponseWriter, status int, err error) {
	fmt.Println("http error" + " " + string(status))
	if err != nil {
		fmt.Println(err)
	}
	http.Error(w, http.StatusText(status), status)
}

func parseBBox(ps httprouter.Params) (BBox, error) {
	res := BBox{
		MIN: Vector3{
			X: 0,
			Y: 0,
			Z: 0},
		MAX: Vector3{
			X: 0,
			Y: 0,
			Z: 0}}

	xrange := strings.Split(ps.ByName("xrange"), ",")
	yrange := strings.Split(ps.ByName("yrange"), ",")
	zrange := strings.Split(ps.ByName("zrange"), ",")

	if len(xrange) != 2 || len(yrange) != 2 || len(zrange) != 2 {
		return res, errors.New("each range should be two integers seperated by a comma")
	}

	// TODO, this parses non numbers as zero
	xmin, _ := strconv.Atoi(xrange[0])
	xmax, _ := strconv.Atoi(xrange[1])
	ymin, _ := strconv.Atoi(yrange[0])
	ymax, _ := strconv.Atoi(yrange[1])
	zmin, _ := strconv.Atoi(zrange[0])
	zmax, _ := strconv.Atoi(zrange[1])

	res.MIN.X = xmin
	res.MIN.Y = ymin
	res.MIN.Z = zmin
	res.MAX.X = xmax - 1
	res.MAX.Y = ymax - 1
	res.MAX.Z = zmax - 1
	// easier to work with inclusive bbox, nda icd defines range to be inclusive exclusive

	return res, nil
}

// Vector3 boop
type Vector3 struct {
	X int `json:"x"`
	Y int `json:"y"`
	Z int `json:"z"`
}

// GreaterEq is this vector greater or equal in every dimension than that vector?
func (v Vector3) GreaterEq(v2 Vector3) bool {
	return v.X >= v2.X && v.Y >= v2.Y && v.Z >= v2.Z
}

// LesserEq is this vector lesser or equal in every dimension than that vector?
func (v Vector3) LesserEq(v2 Vector3) bool {
	return v.X <= v2.X && v.Y <= v2.Y && v.Z <= v2.Z
}

// Max returns a new vector with the max value for each dimension
func (v Vector3) Max(v2 Vector3) Vector3 {
	return Vector3{
		Max2(v.X, v2.X),
		Max2(v.Y, v2.Y),
		Max2(v.Z, v2.Z),
	}
}

// Min returns a new vector with the min value for each dimension
func (v Vector3) Min(v2 Vector3) Vector3 {
	return Vector3{
		Min2(v.X, v2.X),
		Min2(v.Y, v2.Y),
		Min2(v.Z, v2.Z),
	}
}

// DownsampleAniso converts a vector at max resolution to downsampled coordinates
// only downsamples x and y
func (v Vector3) DownsampleAniso(resolution uint64) Vector3 {
	return Vector3{
		v.X / (1 << resolution),
		v.Y / (1 << resolution),
		v.Z,
	}
}

// Inside is this vector inside that bbox?
func (v Vector3) Inside(b BBox) bool {
	return v.GreaterEq(b.MIN) && v.LesserEq(b.MAX)
}

func addVectors(a Vector3, b Vector3) Vector3 {
	return Vector3{
		X: a.X + b.X,
		Y: a.Y + b.Y,
		Z: a.Z + b.Z,
	}
}

func subVectors(a Vector3, b Vector3) Vector3 {
	return Vector3{
		X: a.X - b.X,
		Y: a.Y - b.Y,
		Z: a.Z - b.Z,
	}
}

// BBox boop
type BBox struct {
	MIN Vector3 `json:"min"`
	MAX Vector3 `json:"max"`
}

// String test
func (b BBox) String() string {
	return fmt.Sprintf("%d:%d/%d:%d/%d:%d", b.MIN.X, b.MAX.X, b.MIN.Y, b.MAX.Y, b.MIN.Z, b.MAX.Z)
}

// Intersection returns the intersection of this bbox with that bbox
// an invalid bbox (where dimenions of max is less than the same dimension for z) occurs when there is no overlap
func (b BBox) Intersection(other BBox) (BBox, error) {
	res := BBox{
		b.MIN.Max(other.MIN),
		b.MAX.Min(other.MAX),
	}

	if res.MAX.X < res.MIN.X || res.MAX.Y < res.MIN.Y || res.MAX.Z < res.MIN.Z {
		return res, errors.New("no overlap")
	}

	return res, nil
}

// Inside is this bbox fully inside that bbox?
func (b BBox) Inside(other BBox) bool {
	return b.MIN.GreaterEq(other.MIN) && b.MAX.LesserEq(other.MAX)
}

func keypointHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")

	channelID, parseError := getChannel(ps)
	if parseError != nil {
		httpError(w, http.StatusBadRequest, parseError)
		return
	}

	resolution, parseError := strconv.ParseUint(ps.ByName("resolution"), 10, 0)

	if parseError != nil {
		httpError(w, http.StatusBadRequest, parseError)
		return
	}

	bossID, parseError := strconv.Atoi(ps.ByName("id"))
	if parseError != nil {
		httpError(w, http.StatusBadRequest, parseError)
		return
	}

	keypoint, err := getKeypoint(bossID, channelID)

	keypoint = keypoint.DownsampleAniso(resolution)

	if err == sql.ErrNoRows {
		httpError(w, http.StatusNotFound, err)
	} else if err != nil {
		internalError(w, err)
	} else {
		res := KeypointRes{Keypoint: [...]int{keypoint.X, keypoint.Y, keypoint.Z}}
		json.NewEncoder(w).Encode(res)
	}
}
