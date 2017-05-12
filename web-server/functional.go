package main

import (
	"database/sql"
	"strconv"
)

var functionalDb *sql.DB

type pupilGetter func(int) ([]byte, error)

// pupil information is separated into 3 longblobs, pupil_r (radius), pupil_x (x pos), pupil_y (y_pos)
func getPupilR(scanID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select pupil_r from pupil where scan_idx = ?`, scanID).Scan(&res)

	return res, err
}

func getPupilX(scanID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select pupil_x from pupil where scan_idx = ?`, scanID).Scan(&res)

	return res, err
}

func getPupilY(scanID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select pupil_y from pupil where scan_idx = ?`, scanID).Scan(&res)

	return res, err
}

func getScans() ([]int, error) {
	scans, err2 := functionalDb.Query(`SELECT scan_idx from scan ORDER BY scan_idx asc`)

	res := make([]int, 0)

	if err2 != nil {
		return res, err2
	}

	for scans.Next() {
		var scanIdx int
		err3 := scans.Scan(&scanIdx)

		if err3 != nil {
			return res, err3
		}

		res = append(res, scanIdx)
	}

	return res, nil
}

// ScanMetadataRes boop
type ScanMetadataRes struct {
	Depth      int    `json:"depth"`
	LaserPower int    `json:"laserPower"`
	Wavelength int    `json:"wavelength"`
	Filename   string `json:"filename"`

	NFrames      int     `json:"nFrames"`
	PxWidth      int     `json:"pxWidth"`
	PxHeight     int     `json:"pxHeight"`
	UmHeight     float64 `json:"umHeight"`
	UmWidth      float64 `json:"umWidth"`
	Bidrectional int     `json:"bidrectional"`
	Fps          float64 `json:"fps"`
	Zoom         float64 `json:"zoom"`
	NChannels    int     `json:"nChannels"`
	NSlices      int     `json:"nSlices"`
	FillFraction float64 `json:"fillFraction"`
	RasterPhase  float64 `json:"rasterPhase"`
	SliceOffsets []int   `json:"sliceOffsets"`
}

func getScanMetadata(scanID int) (ScanMetadataRes, error) {
	res := ScanMetadataRes{}

	err := functionalDb.QueryRow(`select
			depth, laser_power, wavelength, filename, nframes, px_width, px_height, um_width, um_height, bidirectional, fps, zoom, nchannels, nslices, fill_fraction, raster_phase
		from
			scan,
			scan_info
		where scan.scan_idx = ? and scan.scan_idx = scan_info.scan_idx`,
		scanID).Scan(&res.Depth, &res.LaserPower, &res.Wavelength, &res.Filename, &res.NFrames, &res.PxWidth, &res.PxHeight,
		&res.UmHeight, &res.UmWidth, &res.Bidrectional, &res.Fps, &res.Zoom, &res.NChannels, &res.NSlices, &res.FillFraction, &res.RasterPhase)

	if err != nil {
		return res, err
	}

	slices, err2 := functionalDb.Query(`SELECT z_offset from slice where scan_idx = ? ORDER BY slice asc`, scanID)

	if err2 != nil {
		return res, err2
	}

	zOffsets := make([]int, 0)

	for slices.Next() {
		var zOffset int
		err3 := slices.Scan(&zOffset)

		if err3 != nil {
			return res, err3
		}

		zOffsets = append(zOffsets, zOffset)
	}

	res.SliceOffsets = zOffsets

	return res, err
}

func getStimulus(scanID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select movie from stimulus where scan_idx = ?`, scanID).Scan(&res)

	return res, err
}

func getStimulusConditions(scanID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select conditions from stimulus where scan_idx = ?`, scanID).Scan(&res)

	return res, err
}

func getTreadmill(scanID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select treadmill_speed from treadmill where scan_idx = ?`, scanID).Scan(&res)

	return res, err
}

type cellDataGetter func(int, int, int) ([]byte, error)

func getTrace(scanID int, slice int, cellID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select trace from trace where scan_idx = ? and slice = ? and em_id = ?`, scanID, slice, cellID).Scan(&res)

	return res, err
}

func getSpike(scanID int, slice int, cellID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select rate from __spike where scan_idx = ? and slice = ? and em_id = ?`, scanID, slice, cellID).Scan(&res)

	return res, err
}

func getMask(scanID int, slice int, cellID int) ([]byte, error) {
	var res []byte
	err := functionalDb.QueryRow(`select mask_pixels from mask where scan_idx = ? and slice = ? and em_id = ?`, scanID, slice, cellID).Scan(&res)

	return res, err
}

func getSlicesForCell(cellID int) (map[string][]int, error) {
	res := make(map[string][]int)

	slices, err := functionalDb.Query(`select scan_idx, slice from mask where em_id = ?`, cellID)

	if err != nil {
		return res, err
	}

	for slices.Next() {
		var scanIdx int
		var slice int
		err2 := slices.Scan(&scanIdx, &slice)

		if err2 != nil {
			return res, err2
		}

		res[strconv.Itoa(scanIdx)] = append(res[strconv.Itoa(scanIdx)], slice)

	}

	return res, err
}
