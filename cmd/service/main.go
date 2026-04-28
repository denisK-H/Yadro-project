package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"time"

	"gopkg.in/yaml.v3"
)

type DeviceConfig struct {
	Resource         string `yaml:"resource"`
	TimeoutMS        int    `yaml:"timeout_ms"`
	WriteTermination string `yaml:"write_termination"`
	ReadTermination  string `yaml:"read_termination"`
	SkipIDN          bool   `yaml:"skip_idn"`
}

type DevicesConfig struct {
	Gen DeviceConfig `yaml:"gen"`
	Osc DeviceConfig `yaml:"osc"`
}

type SCPIConfig struct {
	GeneratorInit []string `yaml:"generator_init"`
	SetFrequency  string   `yaml:"set_frequency"`
	MeasureVin    string   `yaml:"measure_vin"`
	MeasureVout   string   `yaml:"measure_vout"`
}

type ExperimentConfig struct {
	FreqStartHz    float64 `yaml:"freq_start_hz"`
	FreqStopHz     float64 `yaml:"freq_stop_hz"`
	Points         int     `yaml:"points"`
	SettleMS       int     `yaml:"settle_ms"`
	ReadMode       string  `yaml:"read_mode"`
	ReadDelayMS    int     `yaml:"read_delay_ms"`
	ReadMaxBytes   int     `yaml:"read_max_bytes"`
	MeasureRetries int     `yaml:"measure_retries"`
	RetryDelayMS   int     `yaml:"retry_delay_ms"`
}

type AdaptiveConfig struct {
	Enabled           bool    `yaml:"enabled"`
	MaxPoints         int     `yaml:"max_points"`
	MaxPasses         int     `yaml:"max_passes"`
	RefineDBThreshold float64 `yaml:"refine_db_threshold"`
	UnityGainTarget   float64 `yaml:"unity_gain_target"`
	UnitySearchEnable bool    `yaml:"unity_search_enabled"`
	UnityTolRatio     float64 `yaml:"unity_tol_ratio"`
	UnityMaxIter      int     `yaml:"unity_max_iter"`
}

type Config struct {
	BridgeURL  string           `yaml:"bridge_url"`
	Devices    DevicesConfig    `yaml:"devices"`
	SCPI       SCPIConfig       `yaml:"scpi"`
	Experiment ExperimentConfig `yaml:"experiment"`
	Adaptive   AdaptiveConfig   `yaml:"adaptive"`
	UnitySearch UnitySearchConfig `yaml:"unity_search"`
}

type BridgeClient struct {
	BaseURL string
	Client  *http.Client
}

type Point struct {
	FreqHz    float64
	Vin       float64
	Vout      float64
	Gain      float64
	VinRaw    string
	VoutRaw   string
	OK        bool
	PointType string
}

type UnitySearchConfig struct {
	Enabled       bool    `yaml:"enabled"`
	StartHz       float64 `yaml:"start_hz"`
	MaxHz         float64 `yaml:"max_hz"`
	FreqMultiplier float64 `yaml:"freq_multiplier"`
	TargetGain    float64 `yaml:"target_gain"`
	Tolerance      float64 `yaml:"tolerance"`
	MaxRefineIter  int     `yaml:"max_refine_iter"`
}

func NewBridgeClient(baseURL string) *BridgeClient {
	return &BridgeClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func searchUnityGainByIncreasingFrequency(
	bridge *BridgeClient,
	cfg *Config,
) []Point {
	points := make([]Point, 0)

	startHz := cfg.UnitySearch.StartHz
	maxHz := cfg.UnitySearch.MaxHz
	multiplier := cfg.UnitySearch.FreqMultiplier
	target := cfg.UnitySearch.TargetGain

	if startHz <= 0 {
		startHz = cfg.Experiment.FreqStartHz
	}

	if maxHz <= 0 {
		maxHz = cfg.Experiment.FreqStopHz
	}

	if multiplier <= 1 {
		multiplier = 1.3
	}

	if target <= 0 {
		target = 1.0
	}

	fmt.Println("Starting unity gain search...")
	fmt.Printf("start=%.2f Hz, max=%.2f Hz, multiplier=%.3f, target=%.3f\n",
		startHz, maxHz, multiplier, target,
	)

	f := startHz

	first := measurePoint(bridge, cfg, f, "base")
	points = append(points, first)

	if !first.OK {
		fmt.Println("First point measurement failed")
		return points
	}

	if first.Gain <= target {
		fmt.Printf(
			"Gain is already <= %.3f at %.2f Hz. Gain=%.6f\n",
			target,
			first.FreqHz,
			first.Gain,
		)
		return points
	}

	prev := first

	for {
		f = f * multiplier

		if f > maxHz {
			fmt.Printf(
				"Unity gain was not found up to %.2f Hz. Last gain=%.6f\n",
				maxHz,
				prev.Gain,
			)
			break
		}

		curr := measurePoint(bridge, cfg, f, "base")
		points = append(points, curr)

		if !curr.OK {
			fmt.Printf("Measurement failed at %.2f Hz, continue...\n", f)
			prev = curr
			continue
		}

		if prev.OK && prev.Gain > target && curr.Gain <= target {
			fmt.Printf(
				"Unity gain bracket found: %.2f Hz Gain=%.6f -> %.2f Hz Gain=%.6f\n",
				prev.FreqHz,
				prev.Gain,
				curr.FreqHz,
				curr.Gain,
			)

			refined := refineUnityGain(bridge, cfg, prev, curr)
			points = append(points, refined...)

			break
		}

		prev = curr
	}

	sortPoints(points)
	return points
}

func refineUnityGain(
	bridge *BridgeClient,
	cfg *Config,
	left Point,
	right Point,
) []Point {
	refinedPoints := make([]Point, 0)

	target := cfg.UnitySearch.TargetGain
	tolerance := cfg.UnitySearch.Tolerance
	maxIter := cfg.UnitySearch.MaxRefineIter

	if target <= 0 {
		target = 1.0
	}
	if tolerance <= 0 {
		tolerance = 0.03
	}
	if maxIter <= 0 {
		maxIter = 10
	}

	fmt.Println("Refining unity gain frequency...")

	best := left
	if math.Abs(right.Gain-target) < math.Abs(left.Gain-target) {
		best = right
	}

	for i := 0; i < maxIter; i++ {
		midFreq := math.Sqrt(left.FreqHz * right.FreqHz)

		mid := measurePoint(bridge, cfg, midFreq, "unity")
		refinedPoints = append(refinedPoints, mid)

		if !mid.OK {
			fmt.Printf("Refine measurement failed at %.2f Hz\n", midFreq)
			break
		}

		if math.Abs(mid.Gain-target) < math.Abs(best.Gain-target) {
			best = mid
		}

		relativeError := math.Abs(mid.Gain-target) / target
		if relativeError <= tolerance {
			fmt.Printf(
				"Unity gain found: f=%.2f Hz, Gain=%.6f\n",
				mid.FreqHz,
				mid.Gain,
			)
			break
		}

		if mid.Gain > target {
			left = mid
		} else {
			right = mid
		}
	}

	fmt.Printf(
		"Best unity approximation: f=%.2f Hz, Gain=%.6f\n",
		best.FreqHz,
		best.Gain,
	)

	return refinedPoints
}

func (b *BridgeClient) post(path string, reqBody any, out any) error {
	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	resp, err := b.Client.Post(
		b.BaseURL+path,
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(raw))
	}

	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return err
		}
	}

	return nil
}

func (b *BridgeClient) Connect(name string, cfg DeviceConfig) error {
	req := map[string]any{
		"name":              name,
		"resource":          cfg.Resource,
		"timeout_ms":        cfg.TimeoutMS,
		"write_termination": cfg.WriteTermination,
		"read_termination":  cfg.ReadTermination,
		"skip_idn":          cfg.SkipIDN,
	}

	var resp map[string]any
	if err := b.post("/connect", req, &resp); err != nil {
		return err
	}

	fmt.Printf("%s connected: %v\n", name, resp["idn"])
	return nil
}

func (b *BridgeClient) Write(device, cmd string) error {
	req := map[string]any{
		"device": device,
		"cmd":    cmd,
	}
	return b.post("/write", req, nil)
}

func (b *BridgeClient) QueryNumber(device, cmd, mode string, delayMS, maxBytes int) (float64, string, error) {
	req := map[string]any{
		"device":    device,
		"cmd":       cmd,
		"mode":      mode,
		"delay_ms":  delayMS,
		"max_bytes": maxBytes,
	}

	var resp struct {
		OK    bool    `json:"ok"`
		Value float64 `json:"value"`
		Raw   string  `json:"raw"`
		Error string  `json:"error"`
	}

	if err := b.post("/query_number", req, &resp); err != nil {
		return 0, "", err
	}

	return resp.Value, resp.Raw, nil
}

func (b *BridgeClient) CloseAll() {
	_ = b.post("/close_all", map[string]any{}, nil)
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.Experiment.MeasureRetries <= 0 {
		cfg.Experiment.MeasureRetries = 1
	}
	if cfg.Adaptive.MaxPoints <= 0 {
		cfg.Adaptive.MaxPoints = 100
	}
	if cfg.Adaptive.MaxPasses <= 0 {
		cfg.Adaptive.MaxPasses = 4
	}
	if cfg.Adaptive.RefineDBThreshold <= 0 {
		cfg.Adaptive.RefineDBThreshold = 2.0
	}
	if cfg.Adaptive.UnityGainTarget <= 0 {
		cfg.Adaptive.UnityGainTarget = 1.0
	}
	if cfg.Adaptive.UnityTolRatio <= 0 {
		cfg.Adaptive.UnityTolRatio = 0.03
	}
	if cfg.Adaptive.UnityMaxIter <= 0 {
		cfg.Adaptive.UnityMaxIter = 8
	}

	return &cfg, nil
}

func logspace(start, stop float64, n int) []float64 {
	if n <= 0 {
		return nil
	}
	if n == 1 {
		return []float64{start}
	}

	result := make([]float64, n)
	logStart := math.Log10(start)
	logStop := math.Log10(stop)
	step := (logStop - logStart) / float64(n-1)

	for i := 0; i < n; i++ {
		result[i] = math.Pow(10, logStart+float64(i)*step)
	}
	return result
}

func gainDB(g float64) float64 {
	if g <= 0 {
		return math.NaN()
	}
	return 20 * math.Log10(g)
}

func hasPointNear(points []Point, f float64) bool {
	for _, p := range points {
		if math.Abs(p.FreqHz-f)/f < 1e-6 {
			return true
		}
	}
	return false
}

func sortPoints(points []Point) {
	sort.Slice(points, func(i, j int) bool {
		return points[i].FreqHz < points[j].FreqHz
	})
}

func measurePoint(bridge *BridgeClient, cfg *Config, f float64, pointType string) Point {
	freqCmd := fmt.Sprintf(cfg.SCPI.SetFrequency, f)
	if err := bridge.Write("gen", freqCmd); err != nil {
		fmt.Printf("generator write error at %.2f Hz: %v\n", f, err)
		return Point{FreqHz: f, OK: false, PointType: pointType}
	}

	time.Sleep(time.Duration(cfg.Experiment.SettleMS) * time.Millisecond)

	var lastErr error
	for attempt := 1; attempt <= cfg.Experiment.MeasureRetries; attempt++ {
		vin, vinRaw, err := bridge.QueryNumber(
			"osc",
			cfg.SCPI.MeasureVin,
			cfg.Experiment.ReadMode,
			cfg.Experiment.ReadDelayMS,
			cfg.Experiment.ReadMaxBytes,
		)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(cfg.Experiment.RetryDelayMS) * time.Millisecond)
			continue
		}

		vout, voutRaw, err := bridge.QueryNumber(
			"osc",
			cfg.SCPI.MeasureVout,
			cfg.Experiment.ReadMode,
			cfg.Experiment.ReadDelayMS,
			cfg.Experiment.ReadMaxBytes,
		)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(cfg.Experiment.RetryDelayMS) * time.Millisecond)
			continue
		}

		gain := 0.0
		if vin != 0 {
			gain = vout / vin
		}

		fmt.Printf(
			"f=%.2f Hz  Vin=%.6f V  Vout=%.6f V  Gain=%.6f\n",
			f, vin, vout, gain,
		)

		return Point{
			FreqHz:    f,
			Vin:       vin,
			Vout:      vout,
			Gain:      gain,
			VinRaw:    vinRaw,
			VoutRaw:   voutRaw,
			OK:        true,
			PointType: pointType,
		}
	}

	fmt.Printf("measure error at %.2f Hz: %v\n", f, lastErr)
	return Point{FreqHz: f, OK: false, PointType: pointType}
}

func crossesTarget(a, b Point, target float64) bool {
	if !a.OK || !b.OK {
		return false
	}
	return (a.Gain-target)*(b.Gain-target) <= 0
}

func segmentNeedsRefine(a, b Point, acfg AdaptiveConfig) bool {
	if !a.OK || !b.OK {
		return false
	}

	if crossesTarget(a, b, acfg.UnityGainTarget) {
		return true
	}

	db1 := gainDB(a.Gain)
	db2 := gainDB(b.Gain)
	if math.IsNaN(db1) || math.IsNaN(db2) {
		return false
	}

	if math.Abs(db2-db1) >= acfg.RefineDBThreshold {
		return true
	}

	return false
}

func adaptiveSweep(bridge *BridgeClient, cfg *Config) []Point {
	points := make([]Point, 0)

	initialFreqs := logspace(
		cfg.Experiment.FreqStartHz,
		cfg.Experiment.FreqStopHz,
		cfg.Experiment.Points,
	)

	for _, f := range initialFreqs {
		points = append(points, measurePoint(bridge, cfg, f, "base"))
	}
	sortPoints(points)

	if !cfg.Adaptive.Enabled {
		return points
	}

	for pass := 0; pass < cfg.Adaptive.MaxPasses; pass++ {
		sortPoints(points)
		added := false

		current := make([]Point, len(points))
		copy(current, points)

		for i := 0; i < len(current)-1; i++ {
			if len(points) >= cfg.Adaptive.MaxPoints {
				break
			}

			a := current[i]
			b := current[i+1]

			if !segmentNeedsRefine(a, b, cfg.Adaptive) {
				continue
			}

			mid := math.Sqrt(a.FreqHz * b.FreqHz)

			if mid <= a.FreqHz || mid >= b.FreqHz {
				continue
			}
			if hasPointNear(points, mid) {
				continue
			}

			fmt.Printf("refine between %.2f Hz and %.2f Hz -> %.2f Hz\n", a.FreqHz, b.FreqHz, mid)
			points = append(points, measurePoint(bridge, cfg, mid, "refined"))
			added = true
		}

		if !added {
			break
		}
	}

	sortPoints(points)
	return points
}

func searchUnityGain(bridge *BridgeClient, cfg *Config, left, right Point) (Point, bool) {
	target := cfg.Adaptive.UnityGainTarget

	if !crossesTarget(left, right, target) {
		return Point{}, false
	}

	best := left
	if math.Abs(right.Gain-target) < math.Abs(left.Gain-target) {
		best = right
	}

	for iter := 0; iter < cfg.Adaptive.UnityMaxIter; iter++ {
		ratio := right.FreqHz / left.FreqHz
		if ratio <= 1.0+cfg.Adaptive.UnityTolRatio {
			break
		}

		midFreq := math.Sqrt(left.FreqHz * right.FreqHz)
		mid := measurePoint(bridge, cfg, midFreq, "unity")
		if !mid.OK {
			break
		}

		if math.Abs(mid.Gain-target) < math.Abs(best.Gain-target) {
			best = mid
		}

		if crossesTarget(left, mid, target) {
			right = mid
		} else {
			left = mid
		}
	}

	return best, true
}

func appendUnitySearchPoints(bridge *BridgeClient, cfg *Config, points []Point) []Point {
	if !cfg.Adaptive.Enabled || !cfg.Adaptive.UnitySearchEnable {
		return points
	}

	sortPoints(points)

	for i := 0; i < len(points)-1; i++ {
		a := points[i]
		b := points[i+1]

		if !crossesTarget(a, b, cfg.Adaptive.UnityGainTarget) {
			continue
		}

		fmt.Printf("unity-gain bracket found: %.2f Hz .. %.2f Hz\n", a.FreqHz, b.FreqHz)
		best, ok := searchUnityGain(bridge, cfg, a, b)
		if ok && best.OK && !hasPointNear(points, best.FreqHz) {
			points = append(points, best)
			fmt.Printf("unity gain approx at %.2f Hz, Gain=%.6f\n", best.FreqHz, best.Gain)
		}
		break
	}

	sortPoints(points)
	return points
}

func writeCSV(path string, points []Point) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write([]string{
		"frequency_hz",
		"vin_vpp",
		"vout_vpp",
		"gain",
		"gain_db",
		"ok",
		"point_type",
		"vin_raw",
		"vout_raw",
	}); err != nil {
		return err
	}

	for _, p := range points {
		gdb := ""
		if p.OK && p.Gain > 0 {
			gdb = fmt.Sprintf("%.9f", gainDB(p.Gain))
		}

		row := []string{
			fmt.Sprintf("%.6f", p.FreqHz),
			fmt.Sprintf("%.9f", p.Vin),
			fmt.Sprintf("%.9f", p.Vout),
			fmt.Sprintf("%.9f", p.Gain),
			gdb,
			fmt.Sprintf("%t", p.OK),
			p.PointType,
			p.VinRaw,
			p.VoutRaw,
		}
		if err := writer.Write(row); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	cfg, err := loadConfig("config.yaml")
	if err != nil {
		panic(err)
	}

	bridge := NewBridgeClient(cfg.BridgeURL)

	if err := bridge.Connect("gen", cfg.Devices.Gen); err != nil {
		panic(err)
	}
	if err := bridge.Connect("osc", cfg.Devices.Osc); err != nil {
		panic(err)
	}
	defer bridge.CloseAll()

	for _, cmd := range cfg.SCPI.GeneratorInit {
		if err := bridge.Write("gen", cmd); err != nil {
			panic(err)
		}
	}

	var points []Point

	if cfg.UnitySearch.Enabled {
		points = searchUnityGainByIncreasingFrequency(bridge, cfg)
	} else {
		points = adaptiveSweep(bridge, cfg)
		points = appendUnitySearchPoints(bridge, cfg, points)
	}

	sortPoints(points)

	if err := writeCSV("results.csv", points); err != nil {
		panic(err)
	}

	fmt.Println("Done. Results saved to results.csv")

	foundUnity := false
	for i := 0; i < len(points)-1; i++ {
		if crossesTarget(points[i], points[i+1], cfg.Adaptive.UnityGainTarget) {
			fmt.Printf(
				"Unity gain is between %.2f Hz and %.2f Hz\n",
				points[i].FreqHz,
				points[i+1].FreqHz,
			)
			foundUnity = true
			break
		}
	}
	if !foundUnity {
		fmt.Println("Unity gain crossing was not found in current frequency range.")
	}
}
