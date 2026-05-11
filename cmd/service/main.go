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
	"strings"
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
	GeneratorInit    []string `yaml:"generator_init"`
	OscilloscopeInit []string `yaml:"oscilloscope_init"`
	SetFrequency     string   `yaml:"set_frequency"`
	MeasureVin       string   `yaml:"measure_vin"`
	MeasureVout      string   `yaml:"measure_vout"`
}

type OscAutoScaleConfig struct {
	Enabled                 bool     `yaml:"enabled"`
	Commands                []string `yaml:"commands"`
	RunAfterFrequencyChange bool     `yaml:"run_after_frequency_change"`
	SettleMS                int      `yaml:"settle_ms"`
	FailOnError             bool     `yaml:"fail_on_error"`
}

type OscManualScaleConfig struct {
	Enabled         bool     `yaml:"enabled"`
	Scales          []string `yaml:"scales"`
	CH1InitialScale string   `yaml:"ch1_initial_scale"`
	CH2InitialScale string   `yaml:"ch2_initial_scale"`
	TargetDivisions float64  `yaml:"target_divisions"`
	MinDivisions    float64  `yaml:"min_divisions"`
	MaxAdjustments  int      `yaml:"max_adjustments"`
	SettleMS        int      `yaml:"settle_ms"`
	ch1ScaleV       float64
	ch2ScaleV       float64
}

type OscTimeScaleConfig struct {
	Enabled         bool     `yaml:"enabled"`
	Scales          []string `yaml:"scales"`
	PeriodsOnScreen float64  `yaml:"periods_on_screen"`
	SettleMS        int      `yaml:"settle_ms"`
	currentScaleS   float64
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
	BridgeURL      string               `yaml:"bridge_url"`
	Devices        DevicesConfig        `yaml:"devices"`
	SCPI           SCPIConfig           `yaml:"scpi"`
	Experiment     ExperimentConfig     `yaml:"experiment"`
	OscAutoScale   OscAutoScaleConfig   `yaml:"osc_autoscale"`
	OscManualScale OscManualScaleConfig `yaml:"osc_manual_scale"`
	OscTimeScale   OscTimeScaleConfig   `yaml:"osc_time_scale"`
	Adaptive       AdaptiveConfig       `yaml:"adaptive"`
	UnitySearch    UnitySearchConfig    `yaml:"unity_search"`
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
	Enabled         bool    `yaml:"enabled"`
	StartHz         float64 `yaml:"start_hz"`
	MaxHz           float64 `yaml:"max_hz"`
	HardMaxHz       float64 `yaml:"hard_max_hz"`
	Scale           string  `yaml:"scale"`
	Points          int     `yaml:"points"`
	PointsPerDecade int     `yaml:"points_per_decade"`
	MaxExtendPoints int     `yaml:"max_extend_points"`
	FreqMultiplier  float64 `yaml:"freq_multiplier"`
	TargetGain      float64 `yaml:"target_gain"`
	Tolerance       float64 `yaml:"tolerance"`
	MaxRefineIter   int     `yaml:"max_refine_iter"`
}

func NewBridgeClient(baseURL string) *BridgeClient {
	return &BridgeClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 20 * time.Second,
		},
	}
}

func normalizedUnityTarget(cfg *Config) float64 {
	target := cfg.UnitySearch.TargetGain
	if target <= 0 {
		target = cfg.Adaptive.UnityGainTarget
	}
	if target <= 0 {
		target = 1.0
	}
	return target
}

func normalizePlanScale(scale string) string {
	if scale == "linear" {
		return "linear"
	}
	return "log"
}

func codedFrequency(x, start, stop float64, scale string) float64 {
	scale = normalizePlanScale(scale)
	if scale == "linear" {
		x0 := (start + stop) / 2
		dx := (stop - start) / 2
		return x0 + x*dx
	}

	logStart := math.Log10(start)
	logStop := math.Log10(stop)
	x0 := (logStart + logStop) / 2
	dx := (logStop - logStart) / 2
	return math.Pow(10, x0+x*dx)
}

func theoryPlanFrequencies(start, stop float64, points int, scale string) []float64 {
	if points <= 0 || start <= 0 || stop <= 0 || stop < start {
		return nil
	}
	if points == 1 {
		return []float64{start}
	}

	result := make([]float64, 0, points)
	for i := 0; i < points; i++ {
		codedX := -1.0 + 2.0*float64(i)/float64(points-1)
		result = append(result, codedFrequency(codedX, start, stop, scale))
	}

	return result
}

func unityPlanPointCount(cfg *Config, start, stop float64) int {
	if cfg.UnitySearch.Points > 1 {
		return cfg.UnitySearch.Points
	}

	pointsPerDecade := cfg.UnitySearch.PointsPerDecade
	if pointsPerDecade <= 0 {
		pointsPerDecade = 12
	}

	decades := math.Abs(math.Log10(stop / start))
	points := int(math.Ceil(decades*float64(pointsPerDecade))) + 1
	if points < 2 {
		points = 2
	}
	return points
}

func nextPlannedFrequency(prev, start, stop float64, points int, scale string) float64 {
	if prev <= 0 || start <= 0 || stop <= 0 || points <= 1 {
		return 0
	}

	scale = normalizePlanScale(scale)
	if scale == "linear" {
		step := (stop - start) / float64(points-1)
		return prev + step
	}

	ratio := math.Pow(stop/start, 1.0/float64(points-1))
	return prev * ratio
}

func searchUnityGainByIncreasingFrequency(
	bridge *BridgeClient,
	cfg *Config,
) []Point {
	points := make([]Point, 0)

	startHz := cfg.UnitySearch.StartHz
	maxHz := cfg.UnitySearch.MaxHz
	hardMaxHz := cfg.UnitySearch.HardMaxHz
	scale := normalizePlanScale(cfg.UnitySearch.Scale)
	target := normalizedUnityTarget(cfg)
	maxExtendPoints := cfg.UnitySearch.MaxExtendPoints

	if startHz <= 0 {
		startHz = cfg.Experiment.FreqStartHz
	}

	if maxHz <= 0 {
		maxHz = cfg.Experiment.FreqStopHz
	}

	if hardMaxHz <= maxHz {
		hardMaxHz = maxHz * 10
	}

	if maxExtendPoints <= 0 {
		maxExtendPoints = 40
	}

	planPoints := unityPlanPointCount(cfg, startHz, maxHz)
	baseFreqs := theoryPlanFrequencies(startHz, maxHz, planPoints, scale)

	fmt.Println("Starting unity gain search by coded one-factor plan...")
	fmt.Printf("start=%.2f Hz, plan_max=%.2f Hz, hard_max=%.2f Hz, scale=%s, points=%d, target=%.3f\n",
		startHz, maxHz, hardMaxHz, scale, len(baseFreqs), target,
	)

	if len(baseFreqs) == 0 {
		fmt.Println("Unity gain search has no planned frequencies")
		return points
	}

	first := measurePoint(bridge, cfg, baseFreqs[0], "base")
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

	foundBracket := false
	for _, f := range baseFreqs[1:] {
		curr := measurePoint(bridge, cfg, f, "base")
		points = append(points, curr)

		if !curr.OK {
			fmt.Printf("Measurement failed at %.2f Hz, continue...\n", f)
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

			foundBracket = true
			break
		}

		prev = curr
	}

	if !foundBracket && prev.OK && prev.Gain > target {
		fmt.Printf(
			"Unity gain was not found up to planned %.2f Hz. Last gain=%.6f; extending search...\n",
			maxHz,
			prev.Gain,
		)

		f := nextPlannedFrequency(prev.FreqHz, startHz, maxHz, planPoints, scale)
		for i := 0; i < maxExtendPoints && f > prev.FreqHz && f <= hardMaxHz; i++ {
			curr := measurePoint(bridge, cfg, f, "extended")
			points = append(points, curr)

			if !curr.OK {
				fmt.Printf("Measurement failed at %.2f Hz, continue...\n", f)
				f = nextPlannedFrequency(f, startHz, maxHz, planPoints, scale)
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

				foundBracket = true
				break
			}

			prev = curr
			f = nextPlannedFrequency(f, startHz, maxHz, planPoints, scale)
		}

		if !foundBracket {
			fmt.Printf(
				"Unity gain was not found up to hard limit %.2f Hz. Last gain=%.6f\n",
				hardMaxHz,
				prev.Gain,
			)
		}
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

	target := normalizedUnityTarget(cfg)
	tolerance := cfg.UnitySearch.Tolerance
	maxIter := cfg.UnitySearch.MaxRefineIter

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
	if cfg.OscAutoScale.Enabled {
		if len(cfg.OscAutoScale.Commands) == 0 {
			cfg.OscAutoScale.Commands = []string{":AUTOset ON"}
		}
		if cfg.OscAutoScale.SettleMS <= 0 {
			cfg.OscAutoScale.SettleMS = 2500
		}
	}
	if cfg.OscManualScale.Enabled {
		if len(cfg.OscManualScale.Scales) == 0 {
			cfg.OscManualScale.Scales = []string{
				"1mv", "2mv", "5mv",
				"10mv", "20mv", "50mv",
				"100mv", "200mv", "500mv",
				"1v", "2v", "5v", "10v",
			}
		}
		if cfg.OscManualScale.CH1InitialScale == "" {
			cfg.OscManualScale.CH1InitialScale = "20mv"
		}
		if cfg.OscManualScale.CH2InitialScale == "" {
			cfg.OscManualScale.CH2InitialScale = "500mv"
		}
		if cfg.OscManualScale.TargetDivisions <= 0 {
			cfg.OscManualScale.TargetDivisions = 5.0
		}
		if cfg.OscManualScale.MinDivisions <= 0 {
			cfg.OscManualScale.MinDivisions = 1.5
		}
		if cfg.OscManualScale.MaxAdjustments <= 0 {
			cfg.OscManualScale.MaxAdjustments = 3
		}
		if cfg.OscManualScale.SettleMS <= 0 {
			cfg.OscManualScale.SettleMS = 500
		}
		ch1Scale, err := parseVoltScale(cfg.OscManualScale.CH1InitialScale)
		if err != nil {
			return nil, err
		}
		ch2Scale, err := parseVoltScale(cfg.OscManualScale.CH2InitialScale)
		if err != nil {
			return nil, err
		}
		cfg.OscManualScale.ch1ScaleV = ch1Scale
		cfg.OscManualScale.ch2ScaleV = ch2Scale
	}
	if cfg.OscTimeScale.Enabled {
		if len(cfg.OscTimeScale.Scales) == 0 {
			cfg.OscTimeScale.Scales = []string{
				"1ns", "2ns", "5ns",
				"10ns", "20ns", "50ns",
				"100ns", "200ns", "500ns",
				"1us", "2us", "5us",
				"10us", "20us", "50us",
				"100us", "200us", "500us",
				"1ms", "2ms", "5ms",
				"10ms", "20ms", "50ms",
				"100ms", "200ms", "500ms",
				"1s",
			}
		}
		if cfg.OscTimeScale.PeriodsOnScreen <= 0 {
			cfg.OscTimeScale.PeriodsOnScreen = 3.0
		}
		if cfg.OscTimeScale.SettleMS <= 0 {
			cfg.OscTimeScale.SettleMS = 200
		}
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
	if cfg.UnitySearch.Scale == "" {
		cfg.UnitySearch.Scale = "log"
	}
	if cfg.UnitySearch.PointsPerDecade <= 0 {
		cfg.UnitySearch.PointsPerDecade = 12
	}
	if cfg.UnitySearch.MaxExtendPoints <= 0 {
		cfg.UnitySearch.MaxExtendPoints = 40
	}
	if cfg.UnitySearch.TargetGain <= 0 {
		cfg.UnitySearch.TargetGain = cfg.Adaptive.UnityGainTarget
	}
	if cfg.UnitySearch.Tolerance <= 0 {
		cfg.UnitySearch.Tolerance = 0.03
	}
	if cfg.UnitySearch.MaxRefineIter <= 0 {
		cfg.UnitySearch.MaxRefineIter = 10
	}
	if cfg.UnitySearch.MaxHz <= 0 {
		cfg.UnitySearch.MaxHz = cfg.Experiment.FreqStopHz
	}
	if cfg.UnitySearch.HardMaxHz <= cfg.UnitySearch.MaxHz {
		cfg.UnitySearch.HardMaxHz = cfg.UnitySearch.MaxHz * 10
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

func parseVoltScale(scale string) (float64, error) {
	normalized := strings.ToLower(strings.TrimSpace(scale))
	normalized = strings.ReplaceAll(normalized, " ", "")
	if normalized == "" {
		return 0, fmt.Errorf("empty oscilloscope scale")
	}

	multiplier := 1.0
	numberText := normalized
	switch {
	case strings.HasSuffix(normalized, "mv"):
		multiplier = 1e-3
		numberText = strings.TrimSuffix(normalized, "mv")
	case strings.HasSuffix(normalized, "v"):
		numberText = strings.TrimSuffix(normalized, "v")
	default:
		return 0, fmt.Errorf("unsupported oscilloscope scale %q", scale)
	}

	var value float64
	if _, err := fmt.Sscanf(numberText, "%f", &value); err != nil {
		return 0, fmt.Errorf("bad oscilloscope scale %q: %w", scale, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("bad oscilloscope scale %q", scale)
	}

	return value * multiplier, nil
}

func parseTimeScale(scale string) (float64, error) {
	normalized := strings.ToLower(strings.TrimSpace(scale))
	normalized = strings.ReplaceAll(normalized, " ", "")
	if normalized == "" {
		return 0, fmt.Errorf("empty oscilloscope time scale")
	}

	multiplier := 1.0
	numberText := normalized
	switch {
	case strings.HasSuffix(normalized, "ns"):
		multiplier = 1e-9
		numberText = strings.TrimSuffix(normalized, "ns")
	case strings.HasSuffix(normalized, "us"):
		multiplier = 1e-6
		numberText = strings.TrimSuffix(normalized, "us")
	case strings.HasSuffix(normalized, "ms"):
		multiplier = 1e-3
		numberText = strings.TrimSuffix(normalized, "ms")
	case strings.HasSuffix(normalized, "s"):
		numberText = strings.TrimSuffix(normalized, "s")
	default:
		return 0, fmt.Errorf("unsupported oscilloscope time scale %q", scale)
	}

	var value float64
	if _, err := fmt.Sscanf(numberText, "%f", &value); err != nil {
		return 0, fmt.Errorf("bad oscilloscope time scale %q: %w", scale, err)
	}
	if value <= 0 {
		return 0, fmt.Errorf("bad oscilloscope time scale %q", scale)
	}

	return value * multiplier, nil
}

func formatScaleCommand(channel int, scale string) string {
	return fmt.Sprintf(":CH%d:SCALe %s", channel, scale)
}

func formatTimeScaleCommand(scale string) string {
	return fmt.Sprintf(":HORIzontal:SCALe %s", scale)
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

func runOscAutoScale(bridge *BridgeClient, cfg *Config, f float64) error {
	acfg := cfg.OscAutoScale
	if !acfg.Enabled || !acfg.RunAfterFrequencyChange {
		return nil
	}

	for _, cmd := range acfg.Commands {
		if err := bridge.Write("osc", cmd); err != nil {
			if acfg.FailOnError {
				return fmt.Errorf("oscilloscope autoscale error at %.2f Hz, cmd %q: %w", f, cmd, err)
			}
			fmt.Printf("oscilloscope autoscale warning at %.2f Hz, cmd %q: %v\n", f, cmd, err)
			return nil
		}
	}

	time.Sleep(time.Duration(acfg.SettleMS) * time.Millisecond)
	return nil
}

func chooseManualScale(scales []string, targetScaleV float64) (string, float64, bool) {
	bestScale := ""
	bestScaleV := 0.0

	for _, scale := range scales {
		scaleV, err := parseVoltScale(scale)
		if err != nil {
			continue
		}
		if scaleV >= targetScaleV {
			return scale, scaleV, true
		}
		if bestScale == "" || scaleV > bestScaleV {
			bestScale = scale
			bestScaleV = scaleV
		}
	}

	return bestScale, bestScaleV, bestScale != ""
}

func nextLargerManualScale(scales []string, currentScaleV float64) (string, float64, bool) {
	bestScale := ""
	bestScaleV := 0.0

	for _, scale := range scales {
		scaleV, err := parseVoltScale(scale)
		if err != nil {
			continue
		}
		if scaleV > currentScaleV*1.01 {
			if bestScale == "" || scaleV < bestScaleV {
				bestScale = scale
				bestScaleV = scaleV
			}
		}
	}

	return bestScale, bestScaleV, bestScale != ""
}

func measurementQuestionable(raw string) bool {
	return strings.Contains(raw, "?") || strings.Contains(strings.ToUpper(raw), "OFF")
}

func chooseTimeScale(scales []string, targetScaleS float64) (string, float64, bool) {
	bestScale := ""
	bestScaleS := 0.0

	for _, scale := range scales {
		scaleS, err := parseTimeScale(scale)
		if err != nil {
			continue
		}
		if scaleS >= targetScaleS {
			return scale, scaleS, true
		}
		if bestScale == "" || scaleS > bestScaleS {
			bestScale = scale
			bestScaleS = scaleS
		}
	}

	return bestScale, bestScaleS, bestScale != ""
}

func adjustTimeScaleForFrequency(bridge *BridgeClient, cfg *Config, f float64) error {
	tcfg := cfg.OscTimeScale
	if !tcfg.Enabled || f <= 0 {
		return nil
	}

	targetScaleS := tcfg.PeriodsOnScreen / (f * 10.0)
	scale, scaleS, ok := chooseTimeScale(tcfg.Scales, targetScaleS)
	if !ok || scaleS <= 0 {
		return nil
	}
	if tcfg.currentScaleS > 0 && math.Abs(scaleS-tcfg.currentScaleS)/tcfg.currentScaleS < 0.01 {
		return nil
	}

	if err := bridge.Write("osc", formatTimeScaleCommand(scale)); err != nil {
		return err
	}
	cfg.OscTimeScale.currentScaleS = scaleS
	fmt.Printf("horizontal scale -> %s for f=%.2f Hz\n", scale, f)
	time.Sleep(time.Duration(tcfg.SettleMS) * time.Millisecond)
	return nil
}

func currentManualScale(cfg *Config, channel int) float64 {
	if channel == 1 {
		return cfg.OscManualScale.ch1ScaleV
	}
	return cfg.OscManualScale.ch2ScaleV
}

func setCurrentManualScale(cfg *Config, channel int, scaleV float64) {
	if channel == 1 {
		cfg.OscManualScale.ch1ScaleV = scaleV
		return
	}
	cfg.OscManualScale.ch2ScaleV = scaleV
}

func adjustManualScaleForSignal(bridge *BridgeClient, cfg *Config, channel int, vpp float64, raw string) (bool, error) {
	acfg := cfg.OscManualScale
	if !acfg.Enabled {
		return false, nil
	}

	currentScaleV := currentManualScale(cfg, channel)
	if currentScaleV <= 0 {
		return false, nil
	}

	if measurementQuestionable(raw) {
		scale, scaleV, ok := nextLargerManualScale(acfg.Scales, currentScaleV)
		if !ok {
			return false, nil
		}
		if err := bridge.Write("osc", formatScaleCommand(channel, scale)); err != nil {
			return false, err
		}
		setCurrentManualScale(cfg, channel, scaleV)
		fmt.Printf("CH%d scale -> %s because measurement is questionable: %q\n", channel, scale, strings.TrimSpace(raw))
		return true, nil
	}

	if vpp <= 0 {
		return false, nil
	}

	divisions := vpp / currentScaleV
	if divisions <= acfg.TargetDivisions {
		return false, nil
	}

	targetScaleV := vpp / acfg.TargetDivisions
	scale, scaleV, ok := chooseManualScale(acfg.Scales, targetScaleV)
	if !ok || scaleV <= 0 {
		return false, nil
	}
	if math.Abs(scaleV-currentScaleV)/currentScaleV < 0.01 {
		return false, nil
	}

	if err := bridge.Write("osc", formatScaleCommand(channel, scale)); err != nil {
		return false, err
	}
	setCurrentManualScale(cfg, channel, scaleV)
	fmt.Printf("CH%d scale -> %s for Vpp=%.6f V\n", channel, scale, vpp)
	return true, nil
}

func initManualOscScale(bridge *BridgeClient, cfg *Config) error {
	acfg := cfg.OscManualScale
	if !acfg.Enabled {
		return nil
	}

	if err := bridge.Write("osc", formatScaleCommand(1, acfg.CH1InitialScale)); err != nil {
		return err
	}
	if err := bridge.Write("osc", formatScaleCommand(2, acfg.CH2InitialScale)); err != nil {
		return err
	}
	time.Sleep(time.Duration(acfg.SettleMS) * time.Millisecond)
	return nil
}

func readMeasuredPoint(bridge *BridgeClient, cfg *Config, f float64, pointType string) (Point, error) {
	vin, vinRaw, err := bridge.QueryNumber(
		"osc",
		cfg.SCPI.MeasureVin,
		cfg.Experiment.ReadMode,
		cfg.Experiment.ReadDelayMS,
		cfg.Experiment.ReadMaxBytes,
	)
	if err != nil {
		return Point{FreqHz: f, OK: false, PointType: pointType}, err
	}

	vout, voutRaw, err := bridge.QueryNumber(
		"osc",
		cfg.SCPI.MeasureVout,
		cfg.Experiment.ReadMode,
		cfg.Experiment.ReadDelayMS,
		cfg.Experiment.ReadMaxBytes,
	)
	if err != nil {
		return Point{FreqHz: f, OK: false, PointType: pointType}, err
	}

	gain := 0.0
	if vin != 0 {
		gain = vout / vin
	}

	return Point{
		FreqHz:    f,
		Vin:       vin,
		Vout:      vout,
		Gain:      gain,
		VinRaw:    vinRaw,
		VoutRaw:   voutRaw,
		OK:        true,
		PointType: pointType,
	}, nil
}

func measurePoint(bridge *BridgeClient, cfg *Config, f float64, pointType string) Point {
	freqCmd := fmt.Sprintf(cfg.SCPI.SetFrequency, f)
	if err := bridge.Write("gen", freqCmd); err != nil {
		fmt.Printf("generator write error at %.2f Hz: %v\n", f, err)
		return Point{FreqHz: f, OK: false, PointType: pointType}
	}

	if err := adjustTimeScaleForFrequency(bridge, cfg, f); err != nil {
		fmt.Printf("horizontal scale error at %.2f Hz: %v\n", f, err)
		return Point{FreqHz: f, OK: false, PointType: pointType}
	}

	time.Sleep(time.Duration(cfg.Experiment.SettleMS) * time.Millisecond)

	if err := runOscAutoScale(bridge, cfg, f); err != nil {
		fmt.Println(err)
		return Point{FreqHz: f, OK: false, PointType: pointType}
	}

	var lastErr error
	maxAttempts := cfg.Experiment.MeasureRetries
	if cfg.OscManualScale.Enabled && maxAttempts < cfg.OscManualScale.MaxAdjustments+1 {
		maxAttempts = cfg.OscManualScale.MaxAdjustments + 1
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		point, err := readMeasuredPoint(bridge, cfg, f, pointType)
		if err != nil {
			lastErr = err
			time.Sleep(time.Duration(cfg.Experiment.RetryDelayMS) * time.Millisecond)
			continue
		}

		if cfg.OscManualScale.Enabled {
			adjusted1, err := adjustManualScaleForSignal(bridge, cfg, 1, point.Vin, point.VinRaw)
			if err != nil {
				lastErr = err
				time.Sleep(time.Duration(cfg.Experiment.RetryDelayMS) * time.Millisecond)
				continue
			}
			adjusted2, err := adjustManualScaleForSignal(bridge, cfg, 2, point.Vout, point.VoutRaw)
			if err != nil {
				lastErr = err
				time.Sleep(time.Duration(cfg.Experiment.RetryDelayMS) * time.Millisecond)
				continue
			}
			if (adjusted1 || adjusted2) && attempt <= cfg.OscManualScale.MaxAdjustments {
				time.Sleep(time.Duration(cfg.OscManualScale.SettleMS) * time.Millisecond)
				continue
			}
		}

		fmt.Printf(
			"f=%.2f Hz  Vin=%.6f V  Vout=%.6f V  Gain=%.6f\n",
			f, point.Vin, point.Vout, point.Gain,
		)

		return point
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

	initialFreqs := theoryPlanFrequencies(
		cfg.Experiment.FreqStartHz,
		cfg.Experiment.FreqStopHz,
		cfg.Experiment.Points,
		"log",
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

func reportUnityCrossing(points []Point, targetGain float64) {
	foundUnity := false
	for i := 0; i < len(points)-1; i++ {
		if crossesTarget(points[i], points[i+1], targetGain) {
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

	for _, cmd := range cfg.SCPI.OscilloscopeInit {
		if err := bridge.Write("osc", cmd); err != nil {
			panic(err)
		}
	}
	if err := initManualOscScale(bridge, cfg); err != nil {
		panic(err)
	}

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

	targetGain := cfg.Adaptive.UnityGainTarget
	if cfg.UnitySearch.Enabled {
		targetGain = normalizedUnityTarget(cfg)
	}
	reportUnityCrossing(points, targetGain)
}
