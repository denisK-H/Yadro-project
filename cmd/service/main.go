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
	"time"

	"gopkg.in/yaml.v3"
)

type DeviceConfig struct {
	Resource         string `yaml:"resource"`
	TimeoutMS        int    `yaml:"timeout_ms"`
	WriteTermination string `yaml:"write_termination"`
	ReadTermination  string `yaml:"read_termination"`
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
	FreqStartHz float64 `yaml:"freq_start_hz"`
	FreqStopHz  float64 `yaml:"freq_stop_hz"`
	Points      int     `yaml:"points"`
	SettleMS    int     `yaml:"settle_ms"`

	ReadMode    string `yaml:"read_mode"`
	ReadDelayMS int    `yaml:"read_delay_ms"`
	ReadMaxBytes int   `yaml:"read_max_bytes"`
}

type Config struct {
	BridgeURL  string           `yaml:"bridge_url"`
	Devices    DevicesConfig    `yaml:"devices"`
	SCPI       SCPIConfig       `yaml:"scpi"`
	Experiment ExperimentConfig `yaml:"experiment"`
}

type BridgeClient struct {
	BaseURL string
	Client  *http.Client
}

func NewBridgeClient(baseURL string) *BridgeClient {
	return &BridgeClient{
		BaseURL: baseURL,
		Client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
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

func (b *BridgeClient) QueryNumber(device, cmd, mode string, delayMS, maxBytes int) (float64, error) {
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
		return 0, err
	}

	return resp.Value, nil
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

	freqs := logspace(cfg.Experiment.FreqStartHz, cfg.Experiment.FreqStopHz, cfg.Experiment.Points)

	file, err := os.Create("results.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	_ = writer.Write([]string{"frequency_hz", "vin_vpp", "vout_vpp", "gain"})

	for _, f := range freqs {
		freqCmd := fmt.Sprintf(cfg.SCPI.SetFrequency, f)
		if err := bridge.Write("gen", freqCmd); err != nil {
			panic(err)
		}

		time.Sleep(time.Duration(cfg.Experiment.SettleMS) * time.Millisecond)

		vin, err := bridge.QueryNumber(
			"osc",
			cfg.SCPI.MeasureVin,
			cfg.Experiment.ReadMode,
			cfg.Experiment.ReadDelayMS,
			cfg.Experiment.ReadMaxBytes,
		)
		if err != nil {
			fmt.Printf("vin read error at %.2f Hz: %v\n", f, err)
			continue
		}

		vout, err := bridge.QueryNumber(
			"osc",
			cfg.SCPI.MeasureVout,
			cfg.Experiment.ReadMode,
			cfg.Experiment.ReadDelayMS,
			cfg.Experiment.ReadMaxBytes,
		)
		if err != nil {
			fmt.Printf("vout read error at %.2f Hz: %v\n", f, err)
			continue
		}

		gain := 0.0
		if vin != 0 {
			gain = vout / vin
		}

		fmt.Printf("f=%.2f Hz  Vin=%.6f  Vout=%.6f  Gain=%.6f\n", f, vin, vout, gain)

		_ = writer.Write([]string{
			fmt.Sprintf("%.6f", f),
			fmt.Sprintf("%.6f", vin),
			fmt.Sprintf("%.6f", vout),
			fmt.Sprintf("%.6f", gain),
		})
	}

	fmt.Println("Done. Results saved to results.csv")
}