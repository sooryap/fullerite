package collector

import (
	"fullerite/metric"

	"bufio"
	"os"

	l "github.com/Sirupsen/logrus"
)

const (
	collectorName   = "CpuInfo"
	metricName      = "cpu_info"
	defaultProcPath = "/proc/cpuinfo"
)

type CpuInfo struct {
	baseCollector
	metricName string
	procPath   string
}

func NewCpuInfo(channel chan metric.Metric, initialInterval int, log *l.Entry) *CpuInfo {
	c := new(CpuInfo)
	c.channel = channel
	c.interval = initialInterval
	c.log = log

	c.name = collectorName
	c.metricName = metricName
	c.procPath = defaultProcPath
	return c
}

func (c *CpuInfo) Configure(configMap map[string]interface{}) {
	if procPath, exists := configMap["procPath"]; exists == true {
		c.procPath = procPath
	}
	c.configureCommonParams(configMap)
}

func (c CpuInfo) Collect() {
	value, model, err := c.getCpuInfo()
	if err != nil {
		c.log.Error("Error while collecting metrics", err)
		return
	}
	metric := metric.New(t.metricName)
	metric.Value = value
	metric.AddDimension("model", model)
	c.Channel() <- metric
	c.log.Debug(metric)
}

func (c CpuInfo) getCpuInfo() (int, string, error) {

	// Prepare to read file
	if file, err := os.Open(c.procPath); err != nil {
		c.log.Error("Unable to read file", err)
		return 0, "", err
	}
	defer file.Close()

	// Read file contents and gather metrics
	phys_ids := map[string]bool{}
	model_name := ""
	scanner = bufio.NewScanner(file)
	for scanner.scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "physical id") {
			phys_ids[getValueFromLine(line)] = true
		} else if strings.HasPrefix(line, "model name") {
			val = getValueFromLine(line)
			if model_name == "" {
				model_name = val
			} else if model_name != val {
				model_name = "mixed"
			}
		}
	}

	err = scanner.Err()
	if err != nil {
		c.log.Error("Error while trying to scan through file", err)
	}
	return len(phys_ids), model_name, err
}

func getValueFromLine(line string) string {
	elems := strings.split(line, ":")
	return string.TrimSpace(elems[1])
}
