package handler

import (
	"fullerite/metric"
        "fullerite/util"

	"bytes"
	"fmt"
	l "github.com/Sirupsen/logrus"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func init() {
	RegisterHandler("Wavefront", newWavefront)
}

// Wavefront handler
type Wavefront struct {
	BaseHandler
	endpoint string
	apiKey   string
        proxyServer string
        port string
        proxyFlag bool
}

type wavefrontPayload struct {
	Series []wavefrontMetric
}

type wavefrontMetric struct {
	Name      string
	Value     float64
	Source    string
	PointTags []string
}

var allowedValuePuncts = []rune{}
var allowedKeyPuncts = []rune{'-', '_', '.'}
var pointTagLength = 255
var sourceLength = 1023

// newWavefront returns a new Wavefront handler
func newWavefront(
        channel chan metric.Metric,
        initialInterval int,
        initialBufferSize int,
        initialTimeout time.Duration,
        log *l.Entry) Handler {

	inst := new(Wavefront)
	inst.name = "Wavefront"

	inst.timeout = initialTimeout
	inst.log = log
	inst.channel = channel
	return inst
}


func (w Wavefront) wavefrontValueSanitize(value string) string {
        value = strings.Trim(value, "_")
        return util.StrSanitize(value, true, allowedValuePuncts)
}

func (w Wavefront) wavefrontKeySanitize(key string) string {
        return util.StrSanitize(key, false, allowedKeyPuncts)
}

func (w Wavefront) wavefrontPointTagSanitize(pointTag string) string {
	if len(pointTag) > pointTagLength {
		runes := []rune(pointTag)
		w.log.Warn("Truncating point tag: \""+pointTag+"\". The maximum allowed length for a combination of a point tag key and value is 255 characters including =")
		truncatedPointTag := string(runes[:pointTagLength])
		
		//check if last last character is \
		if strings.HasSuffix(truncatedPointTag, "\\") {
			cleanTruncatedPointTag := strings.Trim(truncatedPointTag, "\\")
			return cleanTruncatedPointTag
		} 

                return truncatedPointTag
	}
        return pointTag
}

func (w Wavefront) wavefrontMetricNameSanitize(metricName string) string {
        return util.StrSanitize(metricName, false, allowedKeyPuncts)
}

func (w Wavefront) wavefrontSourceSanitize(source string) string {
	sanitizedSource :=  util.StrSanitize(source, false, allowedKeyPuncts)
        if len(sanitizedSource) > sourceLength {
        	runes := []rune(sanitizedSource)
		truncatedSource := string(runes[:sourceLength]) 
		w.log.Warn("Truncating source field. The length of the source field should be less than 1024 characters")	
		return truncatedSource
	}	
        return sanitizedSource
}

// Configure the Wavefront handler
func (w *Wavefront) Configure(configMap map[string]interface{}) {
        if proxyFlag, exists := configMap["proxyFlag"]; exists {
                proxyFlag, err := strconv.ParseBool(proxyFlag.(string))
                if err != nil {
                	w.log.Error("proxyFlag should be true or false for the Wavefront handler, there won't be any emissions")
                } else if proxyFlag {
                        w.proxyFlag = proxyFlag
                	w.configureForProxyIngestion(configMap)
                } else {
                        w.configureForDirectIngestion(configMap)
		}
        } else {
		w.log.Error("There was no proxyFlag specified for the Wavefront handler, there won't be any emissions")
	}

	w.configureCommonParams(configMap)
}

// Configure the Wavefront Handler for Direct Ingestion
func (w *Wavefront) configureForDirectIngestion(configMap map[string]interface{}){
	if apiKey, exists := configMap["apiKey"]; exists {
                w.apiKey = apiKey.(string)
        } else {
                w.log.Error("There was no API key specified for the Wavefront handler, there won't be any emissions")
        }

        if endpoint, exists := configMap["endpoint"]; exists {
                w.endpoint = endpoint.(string)
        } else {
                w.log.Error("There was no endpoint specified for the Wavefront Handler, there won't be any emissions")
        }

}

// Configure the Wavefront Handler for ingestion through Proxy
func (w *Wavefront) configureForProxyIngestion(configMap map[string]interface{}){
        if proxyServer, exists := configMap["proxyServer"]; exists {
                w.proxyServer = proxyServer.(string)
        } else {
                w.log.Error("There was no Proxy server address specified for the Wavefront handler, there won't be any emissions")
        }

        if port, exists := configMap["port"]; exists {
                w.port = port.(string)
        } else {
                w.log.Error("There was no Port number specified for the Wavefront handler, there won't be any emissions")
        }
}

// Endpoint returns the Wavefront API endpoint
func (w Wavefront) Endpoint() string {
	return w.endpoint
}

// Run runs the handler main loop
func (w *Wavefront) Run() {
	w.run(w.emitMetrics)
}



func (w *Wavefront) convertToWavefront(incomingMetric metric.Metric) (datapoint wavefrontMetric) {
	wfm := new(wavefrontMetric)
	wfm.Name = w.Prefix() + w.wavefrontMetricNameSanitize(incomingMetric.Name)
	wfm.Value = incomingMetric.Value
	wfm.Source = w.DefaultDimensions()["host"]
	wfm.PointTags = w.getSanitizedDimensions(incomingMetric)

	return *wfm
}

func (w *Wavefront) emitMetrics(metrics []metric.Metric) bool {
	w.log.Info("Starting emission to Wavefront")
	w.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		w.log.Warn("Skipping send because of an empty payload")
		return false
	}

	series := make([]wavefrontMetric, 0, len(metrics))
	for _, m := range metrics {
		series = append(series, w.convertToWavefront(m))
	}

	p := wavefrontPayload{Series: series}
	pStr := w.wavefrontPayloadToString(p)

	if w.proxyFlag {
		return w.emitMetricsToProxy(metrics, pStr, len(series))
	}
        return w.emitMetricsForDirectIngestion(metrics, pStr, len(series))
}

func (w Wavefront) emitMetricsToProxy(metrics []metric.Metric, pStr string, nDataPoints int) bool {
        addr := fmt.Sprintf("%s:%s", w.proxyServer, w.port)
        conn, err := w.dialTimeout("tcp", addr)
        if err != nil {
            w.log.Error("Failed to connect ", addr)
            return false
        }
        conn.Write([]byte(pStr))
        w.log.Info("Successfully sent ", nDataPoints, " datapoints to Wavefront")
        return true
}

func (w Wavefront) emitMetricsForDirectIngestion(metrics []metric.Metric, pStr string, nDataPoints int) bool {
	apiURL := fmt.Sprintf("%s", w.endpoint)
        req, err := http.NewRequest("POST", apiURL, bytes.NewBufferString(pStr))
	if err != nil {
		w.log.Error("Failed to create a request to endpoint ", w.endpoint)
		return false
	}
	req.Header.Set("Accept", "application/json")
	bearerAPIKey := fmt.Sprintf("Bearer %s", w.apiKey)
	req.Header.Set("Authorization", bearerAPIKey)

	transport := http.Transport{
		Dial: w.dialTimeout,
	}
	client := &http.Client{
		Transport: &transport,
	}
	rsp, err := client.Do(req)
	if err != nil {
		w.log.Error("Failed to complete POST ", err)
		return false
	}

	defer rsp.Body.Close()
	if (rsp.StatusCode == http.StatusOK) || (rsp.StatusCode == http.StatusAccepted) {
		w.log.Info("Successfully sent ", nDataPoints, " datapoints to Wavefront")
		return true
	}

	body, _ := ioutil.ReadAll(rsp.Body)
	w.log.Error("Failed to post to Wavefront @", w.endpoint,
		" status was ", rsp.Status,
		" rsp body was ", string(body),
		" payload was ", string(pStr))
	return false
}

func (w Wavefront) dialTimeout(network, addr string) (net.Conn, error) {
	return net.DialTimeout(network, addr, w.timeout)
}

func (w Wavefront) getSanitizedDimensions(m metric.Metric) (dimensions []string) {
	for name, value := range m.GetDimensions(w.DefaultDimensions()) {
                if name == "host" {
                	continue
		}
                if name == "source" {
			dimensions = append(dimensions, name+"="+w.wavefrontSourceSanitize(value))
		} else {
                        sanitizedName := w.wavefrontKeySanitize(name)
                        sanitizedValue := w.wavefrontValueSanitize(value)
                        pointTags := sanitizedName+"=\""+sanitizedValue+"\""
			sanitizedPointTags := w.wavefrontPointTagSanitize(pointTags)
			dimensions = append(dimensions, sanitizedPointTags)
        	}
	}
	return dimensions
}

func (w Wavefront) wavefrontPayloadToString(p wavefrontPayload) string {
	var payloadBuffer bytes.Buffer
	var pointTagsBuffer bytes.Buffer
	for i, series := range p.Series {
		for _, tagPair := range series.PointTags {
			pointTagsBuffer.WriteString(tagPair + " ")
		}
		payloadBuffer.WriteString(strings.Join([]string{series.Name, " ", strconv.FormatFloat(series.Value, 'f', 2, 64), " source=", series.Source, " ", pointTagsBuffer.String(), "\n"}, ""))
		w.log.Debug("PAYLOAD ", i, ": ", series.Name, " ", series.Value, " source=", series.Source, " ", pointTagsBuffer.String())
		pointTagsBuffer.Reset()
	}
	return payloadBuffer.String()
}
