// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package jmx

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/metricbeat/helper"
	"github.com/elastic/beats/metricbeat/mb"
)

type JMXMapping struct {
	MBean      string
	Attributes []Attribute
	Target     Target
}

type Attribute struct {
	Attr  string
	Field string
	Event string
}

// Target inputs the value you want to set for jolokia target block
type Target struct {
	URL      string
	User     string
	Password string
}

// RequestBlock is used to build the request blocks of the following format:
//
// [
//    {
//       "type":"read",
//       "mbean":"java.lang:type=Runtime",
//       "attribute":[
//          "Uptime"
//       ]
//    },
//    {
//       "type":"read",
//       "mbean":"java.lang:type=GarbageCollector,name=ConcurrentMarkSweep",
//       "attribute":[
//          "CollectionTime",
//          "CollectionCount"
//       ],
//       "target":{
//          "url":"service:jmx:rmi:///jndi/rmi://targethost:9999/jmxrmi",
//          "user":"jolokia",
//          "password":"s!cr!t"
//       }
//    }
// ]
type RequestBlock struct {
	Type      string                 `json:"type"`
	MBean     string                 `json:"mbean"`
	Attribute []string               `json:"attribute"`
	Config    map[string]interface{} `json:"config"`
	Target    *TargetBlock           `json:"target,omitempty"`
}

// TargetBlock is used to build the target blocks of the following format into RequestBlock.
//
// "target":{
//    "url":"service:jmx:rmi:///jndi/rmi://targethost:9999/jmxrmi",
//    "user":"jolokia",
//    "password":"s!cr!t"
// }
type TargetBlock struct {
	URL      string `json:"url"`
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`
}

type attributeMappingKey struct {
	mbean, attr string
}

// AttributeMapping contains the mapping information between attributes in Jolokia
// responses and fields in metricbeat events
type AttributeMapping map[attributeMappingKey]Attribute

// Get the mapping options for the attribute of an mbean
func (m AttributeMapping) Get(mbean, attr string) (Attribute, bool) {
	a, found := m[attributeMappingKey{mbean, attr}]
	return a, found
}

// MBeanName is an internal struct used to store
// the information by the parsed ```mbean``` (bean name) configuration
// field in ```jmx.mappings```.
type MBeanName struct {
	Domain     string
	Properties map[string]string
}

func (m *MBeanName) Canonicalize(escape bool) string {

	var propertySlice []string

	r2 := regexp.MustCompile(`(["]|[.]|[!]|[\/])`)

	for key, value := range m.Properties {

		tmpVal := value
		if escape {
			tmpVal = r2.ReplaceAllString(value, "!$1")
		}

		propertySlice = append(propertySlice, key+"="+tmpVal)
	}

	sort.Strings(propertySlice)

	return m.Domain + ":" + strings.Join(propertySlice, ",")
}

// ParseMBeanName is a factory function which parses a Managed Bean name string
// identified by mBeanName and returns a new MBean object which
// contains all the information, i.e. domain and properties of the MBean.
//
// The Mbean string has to abide by the rules which are imposed by Java.
// For more info: https://docs.oracle.com/javase/8/docs/api/javax/management/ObjectName.html#getCanonicalName--
func ParseMBeanName(mBeanName string) (*MBeanName, error) {

	// Split mbean string in two parts: the bean domain and the properties
	parts := strings.SplitN(mBeanName, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("domain and properties needed in mbean name: %s", mBeanName)
	}

	// Create a new MBean object
	mybean := &MBeanName{
		Domain: parts[0],
	}

	// First of all verify that all bean properties are
	// in the form key=value
	tmpProps := propertyRegexp.FindAllString(parts[1], -1)
	propertyList := strings.Join(tmpProps, ",")
	if len(propertyList) != len(parts[1]) {
		// Some property didn't match
		return nil, fmt.Errorf("mbean properties must be in the form key=value: %s", mBeanName)
	}

	var mbeanRegexp = regexp.MustCompile("([^,=:*?]+)=([^,=:\"]+|\".*\")")

	// Using this regexp we will split the properties in a 2 dimensional array
	// instead of just splitting by commas because values can be quoted
	// and contain commas, what complicates the parsing.
	// For example this MBean property string:
	//
	// name=HttpRequest1,type=RequestProcessor,worker="http-nio-8080"
	//
	// will become:
	//
	// [][]string{
	// 	[]string{"name=HttpRequest1", "name", "HttpRequest1"},
	// 	[]string{"type=RequestProcessor", "type", "RequestProcessor"},
	// 	[]string{"worker=\"http-nio-8080\"", "worker", "\"http-nio-8080\""}
	// }
	properties := mbeanRegexp.FindAllStringSubmatch(parts[1], -1)

	// If we could not parse MBean properties
	if properties == nil {
		return nil, fmt.Errorf("mbean properties must be in the form key=value: %s", mBeanName)
	}

	// Initialise properties map
	mybean.Properties = make(map[string]string)

	for _, prop := range properties {

		// If every row does not have 3 columns, then
		// parsing must have failed.
		if (prop == nil) || (len(prop) < 3) {
			// Some property didn't match
			return nil, fmt.Errorf("mbean properties must be in the form key=value: %s", mBeanName)
		}

		mybean.Properties[prop[1]] = prop[2]
	}

	return mybean, nil
}

// JolokiaHTTPClient is an interface which describes
// the behaviour of the client communication with
// Jolokia
type JolokiaHTTPClient interface {
	// Fetches the information from Jolokia server regarding MBeans
	BuildRequestsAndMappings(configMappings []JMXMapping, base mb.BaseMetricSet, metricsetName string) ([]*helper.HTTP, AttributeMapping, error)
	BuildDebugRequestMessages(httpReqs []*helper.HTTP, base *mb.BaseMetricSet) (string, interface{})
}

type JolokiaHTTPGetClient struct {
}

func (pc *JolokiaHTTPGetClient) BuildRequestsAndMappings(configMappings []JMXMapping, base mb.BaseMetricSet, metricsetName string) ([]*helper.HTTP, AttributeMapping, error) {

	// Create Jolokia URLs
	uris, responseMapping, err := pc.buildGetRequestURIs(configMappings)
	if err != nil {
		return nil, nil, err
	}

	log := logp.NewLogger(metricsetName).With("host", base.HostData().Host)

	// Create one or more HTTP GET requests
	var httpRequests []*helper.HTTP
	for _, i := range uris {
		http, err := helper.NewHTTP(base)

		http.SetMethod("GET")
		http.SetURI(base.HostData().SanitizedURI + i)

		if logp.IsDebug(metricsetName) {
			log.Debugw("Jolokia GET request",
				"URI", http.GetURI, "type", "request")
		}

		if err != nil {
			return nil, nil, err
		}

		httpRequests = append(httpRequests, http)
	}

	return httpRequests, responseMapping, err
}

func (pc *JolokiaHTTPGetClient) BuildDebugRequestMessages(httpReqs []*helper.HTTP, base *mb.BaseMetricSet) (string, interface{}) {

	return "", nil
}

// Builds a GET URI which will have the following format:
//
// /read/<mbean>/<attribute>/[path]?ignoreErrors=true&canonicalNaming=false
func (pc *JolokiaHTTPGetClient) buildJolokiaGETUri(mbean string, attr Attribute) string {
	initialURI := "/read/%s?ignoreErrors=true&canonicalNaming=false"

	tmpURL := mbean + "/" + attr.Attr

	tmpURL = fmt.Sprintf(initialURI, tmpURL)

	return tmpURL
}

func (pc *JolokiaHTTPGetClient) mBeanAttributeHasField(attr *Attribute) bool {

	if attr.Field != "" && (strings.Trim(attr.Field, " ") != "") {
		return true
	}

	return false
}

func (pc *JolokiaHTTPGetClient) buildGetRequestURIs(mappings []JMXMapping) ([]string, AttributeMapping, error) {

	responseMapping := make(AttributeMapping)
	var urls []string

	// At least Jolokia 1.5 responses with canonicalized MBean names when using
	// wildcards, even when canonicalNaming is set to false, this makes mappings to fail.
	// So use canonicalized names everywhere.
	// If Jolokia returns non-canonicalized MBean names, then we'll need to canonicalize
	// them or change our approach to mappings.

	for _, mapping := range mappings {
		mbean, err := ParseMBeanName(mapping.MBean)
		if err != nil {
			return urls, nil, err
		}

		if len(mapping.Target.URL) != 0 {
			err := errors.New("Proxy requests are only valid when using POST method")
			return urls, nil, err
		}

		// For every attribute we will build a new URI
		for _, attribute := range mapping.Attributes {
			responseMapping[attributeMappingKey{mbean.Canonicalize(true), attribute.Attr}] = attribute

			urls = append(urls, pc.buildJolokiaGETUri(mbean.Canonicalize(true), attribute))

		}

	}

	return urls, responseMapping, nil
}

type JolokiaHTTPPostClient struct {
}

func (pc *JolokiaHTTPPostClient) BuildRequestsAndMappings(configMappings []JMXMapping, base mb.BaseMetricSet, metricsetName string) ([]*helper.HTTP, AttributeMapping, error) {

	body, mapping, err := pc.buildRequestBodyAndMapping(configMappings)
	if err != nil {
		return nil, nil, err
	}

	http, err := helper.NewHTTP(base)
	if err != nil {
		return nil, nil, err
	}
	http.SetMethod("POST")
	http.SetBody(body)

	log := logp.NewLogger(metricsetName).With("host", base.HostData().Host)

	if logp.IsDebug(metricsetName) {

		log.Debugw("Jolokia request body",
			"body", string(body), "type", "request")
	}

	// Create an array with only one HTTP POST request
	httpRequests := []*helper.HTTP{http}

	return httpRequests, mapping, nil
}

func (pc *JolokiaHTTPPostClient) BuildDebugRequestMessages(httpReqs []*helper.HTTP, base *mb.BaseMetricSet) (string, interface{}) {

	return "", nil
}

// Parse strings with properties with the format key=value, being:
// - key a nonempty string of characters which may not contain any of the characters,
//   comma (,), equals (=), colon, asterisk, or question mark.
// - value a string that can be quoted or unquoted, if unquoted it cannot be empty and
//   cannot contain any of the characters comma, equals, colon, or quote.
var propertyRegexp = regexp.MustCompile("[^,=:*?]+=([^,=:\"]+|\".*\")")

func (pc *JolokiaHTTPPostClient) buildRequestBodyAndMapping(mappings []JMXMapping) ([]byte, AttributeMapping, error) {
	responseMapping := make(AttributeMapping)
	var blocks []RequestBlock

	// At least Jolokia 1.5 responses with canonicalized MBean names when using
	// wildcards, even when canonicalNaming is set to false, this makes mappings to fail.
	// So use canonicalized names everywhere.
	// If Jolokia returns non-canonicalized MBean names, then we'll need to canonicalize
	// them or change our approach to mappings.
	config := map[string]interface{}{
		"ignoreErrors":    true,
		"canonicalNaming": true,
	}
	for _, mapping := range mappings {
		mbeanObj, err := ParseMBeanName(mapping.MBean)
		if err != nil {
			return nil, nil, err
		}

		mbean := mbeanObj.Canonicalize(false)

		rb := RequestBlock{
			Type:   "read",
			MBean:  mbean,
			Config: config,
		}

		if len(mapping.Target.URL) != 0 {
			rb.Target = new(TargetBlock)
			rb.Target.URL = mapping.Target.URL
			rb.Target.User = mapping.Target.User
			rb.Target.Password = mapping.Target.Password
		}

		for _, attribute := range mapping.Attributes {
			rb.Attribute = append(rb.Attribute, attribute.Attr)
			responseMapping[attributeMappingKey{mbean, attribute.Attr}] = attribute
		}
		blocks = append(blocks, rb)
	}

	content, err := json.Marshal(blocks)
	return content, responseMapping, err
}

// NewJolokiaHTTPClient is a factory method which creates and returns an implementation
// class of JolokiaHTTPClient interface. HTTP GET and POST are currently supported.
func NewJolokiaHTTPClient(httpMethod string) JolokiaHTTPClient {

	if httpMethod == "GET" {
		return &JolokiaHTTPGetClient{}
	}

	return &JolokiaHTTPPostClient{}

}
