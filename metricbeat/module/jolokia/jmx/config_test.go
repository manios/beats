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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildJolokiaGETUri(t *testing.T) {
	cases := []struct {
		mbean     string
		attribute *Attribute
		expected  string
	}{
		{
			mbean: `java.lang:type=Memory`,
			attribute: &Attribute{
				Attr: `NonHeapMemoryUsage`,
			},
			expected: `/read/java.lang:type=Memory/NonHeapMemoryUsage?ignoreErrors=true&canonicalNaming=false`,
		},
		{
			mbean: `java.lang:type=Memory`,
			attribute: &Attribute{
				Attr:  `NonHeapMemoryUsage`,
				Field: `max`,
			},
			expected: `/read/java.lang:type=Memory/NonHeapMemoryUsage?ignoreErrors=true&canonicalNaming=false`,
		},
		{
			mbean: `Catalina:name=HttpRequest1,type=RequestProcessor,worker=!"http-nio-8080!"`,
			attribute: &Attribute{
				Attr:  `globalProcessor`,
				Field: `maxTime`,
			},
			expected: `/read/Catalina:name=HttpRequest1,type=RequestProcessor,worker=!"http-nio-8080!"/globalProcessor?ignoreErrors=true&canonicalNaming=false`,
		},
	}

	for _, c := range cases {
		jolokiaGETClient := &JolokiaHTTPGetClient{}
		getURI := jolokiaGETClient.buildJolokiaGETUri(c.mbean, *c.attribute)

		assert.Equal(t, c.expected, getURI, "mbean: "+c.mbean)

	}
}

func TestParseMBean(t *testing.T) {

	cases := []struct {
		mbean    string
		expected *MBeanName
		ok       bool
	}{
		{
			mbean: ``,
			ok:    false,
		},
		{
			mbean: `type=Runtime`,
			ok:    false,
		},
		{
			mbean: `java.lang`,
			ok:    false,
		},
		{
			mbean: `java.lang:`,
			ok:    false,
		},
		{
			mbean: `java.lang:type=Runtime,name`,
			ok:    false,
		},
		{
			mbean: `java.lang:type=Runtime`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"type": "Runtime",
				},
			},
			ok: true,
		},
		{
			mbean: `java.lang:name=Foo,type=Runtime`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo",
					"type": "Runtime",
				},
			},
			ok: true,
		},
		{
			mbean: `java.lang:name=Foo,type=Runtime`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo",
					"type": "Runtime",
				},
			},
			ok: true,
		},
		{
			mbean: `java.lang:type=Runtime,name=Foo*`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo*",
					"type": "Runtime",
				},
			},
			ok: true,
		},
		{
			mbean: `java.lang:type=Runtime,name=*`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "*",
					"type": "Runtime",
				},
			},
			ok: true,
		},
		{
			mbean: `java.lang:name="foo,bar",type=Runtime`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": `"foo,bar"`,
					"type": "Runtime",
				},
			},
			ok: true,
		},
		{
			mbean: `java.lang:type=Memory`,
			expected: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"type": "Memory",
				},
			},
			ok: true,
		},
		{
			mbean: `Catalina:name=HttpRequest1,type=RequestProcessor,worker="http-nio-8080"`,
			expected: &MBeanName{
				Domain: `Catalina`,
				Properties: map[string]string{
					"name":   "HttpRequest1",
					"type":   "RequestProcessor",
					"worker": `"http-nio-8080"`,
				},
			},
			ok: true,
		},
	}

	for _, c := range cases {
		beanObj, err := ParseMBeanName(c.mbean)

		if c.ok {
			assert.NoError(t, err, "failed parsing for: "+c.mbean)
			assert.Equal(t, c.expected, beanObj, "mbean: "+c.mbean)
		} else {
			assert.Error(t, err, "should have failed for: "+c.mbean)
		}
	}

}

func TestCanonicalizeMbeanName(t *testing.T) {

	cases := []struct {
		mbean    *MBeanName
		expected string
		escape   bool
	}{

		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"type": "Runtime",
				},
			},
			escape:   true,
			expected: `java.lang:type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"type": "Runtime",
				},
			},
			escape:   false,
			expected: `java.lang:type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo",
					"type": "Runtime",
				},
			},
			escape:   true,
			expected: `java.lang:name=Foo,type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo",
					"type": "Runtime",
				},
			},
			escape:   false,
			expected: `java.lang:name=Foo,type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo",
					"type": "Runtime",
				},
			},
			escape:   true,
			expected: `java.lang:name=Foo,type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "Foo*",
					"type": "Runtime",
				},
			},
			escape:   true,
			expected: `java.lang:name=Foo*,type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": "*",
					"type": "Runtime",
				},
			},
			escape:   true,
			expected: `java.lang:name=*,type=Runtime`,
		},
		{
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"name": `"foo,bar"`,
					"type": "Runtime",
				},
			},
			escape:   true,
			expected: `java.lang:name=!"foo,bar!",type=Runtime`,
		},
		{
			expected: `java.lang:type=Memory`,
			mbean: &MBeanName{
				Domain: `java.lang`,
				Properties: map[string]string{
					"type": "Memory",
				},
			},
			escape: true,
		},
		{
			expected: `jboss.jmx:alias=jmx!/rmi!/RMIAdaptor!/State`,
			mbean: &MBeanName{
				Domain: `jboss.jmx`,
				Properties: map[string]string{
					"alias": "jmx/rmi/RMIAdaptor/State",
				},
			},
			escape: true,
		},
		{
			mbean: &MBeanName{
				Domain: `Catalina`,
				Properties: map[string]string{
					"name":   "HttpRequest1",
					"type":   "RequestProcessor",
					"worker": `"http-nio-8080"`,
				},
			},
			escape:   true,
			expected: `Catalina:name=HttpRequest1,type=RequestProcessor,worker=!"http-nio-8080!"`,
		},
	}

	for _, c := range cases {
		canonicalString := c.mbean.Canonicalize(c.escape)

		assert.Equal(t, c.expected, canonicalString)
	}

}

func TestMBeanAttributeHasField(t *testing.T) {

	cases := []struct {
		attribute *Attribute
		expected  bool
	}{

		{
			attribute: &Attribute{
				Attr:  "CollectionTime",
				Field: "",
			},
			expected: false,
		},
		{
			attribute: &Attribute{
				Attr:  "CollectionTime",
				Field: "  ",
			},

			expected: false,
		},
		{
			attribute: &Attribute{
				Attr:  "CollectionTime",
				Field: "gc.cms_collection_time",
			},
			expected: true,
		},
	}

	for _, c := range cases {
		jolokiaGETClient := &JolokiaHTTPGetClient{}
		hasField := jolokiaGETClient.mBeanAttributeHasField(c.attribute)

		assert.Equal(t, c.expected, hasField, "mbean attribute: "+c.attribute.Attr, "mbean attribute field: "+c.attribute.Field)
	}
}

func TestBuildGETRequestsAndMappings(t *testing.T) {

}
func TestBuildPOSTRequestsAndMappings(t *testing.T) {

}

func getMappings() []JMXMapping {
	return []JMXMapping{
		{

			MBean: "java.lang:type=Runtime",
			Attributes: []Attribute{
				{
					Attr:  "Uptime",
					Field: "uptime",
				},
			},
		}, {
			MBean: "java.lang:type=GarbageCollector,name=ConcurrentMarkSweep",
			Attributes: []Attribute{
				{
					Attr:  "CollectionTime",
					Field: "gc.cms_collection_time",
				},
				{
					Attr:  "CollectionCount",
					Field: "gc.cms_collection_count",
				},
			},
		},
		{
			MBean: "java.lang:type=Memory",
			Attributes: []Attribute{
				{
					Attr:  "HeapMemoryUsage",
					Field: "memory.heap_usage",
				},
				{
					Attr:  "NonHeapMemoryUsage",
					Field: "memory.non_heap_usage",
				},
			},
		},
	}
}

func TestNewJolokiaHTTPClient(t *testing.T) {

	cases := []struct {
		httpMethod string
		expected   JolokiaHTTPClient
	}{

		{
			httpMethod: "GET",
			expected:   &JolokiaHTTPGetClient{},
		},
		{
			httpMethod: "",
			expected:   &JolokiaHTTPPostClient{},
		},
		{
			httpMethod: "GET",
			expected:   &JolokiaHTTPGetClient{},
		},
		{
			httpMethod: "POST",
			expected:   &JolokiaHTTPPostClient{},
		},
	}

	for _, c := range cases {
		jolokiaGETClient := NewJolokiaHTTPClient(c.httpMethod)

		assert.Equal(t, c.expected, jolokiaGETClient, "httpMethod: "+c.httpMethod)
	}
}
