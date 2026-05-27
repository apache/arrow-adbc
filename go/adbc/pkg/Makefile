# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

MANAGERS = bigquery flightsql panicdummy snowflake databricks salesforce

GO_BUILD := go build
RM := rm -f
DEBUG ?= false
GOOS := $(shell go env GOOS)

ifeq ($(GOOS),linux)
	SUFFIX=so
else ifeq ($(GOOS),windows)
	SUFFIX=dll
	RM=del
else
	SUFFIX=dylib
endif

# netgo forces Go's pure-DNS resolver, but on Windows it breaks localhost
# resolution when a custom DNS server is set, because the default Windows
# hosts file leaves the localhost entries commented out.
ifeq ($(GOOS),windows)
	BUILD_TAGS := driverlib
else
	BUILD_TAGS := driverlib,netgo
endif

ifeq ($(GOOS),windows)
	GIT_VERSION := $(shell powershell -Command "git tag --sort=-v:refname --points-at $$(git rev-list --tags --max-count=1) | Select-Object -First 1")
else
	GIT_VERSION=$(shell git tag -l --points-at $(shell git rev-list --tags --max-count=1) --sort=-v:refname | head -n 1)
endif

GIT_VERSION ?= unknown
VERSION=$(subst go/adbc/,,$(GIT_VERSION))

# Expand dynamically libadbc_driver_.SUFFIX
DRIVERS := $(addsuffix .$(SUFFIX),$(addprefix libadbc_driver_,$(MANAGERS)))

ifeq ($(DEBUG),$(filter $(DEBUG),true 1 yes))
	GCFLAGS := -N -l

	ifeq ($(GOOS),darwin)
		# Go on macOS forces -w (strip DWARF) for buildmode=c-shared by default
		LDFLAGS := -w=false -compressdwarf=false
	else
		LDFLAGS :=
	endif
else
	GCFLAGS :=
	LDFLAGS := -s -w
endif

# =========
#  Targets
# =========

.PHONY: all regenerate
all: $(DRIVERS)

libadbc_driver_%.$(SUFFIX): % ../driver/% ../go.mod ../go.sum
	$(GO_BUILD) -buildvcs=true -tags $(BUILD_TAGS) \
		-o $@ -buildmode=c-shared \
		-gcflags "$(GCFLAGS)" \
		-ldflags "$(LDFLAGS) -X github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase.infoDriverVersion=$(VERSION)" \
		./$*
	$(RM) $(basename $@).h

regenerate:
	go run gen/main.go -prefix BigQuery -o ./bigquery/ -driver ../driver/bigquery
	go run gen/main.go -prefix Databricks -o ./databricks/ -driver ../driver/databricks
	go run gen/main.go -prefix FlightSQL -o ./flightsql/ -driver ../driver/flightsql
	go run gen/main.go -prefix Databricks -o ./databricks/ -driver ../driver/databricks
	go run gen/main.go -prefix PanicDummy -o ./panicdummy/ -driver ../driver/panicdummy
	go run gen/main.go -prefix Snowflake -o ./snowflake/ -driver ../driver/snowflake
	go run gen/main.go -prefix Salesforce -o ./salesforce/ -driver ../driver/salesforce

clean:
	$(RM) $(DRIVERS)
