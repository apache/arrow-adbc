// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

type optionsFlag map[string]string

func (o *optionsFlag) String() string {
	if o == nil {
		return ""
	}
	parts := make([]string, 0, len(*o))
	for key, val := range *o {
		parts = append(parts, key+"="+val)
	}
	return strings.Join(parts, ",")
}

func (o *optionsFlag) Set(value string) error {
	key, val, ok := strings.Cut(value, "=")
	if !ok || strings.TrimSpace(key) == "" {
		return fmt.Errorf("option must be key=value, got %q", value)
	}
	if *o == nil {
		*o = make(map[string]string)
	}
	(*o)[key] = val
	return nil
}

func main() {
	var (
		driverPath string
		entrypoint string
		options    optionsFlag
	)

	flag.StringVar(&driverPath, "driver", "", "Path or driver name to load via ADBC driver manager")
	flag.StringVar(&entrypoint, "entrypoint", "", "Optional driver entrypoint symbol")
	flag.Var(&options, "option", "Database option in key=value form; may be repeated")
	flag.Parse()

	if strings.TrimSpace(driverPath) == "" {
		fmt.Fprintln(os.Stderr, "missing required --driver")
		flag.Usage()
		os.Exit(2)
	}

	dbOptions := make(map[string]string, len(options)+2)
	for key, val := range options {
		dbOptions[key] = val
	}
	dbOptions["driver"] = driverPath
	if strings.TrimSpace(entrypoint) != "" {
		dbOptions["entrypoint"] = entrypoint
	}

	var drv drivermgr.Driver
	db, err := drv.NewDatabase(dbOptions)
	if err != nil {
		fail("create database", err)
	}
	defer closeOrWarn("database", db.Close)

	cnxn, err := db.Open(context.Background())
	if err != nil {
		fail("open connection", err)
	}
	defer closeOrWarn("connection", cnxn.Close)

	info, err := adbc.GetDriverInfo(context.Background(), cnxn)
	if err != nil {
		fail("get driver info", err)
	}

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(info); err != nil {
		fail("encode output", err)
	}
}

func closeOrWarn(label string, fn func() error) {
	if err := fn(); err != nil {
		fmt.Fprintf(os.Stderr, "warning: failed to close %s: %v\n", label, err)
	}
}

func fail(action string, err error) {
	fmt.Fprintf(os.Stderr, "%s: %v\n", action, err)
	os.Exit(1)
}
