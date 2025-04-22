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

package driverbase

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultTracingFolderPath = ""
	defaultLogNamePrefix     = "apache.adbc.go"
	defaultFileSizeMaxKb     = int64(1024)
	defaultFileCountMax      = 100
	defaultTraceFileExt      = ".jsonl"
)

// Holds the configuration from the options
type config struct {
	TracingFolderPath string
	LogNamePrefix     string
	FileSizeMaxKb     int64
	FileCountMax      int
}

// An option for the RotatingFileWriter
type RotatingFileWriterOption interface {
	apply(config) config
}

// Adds the TracingFolderPath option
func WithTracingFolderPath(tracingFolderPath string) RotatingFileWriterOption {
	return tracingFolderPathOption{tracingFolderPath}
}

type tracingFolderPathOption struct {
	TracingFolderPath string
}

func (o tracingFolderPathOption) apply(cfg config) config {
	cfg.TracingFolderPath = o.TracingFolderPath
	return cfg
}

// Adds the LogNamePrefix option
func WithLogNamePrefix(logNamePrefix string) RotatingFileWriterOption {
	return logNamePrefixOption{logNamePrefix}
}

type logNamePrefixOption struct {
	LogNamePrefix string
}

func (o logNamePrefixOption) apply(cfg config) config {
	cfg.LogNamePrefix = o.LogNamePrefix
	return cfg
}

// Adds the FileSizeMaxKb option
func WithFileSizeMaxKb(fileSizeMaxKb int64) RotatingFileWriterOption {
	return fileSizeMaxKbOption{fileSizeMaxKb}
}

type fileSizeMaxKbOption struct {
	FileSizeMaxKb int64
}

func (o fileSizeMaxKbOption) apply(cfg config) config {
	cfg.FileSizeMaxKb = o.FileSizeMaxKb
	return cfg
}

// Adds the FileCountMax option
func WithFileCountMax(fileCountMax int) RotatingFileWriterOption {
	return fileCountMaxOption{fileCountMax}
}

type fileCountMaxOption struct {
	FileCountMax int
}

func (o fileCountMaxOption) apply(cfg config) config {
	cfg.FileCountMax = o.FileCountMax
	return cfg
}

func newConfig(options ...RotatingFileWriterOption) config {
	cfg := config{
		TracingFolderPath: defaultTracingFolderPath,
		LogNamePrefix:     defaultLogNamePrefix,
		FileSizeMaxKb:     defaultFileSizeMaxKb,
		FileCountMax:      defaultFileCountMax,
	}
	for _, opt := range options {
		// Applies the option value to the configuration
		cfg = opt.apply(cfg)
	}
	return cfg
}

// A rotating file writer that writes bytes to new trace files into a given TracingFolderPath until the trace file exceeds FileSizeMaxKb in size.
// Then a new trace file is created. If the number archived trace files exceeds FileCountMax, then the oldest file(s) are removed.
// The files are named with the following pattern "<LogNamePrefix>-<current date/time UTC>.jsonl"
type RotatingFileWriter interface {
	// Extends
	io.Writer
	io.Closer

	// Gets the path to the tracing folder
	GetTracingFolderPath() string
	// Gets the prefix for the log file name
	GetLogNamePrefix() string
	// Gets the maximum file size for the trace file
	GetFileSizeMaxKb() int64
	// Gets the maximum number of archive trace files to keep in the rotation
	GetFileCountMax() int
	// Gets the file stats for the current trace file
	Stat() (fs.FileInfo, error)
	// Clears the tracing folder of trace files with the given log file prefix
	Clear() error
}

type rotatingFileWriterImpl struct {
	TracingFolderPath string
	LogNamePrefix     string
	FileSizeMaxKb     int64
	FileCountMax      int
	CurrentWriter     *os.File
}

// Creates a new RotatingFileWriter from the given options
func NewRotatingFileWriter(options ...RotatingFileWriterOption) (RotatingFileWriter, error) {

	cfg := newConfig(options...)

	var tracingFolderPath = cfg.TracingFolderPath
	// Ensure default for tracingFolderPath
	if strings.TrimSpace(tracingFolderPath) == "" {
		fullPath, err := getDefaultTracingFolderPath()
		if err != nil {
			return nil, err
		}
		tracingFolderPath = fullPath
	}
	// Ensure tracingFolderPath exists
	err := os.MkdirAll(tracingFolderPath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	var logNamePrefix = cfg.LogNamePrefix
	// Ensure default for logNamePrefix
	if strings.TrimSpace(logNamePrefix) == "" {
		namePrefix := defaultLogNamePrefix
		logNamePrefix = namePrefix
	}

	var fileSizeMaxKb = cfg.FileSizeMaxKb
	// Ensure default for fileSizeMaxKb
	if fileSizeMaxKb <= 0 {
		var maxKb = defaultFileSizeMaxKb
		fileSizeMaxKb = maxKb
	}

	var fileCountMax = cfg.FileCountMax
	// Ensure default for fileCountMax
	if fileCountMax <= 0 {
		var countMax = defaultFileCountMax
		fileCountMax = countMax
	}

	// Test if we can create/write a file in the traces folder
	tempFile, err := os.CreateTemp(tracingFolderPath, logNamePrefix)
	if err != nil {
		return nil, err
	}
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()
	_, err = tempFile.WriteString("file started")
	if err != nil {
		return nil, err
	}

	return &rotatingFileWriterImpl{
		TracingFolderPath: tracingFolderPath,
		LogNamePrefix:     logNamePrefix,
		FileSizeMaxKb:     fileSizeMaxKb,
		FileCountMax:      fileCountMax,
		CurrentWriter:     nil,
	}, nil
}

func getDefaultTracingFolderPath() (string, error) {
	userHome, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	fullPath := filepath.Join(userHome, ".adbc", "traces")
	return fullPath, nil
}

// Closes the rotating file write
func (base *rotatingFileWriterImpl) Close() error {
	if base.CurrentWriter != nil {
		err := base.CurrentWriter.Close()
		base.CurrentWriter = nil
		return err
	}
	return nil
}

func (base *rotatingFileWriterImpl) Clear() error {
	if base.CurrentWriter != nil {
		err := base.CurrentWriter.Close()
		if err != nil {
			return err
		}
	}
	logFiles, err := getLogFiles(base.TracingFolderPath, base.LogNamePrefix, defaultTraceFileExt)
	if err != nil {
		return err
	}
	for _, file := range logFiles {
		err := os.Remove(filepath.Join(base.TracingFolderPath, file.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (base *rotatingFileWriterImpl) Stat() (fs.FileInfo, error) {
	if base.CurrentWriter != nil {
		return base.CurrentWriter.Stat()
	}
	return nil, errors.New("no trace file is open")
}

func (base *rotatingFileWriterImpl) GetTracingFolderPath() string {
	return base.TracingFolderPath
}

func (base *rotatingFileWriterImpl) GetLogNamePrefix() string {
	return base.LogNamePrefix
}

func (base *rotatingFileWriterImpl) GetFileSizeMaxKb() int64 {
	return base.FileSizeMaxKb
}

func (base *rotatingFileWriterImpl) GetFileCountMax() int {
	return base.FileCountMax
}

func (base *rotatingFileWriterImpl) Write(p []byte) (int, error) {
	shouldReturn := false
	i := 0
	err := (error)(nil)

	// Check to see if file needs to rotate
	shouldReturn, i, err = maybeCloseCurrentWriter(base)
	if shouldReturn {
		return i, err
	}

	// Ensure we have a current writer
	shouldReturn, i, err = ensureCurrentWriter(base)
	if shouldReturn {
		return i, err
	}

	// Perform the write
	return base.CurrentWriter.Write(p)
}

func maybeCloseCurrentWriter(base *rotatingFileWriterImpl) (bool, int, error) {
	if base.CurrentWriter != nil {
		fileInfo, err := base.CurrentWriter.Stat()
		if err != nil {
			return true, 0, err
		}
		if fileInfo.Size() >= int64(base.FileSizeMaxKb)*1024 {
			err := base.CurrentWriter.Close()
			if err != nil {
				return true, 0, err
			}
			base.CurrentWriter = nil
			return removeOldFiles(base)
		}
	}
	return false, 0, nil
}

func ensureCurrentWriter(base *rotatingFileWriterImpl) (bool, int, error) {
	if base.CurrentWriter == nil {
		now := time.Now().UTC()
		safeTimeStamp := strings.ReplaceAll(strings.ReplaceAll(strings.Replace(strings.Replace(now.Format(time.RFC3339Nano), "Z", "", 1), "T", "-", 1), ":", "-"), ".", "-")
		fileName := base.LogNamePrefix + "-" + safeTimeStamp + defaultTraceFileExt
		fullPath := filepath.Join(base.TracingFolderPath, fileName)
		currentWriter, err := os.OpenFile(fullPath, os.O_APPEND|os.O_CREATE, os.ModeAppend)
		if err != nil {
			return true, 0, err
		}
		base.CurrentWriter = currentWriter
	}
	return false, 0, nil
}

func removeOldFiles(base *rotatingFileWriterImpl) (bool, int, error) {
	logFiles, err := getLogFiles(base.TracingFolderPath, base.LogNamePrefix, defaultTraceFileExt)
	if err != nil {
		return true, 0, nil
	}
	len := len(logFiles)
	if len > int(base.FileCountMax) {
		numToRemove := len - int(base.FileCountMax)
		for index, file := range logFiles {
			if index < numToRemove {
				err := os.Remove(filepath.Join(base.TracingFolderPath, file.Name()))
				if err != nil {
					return true, 0, err
				}
			}
		}
	}
	return false, 0, nil
}

func getLogFiles(folderPath string, prefix string, ext string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(folderPath)
	if err != nil {
		return nil, err
	}

	var logFiles []os.DirEntry
	for _, file := range files {
		baseName := filepath.Base(file.Name())
		if strings.HasPrefix(baseName, prefix) && strings.HasSuffix(baseName, ext) {
			logFiles = append(logFiles, file)
		}
	}
	return logFiles, nil
}
