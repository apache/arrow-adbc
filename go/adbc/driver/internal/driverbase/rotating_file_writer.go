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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultLogNamePrefix = "apache.adbc.go"
	defaultFileSizeMaxKb = int64(1024)
	defaultFileCountMax  = 100
	defaultTraceFileExt  = ".jsonl"
)

// Holds the configuration from the options
type config struct {
	TracingFolderPath string
	LogNamePrefix     string
	FileSizeMaxKb     int64
	FileCountMax      int
}

// An option for the RotatingFileWriter
type rotatingFileWriterOption func(*config)

// Adds the TracingFolderPath option
func WithTracingFolderPath(tracingFolderPath string) rotatingFileWriterOption {
	return func(cfg *config) {
		cfg.TracingFolderPath = tracingFolderPath
	}
}

// Adds the LogNamePrefix option
func WithLogNamePrefix(logNamePrefix string) rotatingFileWriterOption {
	return func(cfg *config) {
		cfg.LogNamePrefix = logNamePrefix
	}
}

// Adds the FileSizeMaxKb option
func WithFileSizeMaxKb(fileSizeMaxKb int64) rotatingFileWriterOption {
	return func(cfg *config) {
		cfg.FileSizeMaxKb = fileSizeMaxKb
	}
}

// Adds the FileCountMax option
func WithFileCountMax(fileCountMax int) rotatingFileWriterOption {
	return func(cfg *config) {
		cfg.FileCountMax = fileCountMax
	}
}

func newConfig(options ...rotatingFileWriterOption) (cfg config, err error) {
	cfg = config{
		TracingFolderPath: "",
		LogNamePrefix:     defaultLogNamePrefix,
		FileSizeMaxKb:     defaultFileSizeMaxKb,
		FileCountMax:      defaultFileCountMax,
	}
	for _, opt := range options {
		opt(&cfg)
	}
	// Ensure default for tracingFolderPath
	if strings.TrimSpace(cfg.TracingFolderPath) == "" {
		cfg.TracingFolderPath, err = getDefaultTracingFolderPath()
		if err != nil {
			return
		}
	}

	// Ensure default for logNamePrefix
	if strings.TrimSpace(cfg.LogNamePrefix) == "" {
		cfg.LogNamePrefix = defaultLogNamePrefix
	}

	// Ensure tracingFolderPath exists
	const folderPermissions = 0755
	err = os.MkdirAll(cfg.TracingFolderPath, folderPermissions)
	if err != nil {
		return
	}
	// Test if we can create/write a file in the traces folder
	tempFile, err := os.CreateTemp(cfg.TracingFolderPath, cfg.LogNamePrefix)
	if err != nil {
		return
	}
	defer func() {
		_ = tempFile.Close()
		_ = os.Remove(tempFile.Name())
	}()
	_, err = tempFile.WriteString("file started")
	if err != nil {
		return
	}

	// Ensure default for fileSizeMaxKb
	cfg.FileSizeMaxKb = max(defaultFileSizeMaxKb, cfg.FileSizeMaxKb)

	// Ensure default for fileCountMax
	cfg.FileCountMax = max(defaultFileCountMax, cfg.FileCountMax)

	return
}

// A rotating file writer that writes bytes to new trace files into a given TracingFolderPath until the trace file exceeds FileSizeMaxKb in size.
// Then a new trace file is created. If the number archived trace files exceeds FileCountMax, then the oldest file(s) are removed.
// The files are named with the following pattern "<LogNamePrefix>-<current date/time UTC>.jsonl"
type rotatingFileWriterImpl struct {
	TracingFolderPath string
	LogNamePrefix     string
	FileSizeMaxKb     int64
	FileCountMax      int
	CurrentWriter     *os.File
}

// Creates a new RotatingFileWriter from the given options
func NewRotatingFileWriter(options ...rotatingFileWriterOption) (*rotatingFileWriterImpl, error) {
	cfg, err := newConfig(options...)
	if err != nil {
		return nil, err
	}

	return &rotatingFileWriterImpl{
		TracingFolderPath: cfg.TracingFolderPath,
		LogNamePrefix:     cfg.LogNamePrefix,
		FileSizeMaxKb:     cfg.FileSizeMaxKb,
		FileCountMax:      cfg.FileCountMax,
		CurrentWriter:     nil,
	}, nil
}

func getDefaultTracingFolderPath() (string, error) {
	userConfigDir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	fullPath := filepath.Join(userConfigDir, ".adbc", "traces")
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
	for _, filePath := range logFiles {
		err := os.Remove(filePath)
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

func (base *rotatingFileWriterImpl) Write(p []byte) (nBytes int, err error) {
	// Check to see if file needs to rotate
	if err = maybeCloseCurrentWriter(base); err != nil {
		return
	}

	// Ensure we have a current writer
	if err = ensureCurrentWriter(base); err != nil {
		return
	}

	// Perform the write
	return base.CurrentWriter.Write(p)
}

func maybeCloseCurrentWriter(base *rotatingFileWriterImpl) error {
	if base.CurrentWriter != nil {
		fileInfo, err := base.CurrentWriter.Stat()
		if err != nil {
			return err
		}
		if fileInfo.Size() >= int64(base.FileSizeMaxKb)*1024 {
			err := base.CurrentWriter.Close()
			if err != nil {
				return err
			}
			base.CurrentWriter = nil
			return removeOldFiles(base)
		}
	}
	return nil
}

func ensureCurrentWriter(base *rotatingFileWriterImpl) error {
	const (
		permissions = 0666 // Required to be full writable on Windows we can open it again
		createFlags = os.O_APPEND | os.O_CREATE | os.O_WRONLY
		appendFlags = os.O_APPEND | os.O_WRONLY
	)

	if base.CurrentWriter == nil {
		// check for a candidate file that is not full
		fullPathLastFile, ok := getCandidateLogFileName(base)
		if ok {
			// attempt to open existing candidate file
			currentWriter, err := os.OpenFile(fullPathLastFile, appendFlags, permissions)
			if err == nil {
				base.CurrentWriter = currentWriter
				return nil
			}
			// unable to open candidate file (locked?)
		}

		// open a new file
		fullPath := buildNewFileName(base)
		currentWriter, err := os.OpenFile(fullPath, createFlags, permissions)
		if err != nil {
			return err
		}
		base.CurrentWriter = currentWriter
	}
	return nil
}

func buildNewFileName(base *rotatingFileWriterImpl) string {
	timeStamp := time.Now().UTC().Format("2006-01-02-15-04-05.000000000")
	fileName := base.LogNamePrefix + "-" + timeStamp + defaultTraceFileExt
	fullPath := filepath.Join(base.TracingFolderPath, fileName)
	return fullPath
}

func getCandidateLogFileName(base *rotatingFileWriterImpl) (string, bool) {
	logFiles, err := getLogFiles(base.TracingFolderPath, base.LogNamePrefix, defaultTraceFileExt)
	if err != nil || len(logFiles) < 1 {
		return "", false
	}

	// Assume these file paths are ordered lexicographically, as documented for filepath.Glob()
	candiateFilePath := logFiles[len(logFiles)-1]
	fileSizeMaxBytes := base.FileSizeMaxKb * 1024
	fileInfo, err := os.Stat(candiateFilePath)
	if err != nil || fileInfo.Size() >= fileSizeMaxBytes {
		return "", false
	}

	// Return path
	return candiateFilePath, true
}

func removeOldFiles(base *rotatingFileWriterImpl) error {
	logFiles, err := getLogFiles(base.TracingFolderPath, base.LogNamePrefix, defaultTraceFileExt)
	if err != nil {
		return nil
	}
	nFiles := len(logFiles)
	if nFiles > int(base.FileCountMax) {
		numToRemove := nFiles - int(base.FileCountMax)
		for _, filePath := range logFiles[:numToRemove-1] {
			err := os.Remove(filePath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func getLogFiles(folderPath string, prefix string, ext string) (logFiles []string, err error) {
	filePattern := filepath.Join(folderPath, prefix+"*"+ext)
	return filepath.Glob(filePattern)
}
