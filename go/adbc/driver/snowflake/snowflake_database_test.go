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

package snowflake

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

func TestSetOptionsJwtPrivateKey(t *testing.T) {
	opts := make(map[string]string)
	alloc := memory.DefaultAllocator
	driverBase := driverbase.NewDriverImplBase("Snowflake", alloc)
	databaseBase := driverbase.NewDatabaseImplBase(&driverBase)
	db := &databaseImpl{DatabaseImplBase: databaseBase, useHighPrecision: true}

	writeKey := func(filename string, key []byte) string {
		f, err := os.CreateTemp("", filename)
		if err != nil {
			panic(err)
		}
		_, err = f.Write(key)
		if err != nil {
			panic(err)
		}
		return f.Name()
	}

	// PKCS1 key
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	rsaKeyPem := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})
	pkcs1Key := writeKey("key.pem", rsaKeyPem)
	defer os.Remove(pkcs1Key)
	opts[OptionJwtPrivateKey] = pkcs1Key
	if err := db.SetOptions(opts); err != nil {
		t.Fatalf("failed to set options: %s", err.Error())
	}
	if db.cfg.PrivateKey == nil {
		t.Fatalf("failed to set private key")
	}

	// PKCS8 key
	rsaKeyP8Bytes, _ := x509.MarshalPKCS8PrivateKey(rsaKey)
	rsaKeyP8 := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: rsaKeyP8Bytes,
	})
	pkcs8Key := writeKey("key.p8", rsaKeyP8)
	defer os.Remove(pkcs8Key)
	opts[OptionJwtPrivateKey] = pkcs8Key
	if err := db.SetOptions(opts); err != nil {
		t.Fatalf("failed to set options: %s", err.Error())
	}
	if db.cfg.PrivateKey == nil {
		t.Fatalf("failed to set private key")
	}

	// binary key
	block, _ := pem.Decode([]byte(rsaKeyPem))
	binKey := writeKey("key.bin", block.Bytes)
	defer os.Remove(binKey)
	opts[OptionJwtPrivateKey] = binKey
	if err := db.SetOptions(opts); err != nil {
		t.Fatalf("failed to set options: %s", err.Error())
	}
	if db.cfg.PrivateKey == nil {
		t.Fatalf("failed to set private key")
	}
}
