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
	"encoding/pem"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/apache/arrow/go/v13/arrow/memory"
)

const (
	RSA_KEY_PEM = `-----BEGIN RSA PRIVATE KEY-----
MIIEogIBAAKCAQEAx406GdWr6LsKZreyi60beN5m2mgkQqivtpcS06I2bo7bDK+Y
DwSYpDw2u45VmvXi22c3qH55K66ixI5B/ZTkeA+veACRqw5d/lXf6/+JIwXBNy2T
s0K7kXLb4lz7e2DBZwuU2td5WBoTGbkt9exIYg3uH7Gg3AM3r3pvA0yGMKayZ6bu
h7cxOAlf21G9PrN/mysIbwkhx1Rk9xnYD8YBtaFO2qiv4NU6OT9cCcKoqlSK45tm
zANFUyYGQYX7C1p+pIJkKtPgf8gQlouUnbpg2IOPkMgjl6FZ2SrjMkv6aml4POWQ
qlQl2s3vYaO8paqgzhS2zyB3lzl3GkgPqP3dzQIDAQABAoIBAA/lDRHeEKrhgchr
wmeDcDLros1WdT5FWcr/8+pykHEJpPi44IegX0OEXlGzL9i/ftnouf4MUOeB/19g
+3BQ6fUxk/xJg1DY50i+cK8cyWtw5mfYPrG304vtgQaMpHq6hWfepsqFq5nbvdYT
MiOAC3CarnMiKW93kXnDP/txyXsvb8nYpfoniToiA+VjjJtPTEx7BdvEUnT37W1d
wCDELePB0IGyDKpjEawrhqkr0zGvvV8ZZ4jo7pGlSHuQF9FUk5uj8uOAIMBwSIGl
158oX3o8/ZuyoydwOaf2kkIgp92DQvTev2RcyKzMLIBvmXZt5gukSx1uK2YogKkf
WZYdkvsCgYEA+esWbp9vjF3pD51xwE/SErpdG6h0y+5tlQidTtY1dDhg4eEbHLBo
UUnIvEeqA6s5q55aIwwnKllg7hgbHN6r9CXue+JOSPDSn/p8NEf6D9uOpp3weeH/
iWi0Hvd5R0Cj+WvS0M1ImHLKYP1MwjzJPZwiJgGLqKb3cb55Ww1cFgsCgYEAzGhe
+jEH6dHAv49yzrxTDAiqsnf1YkPKjbqPZHW/NQgGyCpQHhmHP/yvuUhFoSeItDAn
aedpozTjLKsV0BZZWBxnZs97D3nJsLFTmmH2bQ7yrY80Vy29BvBNM7KLIjcpV+/5
otFffd1XvG1wo7iLpCnpqySp1KK6xImFWuXneocCgYAxTaiKQFfgSDKPSw9C0jxu
R2fQ6gXIqYvix+VIxUecWvb5+M/BdCfQSY8ZXcXznVbuPXooyd/8Ic/WiNjodCFW
NBl9RSMOjrvupZrVMZHqiPT2d9gWI3inIgnOxiGd5emzbgsT+DunH5Y/VhlLhHRq
/B9cgheMOQw920bTqKoPCwKBgEGXhFK4hReMzVMrDuY0HFoSNeRLBhzgMBFGuli8
R/0WdEaq/UaLXpEz3peQD3flHcLkaOFc9tL+V5+vzrIVWdoiUzP0hEK1C/l9DZzO
rqrUTx9Ogrcu4Cdn2P4r3uW92bB0OyD6GrBi5JJ8c9a1k2m8YVUf0LeA+Hm0v2wp
thvlAoGADdrA10SF5+J/LSsKZfyPdfQXBd5ojXTBHQmXEMimqyrM3VJMz3ANANPL
GVRHvkgXR3TgwhnFvBySAhr6nVX9kE720yv5FFCApTVpHZjx4QP0O7NMiaAP5wPa
b+f2PVFx/WfocQgZP0FqIgKgQjBlrsAu9Pt+yleG/iApFBgB23k=
-----END RSA PRIVATE KEY-----`

	RSA_KEY_P8 = `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDHjToZ1avouwpm
t7KLrRt43mbaaCRCqK+2lxLTojZujtsMr5gPBJikPDa7jlWa9eLbZzeofnkrrqLE
jkH9lOR4D694AJGrDl3+Vd/r/4kjBcE3LZOzQruRctviXPt7YMFnC5Ta13lYGhMZ
uS317EhiDe4fsaDcAzevem8DTIYwprJnpu6HtzE4CV/bUb0+s3+bKwhvCSHHVGT3
GdgPxgG1oU7aqK/g1To5P1wJwqiqVIrjm2bMA0VTJgZBhfsLWn6kgmQq0+B/yBCW
i5SdumDYg4+QyCOXoVnZKuMyS/pqaXg85ZCqVCXaze9ho7ylqqDOFLbPIHeXOXca
SA+o/d3NAgMBAAECggEAD+UNEd4QquGByGvCZ4NwMuuizVZ1PkVZyv/z6nKQcQmk
+Ljgh6BfQ4ReUbMv2L9+2ei5/gxQ54H/X2D7cFDp9TGT/EmDUNjnSL5wrxzJa3Dm
Z9g+sbfTi+2BBoykerqFZ96myoWrmdu91hMyI4ALcJqucyIpb3eRecM/+3HJey9v
ydil+ieJOiID5WOMm09MTHsF28RSdPftbV3AIMQt48HQgbIMqmMRrCuGqSvTMa+9
XxlniOjukaVIe5AX0VSTm6Py44AgwHBIgaXXnyhfejz9m7KjJ3A5p/aSQiCn3YNC
9N6/ZFzIrMwsgG+Zdm3mC6RLHW4rZiiAqR9Zlh2S+wKBgQD56xZun2+MXekPnXHA
T9ISul0bqHTL7m2VCJ1O1jV0OGDh4RscsGhRSci8R6oDqzmrnlojDCcqWWDuGBsc
3qv0Je574k5I8NKf+nw0R/oP246mnfB54f+JaLQe93lHQKP5a9LQzUiYcspg/UzC
PMk9nCImAYuopvdxvnlbDVwWCwKBgQDMaF76MQfp0cC/j3LOvFMMCKqyd/ViQ8qN
uo9kdb81CAbIKlAeGYc//K+5SEWhJ4i0MCdp52mjNOMsqxXQFllYHGdmz3sPecmw
sVOaYfZtDvKtjzRXLb0G8E0zsosiNylX7/mi0V993Ve8bXCjuIukKemrJKnUorrE
iYVa5ed6hwKBgDFNqIpAV+BIMo9LD0LSPG5HZ9DqBcipi+LH5UjFR5xa9vn4z8F0
J9BJjxldxfOdVu49eijJ3/whz9aI2Oh0IVY0GX1FIw6Ou+6lmtUxkeqI9PZ32BYj
eKciCc7GIZ3l6bNuCxP4O6cflj9WGUuEdGr8H1yCF4w5DD3bRtOoqg8LAoGAQZeE
UriFF4zNUysO5jQcWhI15EsGHOAwEUa6WLxH/RZ0Rqr9RotekTPel5APd+UdwuRo
4Vz20v5Xn6/OshVZ2iJTM/SEQrUL+X0NnM6uqtRPH06Cty7gJ2fY/ive5b3ZsHQ7
IPoasGLkknxz1rWTabxhVR/Qt4D4ebS/bCm2G+UCgYAN2sDXRIXn4n8tKwpl/I91
9BcF3miNdMEdCZcQyKarKszdUkzPcA0A08sZVEe+SBdHdODCGcW8HJICGvqdVf2Q
TvbTK/kUUIClNWkdmPHhA/Q7s0yJoA/nA9pv5/Y9UXH9Z+hxCBk/QWoiAqBCMGWu
wC70+37KV4b+ICkUGAHbeQ==
-----END PRIVATE KEY-----`
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
	pkcs1Key := writeKey("key.pem", []byte(RSA_KEY_PEM))
	defer os.Remove(pkcs1Key)
	opts[OptionJwtPrivateKey] = pkcs1Key
	if err := db.SetOptions(opts); err != nil {
		t.Fatalf("failed to set options: %s", err.Error())
	}
	if db.cfg.PrivateKey == nil {
		t.Fatalf("failed to set private key")
	}

	// PKCS8 key
	pkcs8Key := writeKey("key.p8", []byte(RSA_KEY_P8))
	defer os.Remove(pkcs8Key)
	opts[OptionJwtPrivateKey] = pkcs8Key
	if err := db.SetOptions(opts); err != nil {
		t.Fatalf("failed to set options: %s", err.Error())
	}
	if db.cfg.PrivateKey == nil {
		t.Fatalf("failed to set private key")
	}

	// binary key
	block, _ := pem.Decode([]byte(RSA_KEY_PEM))
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
