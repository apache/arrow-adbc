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

#include <gtest/gtest.h>

#include "utils.h"

TEST(TestStringBuilder, TestBasic) {
  struct StringBuilder str;
  int ret;
  ret = StringBuilderInit(&str, /*initial_size=*/64);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 64);

  ret = StringBuilderAppend(&str, "%s", "BASIC TEST");
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.size, 10);
  EXPECT_STREQ(str.buffer, "BASIC TEST");

  StringBuilderReset(&str);
}

TEST(TestStringBuilder, TestBoundary) {
  struct StringBuilder str;
  int ret;
  ret = StringBuilderInit(&str, /*initial_size=*/10);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 10);

  ret = StringBuilderAppend(&str, "%s", "BASIC TEST");
  EXPECT_EQ(ret, 0);
  // should resize to include \0
  EXPECT_EQ(str.capacity, 11);
  EXPECT_EQ(str.size, 10);
  EXPECT_STREQ(str.buffer, "BASIC TEST");

  StringBuilderReset(&str);
}

TEST(TestStringBuilder, TestMultipleAppends) {
  struct StringBuilder str;
  int ret;
  ret = StringBuilderInit(&str, /*initial_size=*/2);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 2);

  ret = StringBuilderAppend(&str, "%s", "BASIC");
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 6);
  EXPECT_EQ(str.size, 5);
  EXPECT_STREQ(str.buffer, "BASIC");

  ret = StringBuilderAppend(&str, "%s", " TEST");
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 11);
  EXPECT_EQ(str.size, 10);
  EXPECT_STREQ(str.buffer, "BASIC TEST");

  StringBuilderReset(&str);
}
