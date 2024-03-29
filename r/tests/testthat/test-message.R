# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

context("Message")

test_that("read_message can read from input stream", {
  batch <- record_batch(x = 1:10)
  bytes <- batch$serialize()
  stream <- BufferReader$create(bytes)

  message <- read_message(stream)
  expect_r6_class(message, "Message")
  expect_equal(message$type, MessageType$RECORD_BATCH)
  expect_r6_class(message$body, "Buffer")
  expect_r6_class(message$metadata, "Buffer")

  message <- read_message(stream)
  expect_null(read_message(stream))
})

test_that("read_message() can read Schema messages", {
  bytes <- schema(x = int32())$serialize()
  stream <- BufferReader$create(bytes)
  message <- read_message(stream)

  expect_r6_class(message, "Message")
  expect_equal(message$type, MessageType$SCHEMA)
  expect_r6_class(message$body, "Buffer")
  expect_r6_class(message$metadata, "Buffer")

  message <- read_message(stream)
  expect_null(read_message(stream))
})

test_that("read_message() can handle raw vectors", {
  batch <- record_batch(x = 1:10)
  bytes <- batch$serialize()
  stream <- BufferReader$create(bytes)

  message_stream <- read_message(stream)
  message_raw <- read_message(bytes)
  expect_equal(message_stream, message_raw)

  bytes <- schema(x = int32())$serialize()
  stream <- BufferReader$create(bytes)
  message_stream <- read_message(stream)
  message_raw <- read_message(bytes)

  expect_equal(message_stream, message_raw)
})
