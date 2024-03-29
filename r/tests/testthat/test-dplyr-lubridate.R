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

skip_if_not_available("dataset")

library(lubridate)
library(dplyr)

# base::strptime() defaults to local timezone
# but arrow's strptime defaults to UTC.
# So that tests are consistent, set the local timezone to UTC
# TODO: consider reevaluating this workaround after ARROW-12980
withr::local_timezone("UTC")

test_date <- as.POSIXct("2017-01-01 00:00:12.3456789", tz = "")
test_df <- tibble::tibble(date = test_date)

# We can support this feature after ARROW-12980 is merged
test_that("timezone aware timestamps are not supported", {
  tz_aware_date <- as.POSIXct("2017-01-01 00:00:12.3456789", tz = "Pacific/Marquesas")
  tz_aware_df <- tibble::tibble(date = tz_aware_date)

  expect_error(
    Table$create(tz_aware_df) %>%
      mutate(x = wday(date)) %>%
      collect(),
    "Cannot extract components from timestamp with specific timezone"
  )
})

# We can support this feature when ARROW-13138 is resolved
test_that("date32 objects are not supported", {
  date <- ymd("2017-01-01")
  df <- tibble::tibble(date = date)

  expect_error(
    Table$create(df) %>%
      mutate(x = year(date)) %>%
      collect(),
    "Function year has no kernel matching input types"
  )
})


test_that("extract year from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = year(date)) %>%
      collect(),
    test_df,
    check.tzone = FALSE
  )
})

test_that("extract isoyear from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = isoyear(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract quarter from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = quarter(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract month from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = month(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract isoweek from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = isoweek(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract day from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = day(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract wday from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = wday(date)) %>%
      collect(),
    test_df
  )

  expect_dplyr_equal(
    input %>%
      mutate(x = wday(date, week_start = 3)) %>%
      collect(),
    test_df
  )

  expect_dplyr_equal(
    input %>%
      mutate(x = wday(date, week_start = 1)) %>%
      collect(),
    test_df
  )

  # We should be able to support the label argument after this ticket is resolved:
  # https://issues.apache.org/jira/browse/ARROW-13133
  x <- Expression$field_ref("x")
  expect_error(
    nse_funcs$wday(x, label = TRUE),
    "Label argument not supported by Arrow"
  )
})

test_that("extract yday from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = yday(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract hour from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = hour(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract minute from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = minute(date)) %>%
      collect(),
    test_df
  )
})

test_that("extract second from date", {
  expect_dplyr_equal(
    input %>%
      mutate(x = second(date)) %>%
      collect(),
    test_df,
    # arrow supports nanosecond resolution but lubridate does not
    tolerance = 1e-6
  )
})
