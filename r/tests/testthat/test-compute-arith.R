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

test_that("Addition", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_type_equal(a, int32())
  expect_type_equal(a + 4L, int32())
  expect_type_equal(a + 4, float64())
  expect_equal(a + 4L, Array$create(c(5:8, NA_integer_)))
  expect_identical(as.vector(a + 4L), c(5:8, NA_integer_))
  expect_equal(a + 4L, Array$create(c(5:8, NA_integer_)))
  expect_as_vector(a + 4L, c(5:8, NA_integer_))
  expect_equal(a + NA_integer_, Array$create(rep(NA_integer_, 5)))

  a8 <- a$cast(int8())
  expect_type_equal(a8 + Scalar$create(1, int8()), int8())

  # int8 will be promoted to int32 when added to int32
  expect_type_equal(a8 + 127L, int32())
  expect_equal(a8 + 127L, Array$create(c(128:131, NA_integer_)))

  b <- Array$create(c(4:1, NA_integer_))
  expect_type_equal(a8 + b, int32())
  expect_equal(a8 + b, Array$create(c(5L, 5L, 5L, 5L, NA_integer_)))

  expect_type_equal(a + 4.1, float64())
  expect_equal(a + 4.1, Array$create(c(5.1, 6.1, 7.1, 8.1, NA_real_)))
})

test_that("Subtraction", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a - 3L, Array$create(c(-2:1, NA_integer_)))

  expect_equal(
    Array$create(c(5.1, 6.1, 7.1, 8.1, NA_real_)) - a,
    Array$create(c(4.1, 4.1, 4.1, 4.1, NA_real_))
  )
})

test_that("Multiplication", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a * 2L, Array$create(c(1:4 * 2L, NA_integer_)))

  expect_equal(
    (a * 0.5) * 3L,
    Array$create(c(1.5, 3, 4.5, 6, NA_real_))
  )
})

test_that("Division", {
  a <- Array$create(c(1:4, NA_integer_))
  expect_equal(a / 2, Array$create(c(1:4 / 2, NA_real_)))
  expect_equal(a %/% 2, Array$create(c(0L, 1L, 1L, 2L, NA_integer_)))
  expect_equal(a / 2 / 2, Array$create(c(1:4 / 2 / 2, NA_real_)))
  expect_equal(a %/% 2 %/% 2, Array$create(c(0L, 0L, 0L, 1L, NA_integer_)))

  b <- a$cast(float64())
  expect_equal(b / 2, Array$create(c(1:4 / 2, NA_real_)))
  expect_equal(b %/% 2, Array$create(c(0L, 1L, 1L, 2L, NA_integer_)))

  # the behavior of %/% matches R's (i.e. the integer of the quotient, not
  # simply dividing two integers)
  expect_equal(b / 2.2, Array$create(c(1:4 / 2.2, NA_real_)))
  # nolint start
  # c(1:4) %/% 2.2 != c(1:4) %/% as.integer(2.2)
  # c(1:4) %/% 2.2             == c(0L, 0L, 1L, 1L)
  # c(1:4) %/% as.integer(2.2) == c(0L, 1L, 1L, 2L)
  # nolint end
  expect_equal(b %/% 2.2, Array$create(c(0L, 0L, 1L, 1L, NA_integer_)))

  expect_equal(a %% 2, Array$create(c(1L, 0L, 1L, 0L, NA_integer_)))

  expect_equal(b %% 2, Array$create(c(1:4 %% 2, NA_real_)))
})

test_that("Power", {
  a <- Array$create(c(1:4, NA_integer_))
  b <- a$cast(float64())
  c <- a$cast(int64())
  d <- a$cast(uint64())

  expect_equal(a^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(a^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(a^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(a^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))

  expect_equal(b^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(b^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(b^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(b^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))

  expect_equal(c^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(c^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(c^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(c^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))

  expect_equal(d^0, Array$create(c(1, 1, 1, 1, NA_real_)))
  expect_equal(d^2, Array$create(c(1, 4, 9, 16, NA_real_)))
  expect_equal(d^(-1), Array$create(c(1, 1 / 2, 1 / 3, 1 / 4, NA_real_)))
  expect_equal(d^(.5), Array$create(c(1, sqrt(2), sqrt(3), sqrt(4), NA_real_)))
})

test_that("Dates casting", {
  a <- Array$create(c(Sys.Date() + 1:4, NA_integer_))

  skip("ARROW-11090 (date/datetime arithmetic)")
  # Error: NotImplemented: Function add_checked has no kernel matching input types (array[date32[day]], scalar[double])
  expect_equal(a + 2, Array$create(c((Sys.Date() + 1:4) + 2), NA_integer_))
})
