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

# for compatibility with R versions earlier than 4.0.0
if (!exists("deparse1")) {
  deparse1 <- function(expr, collapse = " ", width.cutoff = 500L, ...) {
    paste(deparse(expr, width.cutoff, ...), collapse = collapse)
  }
}

oxford_paste <- function(x, conjunction = "and", quote = TRUE) {
  if (quote && is.character(x)) {
    x <- paste0('"', x, '"')
  }
  if (length(x) < 2) {
    return(x)
  }
  x[length(x)] <- paste(conjunction, x[length(x)])
  if (length(x) > 2) {
    return(paste(x, collapse = ", "))
  } else {
    return(paste(x, collapse = " "))
  }
}

assert_is <- function(object, class) {
  msg <- paste(substitute(object), "must be a", oxford_paste(class, "or"))
  assert_that(inherits(object, class), msg = msg)
}

assert_is_list_of <- function(object, class) {
  msg <- paste(substitute(object), "must be a list of", oxford_paste(class, "or"))
  assert_that(is_list_of(object, class), msg = msg)
}

is_list_of <- function(object, class) {
  is.list(object) && all(map_lgl(object, ~ inherits(., class)))
}

empty_named_list <- function() structure(list(), .Names = character(0))

r_symbolic_constants <- c(
  "pi", "TRUE", "FALSE", "NULL", "Inf", "NA", "NaN",
  "NA_integer_", "NA_real_", "NA_complex_", "NA_character_"
)

is_function <- function(expr, name) {
  if (!is.call(expr)) {
    return(FALSE)
  } else {
    if (deparse1(expr[[1]]) == name) {
      return(TRUE)
    }
    out <- lapply(expr, is_function, name)
  }
  any(vapply(out, isTRUE, TRUE))
}

all_funs <- function(expr) {
  names <- all_names(expr)
  names[vapply(names, function(name) is_function(expr, name), TRUE)]
}

all_vars <- function(expr) {
  setdiff(all.vars(expr), r_symbolic_constants)
}

all_names <- function(expr) {
  setdiff(all.names(expr), r_symbolic_constants)
}

is_constant <- function(expr) {
  length(all_vars(expr)) == 0
}

read_compressed_error <- function(e) {
  msg <- conditionMessage(e)
  if (grepl(" codec ", msg)) {
    compression <- sub(".*Support for codec '(.*)'.*", "\\1", msg)
    e$message <- paste0(
      msg,
      "\nIn order to read this file, you will need to reinstall arrow with additional features enabled.",
      "\nSet one of these environment variables before installing:",
      sprintf("\n\n * LIBARROW_MINIMAL=false (for all optional features, including '%s')", compression),
      sprintf("\n * ARROW_WITH_%s=ON (for just '%s')", toupper(compression), compression),
      "\n\nSee https://arrow.apache.org/docs/r/articles/install.html for details"
    )
  }
  stop(e)
}

handle_embedded_nul_error <- function(e) {
  msg <- conditionMessage(e)
  if (grepl(" nul ", msg)) {
    e$message <- paste0(msg, "; to strip nuls when converting from Arrow to R, set options(arrow.skip_nul = TRUE)")
  }
  stop(e)
}

handle_parquet_io_error <- function(e, format) {
  msg <- conditionMessage(e)
  if (grepl("Parquet magic bytes not found in footer", msg) && length(format) > 1 && is_character(format)) {
    # If length(format) > 1, that means it is (almost certainly) the default/not specified value
    # so let the user know that they should specify the actual (not parquet) format
    abort(c(
      msg,
      i = "Did you mean to specify a 'format' other than the default (parquet)?"
    ))
  }
  stop(e)
}

is_writable_table <- function(x) {
  inherits(x, c("data.frame", "ArrowTabular"))
}

# This attribute is used when is_writable is passed into assert_that, and allows
# the call to form part of the error message when is_writable is FALSE
attr(is_writable_table, "fail") <- function(call, env) {
  paste0(
    deparse(call$x),
    " must be an object of class 'data.frame', 'RecordBatch', or 'Table', not '",
    class(env[[deparse(call$x)]])[[1]],
    "'."
  )
}

#' Recycle scalar values in a list of arrays
#'
#' @param arrays List of arrays
#' @return List of arrays with any vector/Scalar/Array/ChunkedArray values of length 1 recycled
#' @keywords internal
recycle_scalars <- function(arrays) {
  # Get lengths of items in arrays
  arr_lens <- map_int(arrays, NROW)

  is_scalar <- arr_lens == 1

  if (length(arrays) > 1 && any(is_scalar) && !all(is_scalar)) {

    # Recycling not supported for tibbles and data.frames
    if (all(map_lgl(arrays, ~ inherits(.x, "data.frame")))) {
      abort(c(
        "All input tibbles or data.frames must have the same number of rows",
        x = paste(
          "Number of rows in longest and shortest inputs:",
          oxford_paste(c(max(arr_lens), min(arr_lens)))
        )
      ))
    }

    max_array_len <- max(arr_lens)
    arrays[is_scalar] <- lapply(arrays[is_scalar], repeat_value_as_array, max_array_len)
  }
  arrays
}

#' Take an object of length 1 and repeat it.
#'
#' @param object Object of length 1 to be repeated - vector, `Scalar`, `Array`, or `ChunkedArray`
#' @param n Number of repetitions
#'
#' @return `Array` of length `n`
#'
#' @keywords internal
repeat_value_as_array <- function(object, n) {
  if (inherits(object, "ChunkedArray")) {
    return(Scalar$create(object$chunks[[1]])$as_array(n))
  }
  return(Scalar$create(object)$as_array(n))
}
