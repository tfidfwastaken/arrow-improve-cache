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

#include <stdlib.h>
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "benchmark/benchmark.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tests/timed_evaluate.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

static void TimedLiteralAdd(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto res_0 = field("res0", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);

  auto configuration = TestConfiguration();
  configuration->set_optimize(true);
  ARROW_LOG(INFO) << "config: " << configuration->optimize();

  Int32DataGenerator data_generator;
  auto batch_size = 1 * MILLION;
  std::vector<int32_t> data = GenerateData<int32_t>(batch_size, data_generator);
  std::vector<bool> validity(batch_size, true);
  ArrayPtr col_data =
    MakeArrowArray<arrow::Int32Type, int32_t>(schema->field(0)->type(), data, validity);
  for (auto _ : state) {
    std::shared_ptr<Projector> projector1;
    auto status1 = Projector::Make(schema, {expr_0}, configuration, &projector1);

    ProjectEvaluator evaluator1(projector1);

    std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, batch_size, {col_data});
    status1 = evaluator1.Evaluate(*batch, pool_);
    ASSERT_OK(status1);
  }
}

static void TimedLiteralAddNext(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto res_1 = field("res1", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((int32_t)3);
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, int32());
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  ARROW_LOG(INFO) << "config: " << configuration->optimize();

  Int32DataGenerator data_generator;
  auto batch_size = 1 * MILLION;
  std::vector<int32_t> data = GenerateData<int32_t>(batch_size, data_generator);
  std::vector<bool> validity(batch_size, true);
  ArrayPtr col_data =
    MakeArrowArray<arrow::Int32Type, int32_t>(schema->field(0)->type(), data, validity);

  for (auto _ : state) {
    std::shared_ptr<Projector> projector2;
    auto status2 = Projector::Make(schema, {expr_1}, configuration, &projector2);

    ProjectEvaluator evaluator2(projector2);

    std::shared_ptr<arrow::RecordBatch> batch =
      arrow::RecordBatch::Make(schema, batch_size, {col_data});
    status2 = evaluator2.Evaluate(*batch, pool_);
    ASSERT_OK(status2);
  }
}

static void TimedQueryStreamRandomizedLiterals(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto res_1 = field("res1", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);

  auto configuration = TestConfiguration();
  ARROW_LOG(INFO) << "config: " << configuration->optimize();
  //configuration->set_optimize(true);

  Int32DataGenerator data_generator;
  auto batch_size = 1 * MILLION;
  std::vector<int32_t> data = GenerateData<int32_t>(batch_size, data_generator);
  std::vector<bool> validity(batch_size, true);
  ArrayPtr col_data =
    MakeArrowArray<arrow::Int32Type, int32_t>(schema->field(0)->type(), data, validity);

  for (auto _ : state) {
    for (int i = 0; i < 20; i++) {
      auto int_literal = TreeExprBuilder::MakeLiteral((int32_t)i);
      auto add_a_i = TreeExprBuilder::MakeFunction("subtract", {node_a, int_literal}, int32());
      auto expr_1 = TreeExprBuilder::MakeExpression(add_a_i, res_1);
      std::shared_ptr<Projector> projector;
      auto status = Projector::Make(schema, {expr_1}, configuration, &projector);

      ProjectEvaluator evaluator(projector);

      std::shared_ptr<arrow::RecordBatch> batch =
        arrow::RecordBatch::Make(schema, batch_size, {col_data});
      status = evaluator.Evaluate(*batch, pool_);
      ASSERT_OK(status);
    }
  }
}

static void TimedQueryStreamRandomizedMultiLiterals(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto res_1 = field("res1", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);

  auto configuration = TestConfiguration();
  ARROW_LOG(INFO) << "config: " << configuration->optimize();
  //configuration->set_optimize(true);

  Int32DataGenerator data_generator;
  auto batch_size = 1 * MILLION;
  std::vector<int32_t> data = GenerateData<int32_t>(batch_size, data_generator);
  std::vector<bool> validity(batch_size, true);
  ArrayPtr col_data =
    MakeArrowArray<arrow::Int32Type, int32_t>(schema->field(0)->type(), data, validity);

  for (auto _ : state) {
    for (int i = 0; i < 20; i++) {
      auto int_literal = TreeExprBuilder::MakeLiteral((int32_t)i);
      auto int_other_literal = TreeExprBuilder::MakeLiteral((int32_t)(20 - i));
      auto sub_a_i = TreeExprBuilder::MakeFunction("subtract", {node_a, int_literal}, int32());
      auto add_a_other = TreeExprBuilder::MakeFunction("add", {sub_a_i, int_other_literal}, int32());
      auto expr_1 = TreeExprBuilder::MakeExpression(add_a_other, res_1);
      std::shared_ptr<Projector> projector;
      auto status = Projector::Make(schema, {expr_1}, configuration, &projector);

      ProjectEvaluator evaluator(projector);

      std::shared_ptr<arrow::RecordBatch> batch =
        arrow::RecordBatch::Make(schema, batch_size, {col_data});
      status = evaluator.Evaluate(*batch, pool_);
      ASSERT_OK(status);
    }
  }
}

BENCHMARK(TimedLiteralAdd)->Iterations(1)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedLiteralAddNext)->Iterations(1)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedQueryStreamRandomizedLiterals)->Iterations(1)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedQueryStreamRandomizedMultiLiterals)->Iterations(1)->Unit(benchmark::kMicrosecond);

}  // namespace gandiva
