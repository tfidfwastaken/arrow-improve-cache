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
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/logging.h"

#include "gandiva/projector.h"
#include "gandiva/filter.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::float64;
using arrow::int8;
using arrow::int32;
using arrow::int64;
using arrow::uint8;
using arrow::uint32;
using arrow::uint64;

class TestParamLiteral : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = arrow::default_memory_pool();
  }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestParamLiteral, TestAddLiteralInt64) {
  // schema for input fields
  auto field_a = field("a", int64());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", int64());
  auto res_1 = field("res1", int64());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((int64_t)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((int64_t)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, int64());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, int64());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayInt64({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt64({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayInt64({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralInt32) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", int32());
  auto res_1 = field("res1", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((int32_t)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, int32());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, int32());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayInt32({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt32({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayInt32({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralInt8) {
  // schema for input fields
  auto field_a = field("a", int8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", int8());
  auto res_1 = field("res1", int8());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((int8_t)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((int8_t)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, int8());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, int8());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayInt8({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayInt8({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayInt8({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralUInt8) {
  // schema for input fields
  auto field_a = field("a", uint8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", uint8());
  auto res_1 = field("res1", uint8());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((uint8_t)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((uint8_t)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, uint8());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, uint8());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUint8({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayUint8({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayUint8({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralUInt64) {
  // schema for input fields
  auto field_a = field("a", uint64());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", uint64());
  auto res_1 = field("res1", uint64());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((uint64_t)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((uint64_t)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, uint64());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, uint64());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUint64({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayUint64({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayUint64({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralDouble) {
  // schema for input fields
  auto field_a = field("a", float64());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", float64());
  auto res_1 = field("res1", float64());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((double)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((double)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, float64());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, float64());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayFloat64({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayFloat64({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayFloat64({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralFloat) {
  // schema for input fields
  auto field_a = field("a", float32());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", float32());
  auto res_1 = field("res1", float32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((float)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((float)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, float32());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, float32());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayFloat32({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayFloat32({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayFloat32({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestAddLiteralUInt32) {
  // schema for input fields
  auto field_a = field("a", uint32());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", uint32());
  auto res_1 = field("res1", uint32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((uint32_t)2);
  auto int_literal3 = TreeExprBuilder::MakeLiteral((uint32_t)3);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, uint32());
  auto add_a_3 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal3}, uint32());
  auto expr_0 = TreeExprBuilder::MakeExpression(add_a_2, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(add_a_3, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayUint32({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayUint32({3, 4, 5, 6, 7}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayUint32({4, 5, 6, 7, 8}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, MultiLiteralTest) {
  // schema for input fields
  auto field_a = field("a", int32());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res_0 = field("res0", boolean());
  auto res_1 = field("res1", boolean());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto int_literal2 = TreeExprBuilder::MakeLiteral((int32_t)2);
  auto int_literal5 = TreeExprBuilder::MakeLiteral((int32_t)5);
  auto add_a_2 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal2}, int32());
  auto less_than_a_5 = TreeExprBuilder::MakeFunction("less_than", {add_a_2, int_literal5}, boolean());
  auto int_literal6 = TreeExprBuilder::MakeLiteral((int32_t)6);
  auto int_literal10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto add_a_6 = TreeExprBuilder::MakeFunction("add", {node_a, int_literal6}, int32());
  auto less_than_a_10 = TreeExprBuilder::MakeFunction("less_than", {add_a_6, int_literal10}, boolean());
  auto expr_0 = TreeExprBuilder::MakeExpression(less_than_a_5, res_0);
  auto expr_1 = TreeExprBuilder::MakeExpression(less_than_a_10, res_1);

  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr_0}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Create a row-batch with some sample data
  int num_records = 5;
  auto array_a = MakeArrowArrayInt32({1, 2, 3, 4, 5}, {true, true, true, true, true});

  // expected output
  auto exp_1 = MakeArrowArrayBool({true, true, false, false, false}, {true, true, true, true, true});
  auto exp_2 = MakeArrowArrayBool({true, true, true, false, false}, {true, true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression 1
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  std::shared_ptr<Projector> projector1;
  status = Projector::Make(schema, {expr_1}, TestConfiguration(), &projector1);
  EXPECT_TRUE(status.ok()) << status.message();

  EXPECT_TRUE(projector.get() == projector1.get());

  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch, pool_, &outputs1);
  EXPECT_TRUE(status.ok()) << status.message();

  //  Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp_1, outputs.at(0));
  EXPECT_ARROW_ARRAY_EQUALS(exp_2, outputs1.at(0));
}

TEST_F(TestParamLiteral, TestBoolean) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto literal_5 = TreeExprBuilder::MakeLiteral((int32_t)5);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto greater_than_5 = TreeExprBuilder::MakeFunction("greater_than", {sum_func, literal_5},
                                                    arrow::boolean());
  auto node_or = TreeExprBuilder::MakeOr({less_than_10, greater_than_5});
  auto condition = TreeExprBuilder::MakeCondition(node_or);

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, TestConfiguration(), &filter);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 3, 4, 6}, {true, true, true, false, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 6, 17, 3}, {true, true, false, true, true});
  // expected output (indices for which condition matches)
  auto exp = MakeArrowArrayUint16({0, 1, 4});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, selection_vector->ToArray());
}

// NEEDSWORK: Fails due to like optimizations
TEST_F(TestParamLiteral, TestLike) {
  // schema for input fields
  auto field_a = field("a", arrow::utf8());
  auto schema = arrow::schema({field_a});

  // output fields
  auto res = field("res", boolean());

  // build expressions.
  // like(literal(s), a)

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto literal_s = TreeExprBuilder::MakeStringLiteral("%spark%");
  auto is_like = TreeExprBuilder::MakeFunction("like", {node_a, literal_s}, boolean());
  auto expr = TreeExprBuilder::MakeExpression(is_like, res);

  // Build a projector for the expressions.
  auto configuration = TestConfiguration();
  configuration->set_optimize(false);
  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {expr}, configuration, &projector);
  EXPECT_TRUE(status.ok()) << status.message();

  // Create a row-batch with some sample data
  int num_records = 4;
  auto array_a = MakeArrowArrayUtf8({"park", "sparkle", "bright spark and fire", "spark"},
                                    {true, true, true, true});

  // expected output
  auto exp = MakeArrowArrayBool({false, true, true, true}, {true, true, true, true});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array_a});

  // Evaluate expression
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, pool_, &outputs);
  EXPECT_TRUE(status.ok()) << status.message();

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}

}  // namespace gandiva
