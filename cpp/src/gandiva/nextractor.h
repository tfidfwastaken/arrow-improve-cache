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

#include <cmath>
#include <string>
#include <utility>

#include "arrow/status.h"

#include "arrow/util/logging.h"
#include "gandiva/expression.h"
#include "gandiva/llvm_generator.h"
#include "gandiva/node.h"
#include "gandiva/node_visitor.h"

namespace gandiva {

/// \brief Nextractor
class Nextractor : public NodeVisitor {
 public:
  explicit Nextractor() : found_literal_(false) {}
  Status Extract(const ExpressionPtr& expr);
  Status Visit(const FieldNode& node);
  Status Visit(const FunctionNode& node);
  Status Visit(const IfNode& node);
  Status Visit(const LiteralNode& node);
  Status Visit(const BooleanNode& node);
  Status Visit(const InExpressionNode<int32_t>& node);
  Status Visit(const InExpressionNode<int64_t>& node);
  Status Visit(const InExpressionNode<float>& node);
  Status Visit(const InExpressionNode<double>& node);
  Status Visit(const InExpressionNode<gandiva::DecimalScalar128>& node);
  Status Visit(const InExpressionNode<std::string>& node);

  std::vector<std::pair<arrow::Type::type, LiteralHolder>> query_params() { return query_params_; }
  bool literal_found() { return found_literal_; }

 private:
  std::vector<std::pair<arrow::Type::type, LiteralHolder>> query_params_;
  bool found_literal_;
};

}  // namespace gandiva
