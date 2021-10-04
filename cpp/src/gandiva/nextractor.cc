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

#include "arrow/util/variant.h"
#include "gandiva/nextractor.h"

namespace gandiva {

Status Nextractor::Extract(const ExpressionPtr& expr) {
  query_params_.clear();
  Node& root = *expr->root();
  ARROW_RETURN_NOT_OK(root.Accept(*this));
  return Status::OK();
}

Status Nextractor::Visit(const FieldNode& node) {
  return Status::OK();
}

Status Nextractor::Visit(const FunctionNode& node) {
  for (auto& child : node.children()) {
    ARROW_RETURN_NOT_OK(child->Accept(*this));
  }
  return Status::OK();
}

Status Nextractor::Visit(const IfNode& node) {
  ARROW_RETURN_NOT_OK(node.condition()->Accept(*this));
  ARROW_RETURN_NOT_OK(node.then_node()->Accept(*this));
  ARROW_RETURN_NOT_OK(node.else_node()->Accept(*this));
  return Status::OK();
}

Status Nextractor::Visit(const LiteralNode& node) {
  auto type = node.return_type()->id();
  auto value = node.holder();
  auto pair = std::make_pair(type, value);
  if (type == arrow::Type::type::STRING ||
      type == arrow::Type::type::BINARY) {
    auto lentype = arrow::Type::type::INT32;
    int32_t len = arrow::util::get<std::string>(node.holder()).length();
    auto lenpair = std::make_pair(lentype, LiteralHolder(len));
    query_params_.push_back(lenpair);
  }
  query_params_.push_back(pair);
  found_literal_ = false;
  return Status::OK();
}

Status Nextractor::Visit(const BooleanNode& node) {
  for (auto& child : node.children()) {
    ARROW_RETURN_NOT_OK(child->Accept(*this));
  }
  return Status::OK();
}

Status Nextractor::Visit(const InExpressionNode<int32_t>& node) {
  return Status::OK();
}

Status Nextractor::Visit(const InExpressionNode<int64_t>& node) {
  return Status::OK();
}
Status Nextractor::Visit(const InExpressionNode<float>& node) {
  return Status::OK();
}
Status Nextractor::Visit(const InExpressionNode<double>& node) {
  return Status::OK();
}

Status Nextractor::Visit(const InExpressionNode<gandiva::DecimalScalar128>& node) {
  return Status::OK();
}

Status Nextractor::Visit(const InExpressionNode<std::string>& node) {
  return Status::OK();
}

} // namespace gandiva
