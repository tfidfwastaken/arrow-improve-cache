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

#include "gandiva/nextractor.h"

namespace gandiva {

Status Nextractor::Extract(const ExpressionPtr& expr) {
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
  type_ = node.return_type()->id();
  query_param_holder_ = node.holder();
  found_literal_ = false;
  return Status::OK();
}

Status Nextractor::Visit(const BooleanNode& node) {
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
