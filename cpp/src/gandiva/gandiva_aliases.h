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

#pragma once

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "gandiva/literal_holder.h"
#include "arrow/type_fwd.h"

namespace gandiva {

class Dex;
using DexPtr = std::shared_ptr<Dex>;
using DexVector = std::vector<std::shared_ptr<Dex>>;

class ValueValidityPair;
using ValueValidityPairPtr = std::shared_ptr<ValueValidityPair>;
using ValueValidityPairVector = std::vector<ValueValidityPairPtr>;

class FieldDescriptor;
using FieldDescriptorPtr = std::shared_ptr<FieldDescriptor>;

class FuncDescriptor;
using FuncDescriptorPtr = std::shared_ptr<FuncDescriptor>;

class LValue;
using LValuePtr = std::shared_ptr<LValue>;

class Expression;
using ExpressionPtr = std::shared_ptr<Expression>;
using ExpressionVector = std::vector<ExpressionPtr>;

class Condition;
using ConditionPtr = std::shared_ptr<Condition>;

class Node;
using NodePtr = std::shared_ptr<Node>;
using NodeVector = std::vector<std::shared_ptr<Node>>;

class EvalBatch;
using EvalBatchPtr = std::shared_ptr<EvalBatch>;

class FunctionSignature;
using FuncSignaturePtr = std::shared_ptr<FunctionSignature>;
using FuncSignatureVector = std::vector<FuncSignaturePtr>;

using LiteralParamPair = std::pair<arrow::Type::type, LiteralHolder>;
using LiteralParamPairVector = std::vector<LiteralParamPair>;
}  // namespace gandiva
