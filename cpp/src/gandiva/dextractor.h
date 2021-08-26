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

#include <cmath>
#include <string>
#include <map>

#include <llvm/IR/Module.h>

#include "arrow/util/variant.h"

#include "gandiva/decimal_scalar.h"
#include "gandiva/dex_visitor.h"
#include "gandiva/literal_holder.h"
#include "gandiva/value_validity_pair.h"
#include "gandiva/visibility.h"

namespace gandiva {

class LLVMGenerator;
class VectorReadValidityDex;
class VectorReadFixedLenValueDex;
class VectorReadVarLenValueDex;
class LocalBitMapValidityDex;
class LiteralDex;
class TrueDex;
class FalseDex;
class NonNullableFuncDex;
class NullableNeverFuncDex;
class NullableInternalFuncDex;
class IfDex;
class BooleanAndDex;
class BooleanOrDex;
template <typename Type>
class InExprDexBase;

/// \brief Visitor for decomposed expression.
  class GANDIVA_EXPORT Dextractor : public DexVisitor {
 public:
  Dextractor(LLVMGenerator* generator);
  void Visit(const VectorReadValidityDex& dex);
  void Visit(const VectorReadFixedLenValueDex& dex);
  void Visit(const VectorReadVarLenValueDex& dex);
  void Visit(const LocalBitMapValidityDex& dex);
  void Visit(const TrueDex& dex);
  void Visit(const FalseDex& dex);
  void Visit(const LiteralDex& dex);
  void Visit(const NonNullableFuncDex& dex);
  void Visit(const NullableNeverFuncDex& dex);
  void Visit(const NullableInternalFuncDex& dex);
  void Visit(const IfDex& dex);
  void Visit(const BooleanAndDex& dex);
  void Visit(const BooleanOrDex& dex);
  void Visit(const InExprDexBase<int32_t>& dex);
  void Visit(const InExprDexBase<int64_t>& dex);
  void Visit(const InExprDexBase<float>& dex);
  void Visit(const InExprDexBase<double>& dex);
  void Visit(const InExprDexBase<gandiva::DecimalScalar128>& dex);
  void Visit(const InExprDexBase<std::string>& dex);

  llvm::Type* type() { return type_; }
  size_t hash_key() { return hash_key_; }
  LiteralHolder holder() { return holder_; }

 private:
  void VisitFunc(const FuncDescriptorPtr& descriptor, const ValueValidityPairVector& args);
  void AcceptChildren(const ValueValidityPair& vv);
  LLVMGenerator* generator_;
  llvm::Type* type_;
  size_t hash_key_;
  LiteralHolder holder_;
  // future plans
  // std::map<size_t, map<size_t, std::pair<arrow::Type:type, DexBaseType>>>
};

} // namespace gandiva
