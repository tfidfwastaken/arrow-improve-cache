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

#include "gandiva/dextractor.h"
#include "gandiva/dex.h"
#include "gandiva/llvm_types.h"
#include "gandiva/llvm_generator.h"

#include "arrow/util/hash_util.h"

namespace gandiva {

Dextractor::Dextractor(LLVMGenerator* generator)
  : generator_(generator) {}

void Dextractor::Visit(const LiteralDex& dex) {
  arrow::Type::type arrow_type_id = dex.type()->id();
  LLVMTypes* types_module = generator_->types();
  switch(arrow_type_id) {
    case arrow::Type::INT32: {
      int32_t value = arrow::util::get<int32_t>(dex.holder());
      ARROW_LOG(INFO) << value;
      arrow::internal::hash_combine(hash_key_, value);
      type_ = types_module->IRType(arrow_type_id);
      arrow::internal::hash_combine(hash_key_, arrow_type_id);
      holder_ = value;
      break;
    }
    case arrow::Type::INT64: {
      int64_t value = arrow::util::get<int64_t>(dex.holder());
      arrow::internal::hash_combine(hash_key_, value);
      type_ = types_module->IRType(arrow_type_id);
      arrow::internal::hash_combine(hash_key_, arrow_type_id);
      holder_ = value;
      break;
    }
    default:
      ARROW_LOG(DEBUG) << "unhandled type: " << arrow_type_id;
      DCHECK(0);
  }
}

void Dextractor::Visit(const VectorReadFixedLenValueDex &dex) {
  arrow::internal::hash_combine(hash_key_, dex.FieldType()->id());
  arrow::internal::hash_combine(hash_key_, dex.FieldName());
}

void Dextractor::Visit(const VectorReadVarLenValueDex &dex) {
  arrow::internal::hash_combine(hash_key_, dex.FieldType()->id());
  arrow::internal::hash_combine(hash_key_, dex.OffsetsIdx());
  arrow::internal::hash_combine(hash_key_, dex.DataIdx());
  arrow::internal::hash_combine(hash_key_, dex.FieldName());
}

void Dextractor::Visit(const VectorReadValidityDex &dex) {
  arrow::internal::hash_combine(hash_key_, dex.FieldType()->id());
  arrow::internal::hash_combine(hash_key_, dex.ValidityIdx());
  arrow::internal::hash_combine(hash_key_, dex.FieldName());
}

void Dextractor::Visit(const LocalBitMapValidityDex& dex) {
  arrow::internal::hash_combine(hash_key_, dex.local_bitmap_idx());
}

void Dextractor::Visit(const TrueDex &dex) {
  arrow::internal::hash_combine(hash_key_, generator_->types()->true_constant());
}

void Dextractor::Visit(const FalseDex &dex) {
  arrow::internal::hash_combine(hash_key_, generator_->types()->false_constant());
}

void Dextractor::VisitFunc(const FuncDescriptorPtr& descriptor,
                           const ValueValidityPairVector& args) {
  arrow::internal::hash_combine(hash_key_, descriptor->name());
  arrow::internal::hash_combine(hash_key_, descriptor->return_type()->id());

  for (auto type : descriptor->params()) {
    arrow::internal::hash_combine(hash_key_, type->id());
  }

  for (auto& pair : args) {
    DexPtr value_expr = pair->value_expr();
    value_expr->Accept(*this);
  }
}

void Dextractor::Visit(const NonNullableFuncDex& dex) {
  VisitFunc(dex.func_descriptor(), dex.args());
}

void Dextractor::Visit(const NullableNeverFuncDex& dex) {
  VisitFunc(dex.func_descriptor(), dex.args());
}

void Dextractor::Visit(const NullableInternalFuncDex& dex) {
  VisitFunc(dex.func_descriptor(), dex.args());
}

void Dextractor::AcceptChildren(const ValueValidityPair& vv) {
  DexPtr value_expr = vv.value_expr();
  value_expr->Accept(*this);
}

void Dextractor::Visit(const IfDex& dex) {
  AcceptChildren(dex.condition_vv());
  AcceptChildren(dex.then_vv());
  AcceptChildren(dex.else_vv());
}

void Dextractor::Visit(const BooleanAndDex& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const BooleanOrDex& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const InExprDexBase<int32_t>& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const InExprDexBase<int64_t>& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const InExprDexBase<float>& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const InExprDexBase<double>& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const InExprDexBase<DecimalScalar128>& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

void Dextractor::Visit(const InExprDexBase<std::string>& dex) {
  for (auto& pair : dex.args())
    AcceptChildren(*pair);
}

} // namespace gandiva
