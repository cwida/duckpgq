#pragma once
#include "duckpgq/common.hpp"

namespace duckpgq {

namespace core {

struct CSROperatorFunctionData : FunctionData {
  CSROperatorFunctionData(ClientContext &context);
  unique_ptr<FunctionData> Copy() const override;
  bool Equals(const FunctionData &other_p) const override;
  // static unique_ptr<FunctionData>
  // CSREdgeBind(ClientContext &context, ScalarFunction &bound_function,
  //             vector<unique_ptr<Expression>> &arguments);
  // static unique_ptr<FunctionData>
  // CSRBind(ClientContext &context, ScalarFunction &bound_function,
  //         vector<unique_ptr<Expression>> &arguments);

  ClientContext &context;
};

} // namespace core

} // namespace duckpgq