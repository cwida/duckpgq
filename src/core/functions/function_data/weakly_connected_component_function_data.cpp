#include "duckpgq/core/functions/function_data/weakly_connected_component_function_data.hpp"

namespace duckpgq {

namespace core {

WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentFunctionData(
    ClientContext &context, int32_t csr_id)
    : context(context), csr_id(csr_id) {
  componentId = vector<int64_t>();
  component_id_initialized = false;
}

WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentFunctionData(
    ClientContext &context, int32_t csr_id, const vector<int64_t> &componentId)
    : context(context), csr_id(csr_id), componentId(componentId) {
  component_id_initialized = false;
}

unique_ptr<FunctionData>
WeaklyConnectedComponentFunctionData::WeaklyConnectedComponentBind(
    ClientContext &context, ScalarFunction &bound_function,
    vector<unique_ptr<Expression>> &arguments) {
  if (!arguments[0]->IsFoldable()) {
    throw InvalidInputException("Id must be constant.");
  }

  int32_t csr_id = ExpressionExecutor::EvaluateScalar(context, *arguments[0])
                       .GetValue<int32_t>();

  return make_uniq<WeaklyConnectedComponentFunctionData>(context, csr_id);
}

unique_ptr<FunctionData> WeaklyConnectedComponentFunctionData::Copy() const {
  auto result = make_uniq<WeaklyConnectedComponentFunctionData>(context, csr_id,
                                                                componentId);
  result->component_id_initialized = component_id_initialized;
  return std::move(result);
}
bool WeaklyConnectedComponentFunctionData::Equals(
    const FunctionData &other_p) const {
  auto &other = (const WeaklyConnectedComponentFunctionData &)other_p;
  if (csr_id != other.csr_id) {
    return false;
  }
  if (componentId != other.componentId) {
    return false;
  }

  return true;
}

} // namespace core

} // namespace duckpgq
