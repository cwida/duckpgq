#pragma once

#include "duckpgq/common.hpp"

#include <duckpgq/core/operator/physical_path_finding_operator.hpp>

namespace duckpgq {
namespace core {

class PhysicalCSREdgeCreationTask : public ExecutorTask {
public:
  PhysicalCSREdgeCreationTask(shared_ptr<Event> event_p, ClientContext &context,
                              PathFindingGlobalState &state);

  TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
  ClientContext &context;
  PathFindingGlobalState &state;
};


} // namespace core
} // namespace duckpgq