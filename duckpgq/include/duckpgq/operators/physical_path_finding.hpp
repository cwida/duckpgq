//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_path_finding.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {
class PhysicalPathFinding : public CachingPhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;

public:
  PhysicalPathFinding(LogicalExtensionOperator &op,
                      unique_ptr<PhysicalOperator> left,
                      unique_ptr<PhysicalOperator> right);

  // vector<LogicalType> join_key_types;
  // vector<vector<BoundOrderByNode>> lhs_orders;
  // vector<vector<BoundOrderByNode>> rhs_orders;

public:
  // CachingOperator Interface
  OperatorResultType ExecuteInternal(ExecutionContext &context,
                                     DataChunk &input, DataChunk &chunk,
                                     GlobalOperatorState &gstate,
                                     OperatorState &state) const override;

public:
  // Source interface
  unique_ptr<LocalSourceState>
  GetLocalSourceState(ExecutionContext &context,
                      GlobalSourceState &gstate) const override;
  unique_ptr<GlobalSourceState>
  GetGlobalSourceState(ClientContext &context) const override;
  SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
                           OperatorSourceInput &input) const override;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return true; }

public:
  // Sink Interface
  unique_ptr<GlobalSinkState>
  GetGlobalSinkState(ClientContext &context) const override;
  unique_ptr<LocalSinkState>
  GetLocalSinkState(ExecutionContext &context) const override;
  SinkResultType Sink(ExecutionContext &context, DataChunk &chunk,
                      OperatorSinkInput &input) const override;
  SinkCombineResultType Combine(ExecutionContext &context,
                                OperatorSinkCombineInput &input) const override;
  SinkFinalizeType Finalize(Pipeline &pipeline, Event &event,
                            ClientContext &context,
                            OperatorSinkFinalizeInput &input) const override;

  bool IsSink() const override { return true; }
  bool ParallelSink() const override { return true; }

  void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
