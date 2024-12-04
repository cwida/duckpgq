//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckpgq/core/operator/physical_path_finding_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/common/types/row/row_layout.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/core/utils/duckpgq_barrier.hpp"
#include "duckpgq/core/utils/duckpgq_path_reconstruction.hpp"

#include <duckpgq/core/utils/compressed_sparse_row.hpp>

namespace duckpgq {

namespace core {
class GlobalBFSState;

class PhysicalPathFinding : public PhysicalComparisonJoin {
#define LANE_LIMIT 512

public:
  PhysicalPathFinding(LogicalExtensionOperator &op,
                      unique_ptr<PhysicalOperator> pairs,
                      unique_ptr<PhysicalOperator> csr);

  static constexpr PhysicalOperatorType TYPE =
      PhysicalOperatorType::EXTENSION;
  vector<unique_ptr<Expression>> expressions;
  string mode; // "iterativelength" or "shortestpath"

public:
  InsertionOrderPreservingMap<string> ParamsToString() const override;

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

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PathFindingLocalSinkState : public LocalSinkState {
public:
  PathFindingLocalSinkState(ClientContext &context, const PhysicalPathFinding &op);

  void Sink(DataChunk &input, idx_t child);

  ColumnDataCollection local_pairs;

};

class GlobalBFSState : public enable_shared_from_this<GlobalBFSState> {

public:
  GlobalBFSState(unique_ptr<ColumnDataCollection> &pairs_, CSR* csr_, int64_t vsize_,
                 idx_t num_threads_, string mode_, ClientContext &context_);

  void ScheduleBFSEvent(Pipeline &pipeline, Event &event, const PhysicalPathFinding *op);

  void Clear();

  void CreateTasks();
  shared_ptr<pair<idx_t, idx_t>> FetchTask();      // Function to fetch a task
  void ResetTaskIndex();

  pair<idx_t, idx_t> BoundaryCalculation(idx_t worker_id) const;
  CSR *csr;
  unique_ptr<ColumnDataCollection> &pairs; // (src, dst) pairs
  unique_ptr<DataChunk> current_pairs_batch;
  const PhysicalPathFinding *op;
  int64_t iter;
  int64_t v_size; // Number of vertices
  bool change;
  idx_t started_searches; // Number of started searches in current batch
  int64_t total_len;
  int64_t *src;
  int64_t *dst;
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  int64_t lane_to_num[LANE_LIMIT];
  idx_t active = 0;
  unique_ptr<ColumnDataCollection> results; // results of (src, dst, path-finding)
  ColumnDataScanState result_scan_state;
  ColumnDataScanState input_scan_state;
  ColumnDataAppendState append_state;
  ClientContext &context;
  vector<std::bitset<LANE_LIMIT>> seen;
  vector<std::bitset<LANE_LIMIT>> visit1;
  vector<std::bitset<LANE_LIMIT>> visit2;
  vector<std::array<ve, LANE_LIMIT>> parents_ve;

  idx_t total_pairs_processed;
  idx_t num_threads;
  idx_t scheduled_threads;

  // task_queues[workerId] = {curTaskIdx, queuedTasks}
  // queuedTasks[curTaskIx] = {start, end}
  vector<pair<idx_t, idx_t>> global_task_queue;
  std::mutex queue_mutex; // Mutex for synchronizing access
  std::condition_variable queue_cv; // Condition variable for task availability
  size_t current_task_index = 0; // Index to track the current task
  int64_t split_size = 256;

  unique_ptr<Barrier> barrier;

  // lock for next
  mutable vector<mutex> element_locks;

  string mode;
};

class PathFindingGlobalSinkState : public GlobalSinkState {
public:
  PathFindingGlobalSinkState(ClientContext &context,
                         const PhysicalPathFinding &op);

  void Sink(DataChunk &input, PathFindingLocalSinkState &lstate);

  // pairs is a 2-column table with src and dst
  unique_ptr<ColumnDataCollection> global_pairs;
  unique_ptr<ColumnDataCollection> global_csr_column_data;
  shared_ptr<GlobalBFSState> global_bfs_state;
  CSR* csr;
  int32_t csr_id;
  size_t child;
  string mode;
  ClientContext &context_;
  idx_t num_threads;
};

} // namespace core
} // namespace duckpgq

