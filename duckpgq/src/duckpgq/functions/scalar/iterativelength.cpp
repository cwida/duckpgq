#include <duckpgq_extension.hpp>
#include "duckdb/main/client_data.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckpgq/common.hpp"
#include "duckpgq/duckpgq_functions.hpp"

namespace duckdb {

static bool IterativeLength(int64_t v_size, int64_t *v, vector<int64_t> &e,
                            vector<vector<set<int64_t>>> &parents_v,
                            vector<std::bitset<LANE_LIMIT>> &seen,
                            vector<std::bitset<LANE_LIMIT>> &visit,
                            vector<std::bitset<LANE_LIMIT>> &next) {
  bool change = false;
  for (auto i = 0; i < v_size; i++) {
    next[i] = 0;
  }

  for (auto lane = 0; lane < LANE_LIMIT; lane++) {
    for (auto i = 0; i < v_size; i++) {
      if (visit[i][lane]) {
        for (auto offset = v[i]; offset < v[i + 1]; offset++) {
          auto n = e[offset];
          if (parents_v[i][lane].find(n) == parents_v[i][lane].end()) {
            parents_v[i][lane].insert(n);
            next[n][lane] = true;
          }
        }
      }
    }
  }

  // for (auto i = 0; i < v_size; i++) {
  //   if (visit[i].any()) {
  //     for (auto offset = v[i]; offset < v[i + 1]; offset++) {
  //       auto n = e[offset];
  //       next[n] = next[n] | visit[i];
  //     }
  //   }
  // }
  for (auto i = 0; i < v_size; i++) {
    // next[i] = next[i] & ~seen[i];
    seen[i] = seen[i] | next[i];
    change |= next[i].any();
  }

  // vector<std::bitset<LANE_LIMIT>> next_next = vector<std::bitset<LANE_LIMIT>>(v_size, 0);
  // // If a vertex in next is a successor of other vertices in next, set it as unvisited
  // for (auto i = 0; i < v_size; i++) {
  //   if (next[i].any()) {
  //     for (auto offset = v[i]; offset < v[i + 1]; offset++) {
  //       auto n = e[offset];
  //       next_next[n] = next_next[n] | next[i];
  //     }
  //   }
  // }
  // for (auto i = 0; i < v_size; i++) {
  //   next[i] = next[i] & ~next_next[i];
  // }

  // for (auto i = 0; i < v_size; i++) {
  //   change |= next[i].any();
  // }
  
  return change;
}

static void IterativeLengthFunction(DataChunk &args, ExpressionState &state,
                                    Vector &result) {
  auto &func_expr = (BoundFunctionExpression &)state.expr;
  auto &info = (IterativeLengthFunctionData &)*func_expr.bind_info;
  auto duckpgq_state_entry = info.context.registered_state.find("duckpgq");
  if (duckpgq_state_entry == info.context.registered_state.end()) {
    //! Wondering how you can get here if the extension wasn't loaded, but
    //! leaving this check in anyways
    throw MissingExtensionException(
        "The DuckPGQ extension has not been loaded");
  }
  auto duckpgq_state =
      reinterpret_cast<DuckPGQState *>(duckpgq_state_entry->second.get());

  D_ASSERT(duckpgq_state->csr_list[info.csr_id]);

  if ((uint64_t)info.csr_id + 1 > duckpgq_state->csr_list.size()) {
    throw ConstraintException("Invalid ID");
  }
  auto csr_entry = duckpgq_state->csr_list.find((uint64_t)info.csr_id);
  if (csr_entry == duckpgq_state->csr_list.end()) {
    throw ConstraintException(
        "Need to initialize CSR before doing shortest path");
  }

  if (!(csr_entry->second->initialized_v && csr_entry->second->initialized_e)) {
    throw ConstraintException(
        "Need to initialize CSR before doing shortest path");
  }
  int64_t v_size = args.data[1].GetValue(0).GetValue<int64_t>();
  int64_t *v = (int64_t *)duckpgq_state->csr_list[info.csr_id]->v;
  vector<int64_t> &e = duckpgq_state->csr_list[info.csr_id]->e;

  // get src and dst vectors for searches
  auto &src = args.data[2];
  auto &dst = args.data[3];
  UnifiedVectorFormat vdata_src;
  UnifiedVectorFormat vdata_dst;
  src.ToUnifiedFormat(args.size(), vdata_src);
  dst.ToUnifiedFormat(args.size(), vdata_dst);
  auto src_data = (int64_t *)vdata_src.data;
  auto dst_data = (int64_t *)vdata_dst.data;

  // get lowerbound and upperbound
  auto &lower_bound = args.data[4];
  auto &upper_bound = args.data[5];
  UnifiedVectorFormat vdata_lower_bound;
  UnifiedVectorFormat vdata_upper_bound;
  lower_bound.ToUnifiedFormat(args.size(), vdata_lower_bound);
  upper_bound.ToUnifiedFormat(args.size(), vdata_upper_bound);
  auto lower_bound_data = (int64_t *)vdata_lower_bound.data;
  auto upper_bound_data = (int64_t *)vdata_upper_bound.data;

  ValidityMask &result_validity = FlatVector::Validity(result);

  // create result vector
  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto result_data = FlatVector::GetData<int64_t>(result);

  // create temp SIMD arrays
  vector<std::bitset<LANE_LIMIT>> seen(v_size);
  vector<std::bitset<LANE_LIMIT>> visit1(v_size);
  vector<std::bitset<LANE_LIMIT>> visit2(v_size);
  // vector<vector<int64_t>> level(v_size, std::vector<int64_t>(LANE_LIMIT, INT64_MAX));
  vector<vector<set<int64_t>>> parents_v(v_size, std::vector<set<int64_t>>(LANE_LIMIT));

  // maps lane to search number
  short lane_to_num[LANE_LIMIT];
  for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
    lane_to_num[lane] = -1; // inactive
  }

  idx_t started_searches = 0;
  while (started_searches < args.size()) {

    // empty visit vectors
    for (auto i = 0; i < v_size; i++) {
      seen[i] = 0;
      visit1[i] = 0;
    }

    // add search jobs to free lanes
    uint64_t active = 0;
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      lane_to_num[lane] = -1;
      while (started_searches < args.size()) {
        int64_t search_num = started_searches++;
        int64_t src_pos = vdata_src.sel->get_index(search_num);
        int64_t dst_pos = vdata_dst.sel->get_index(search_num);
        if (!vdata_src.validity.RowIsValid(src_pos)) {
          result_validity.SetInvalid(search_num);
          result_data[search_num] = (int64_t)-1; /* no path */
        } else if (src_data[src_pos] == dst_data[dst_pos]) {
          result_data[search_num] =
              (int64_t)0; // path of length 0 does not require a search
        } else {
          result_data[search_num] = (int64_t)-1; /* initialize to no path */
          seen[src_data[src_pos]][lane] = true;
          visit1[src_data[src_pos]][lane] = true;
          lane_to_num[lane] = search_num; // active lane
          active++;
          break;
        }
      }
    }

    // make passes while a lane is still active
    for (int64_t iter = 1; active; iter++) {
      // if (!IterativeLength(v_size, v, e, seen, (iter & 1) ? visit1 : visit2,
      //                      (iter & 1) ? visit2 : visit1)) {
      //   break;
      // }
      bool stop = !IterativeLength(v_size, v, e, parents_v, seen, (iter & 1) ? visit1 : visit2,
                                   (iter & 1) ? visit2 : visit1);
      // detect lanes that finished
      for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
        int64_t search_num = lane_to_num[lane];
        if (search_num >= 0) { // active lane
          int64_t dst_pos = vdata_dst.sel->get_index(search_num);
          if (seen[dst_data[dst_pos]][lane]){

            // check if the path length is within bounds
            // bound vector is either a constant or a flat vector
            if (lower_bound.GetVectorType() == VectorType::CONSTANT_VECTOR ? 
                iter < lower_bound_data[0] : iter < lower_bound_data[dst_pos]) {
              // when reach the destination too early, treat destination as null
              // looks like the graph does not have that vertex
              seen[dst_data[dst_pos]][lane] = false;
              (iter & 1) ? visit2[dst_data[dst_pos]][lane] = false
                         : visit1[dst_data[dst_pos]][lane] = false;
              continue;
            } else if (upper_bound.GetVectorType() == VectorType::CONSTANT_VECTOR ? 
                iter > upper_bound_data[0] : iter > upper_bound_data[dst_pos]) {
              result_validity.SetInvalid(search_num);
              result_data[search_num] = (int64_t)-1; /* no path */
            } else {
              result_data[search_num] =
                  iter;               /* found at iter => iter = path length */
            }
            lane_to_num[lane] = -1; // mark inactive
            active--;
          }
        }
      }
      if (stop) {
        break;
      }
    }

    // no changes anymore: any still active searches have no path
    for (int64_t lane = 0; lane < LANE_LIMIT; lane++) {
      int64_t search_num = lane_to_num[lane];
      if (search_num >= 0) { // active lane
        result_validity.SetInvalid(search_num);
        result_data[search_num] = (int64_t)-1; /* no path */
        lane_to_num[lane] = -1;                // mark inactive
      }
    }
  }
  duckpgq_state->csr_to_delete.insert(info.csr_id);
}

CreateScalarFunctionInfo DuckPGQFunctions::GetIterativeLengthFunction() {
  auto fun = ScalarFunction("iterativelength",
                            {LogicalType::INTEGER, LogicalType::BIGINT,
                             LogicalType::BIGINT, LogicalType::BIGINT,
                             LogicalType::BIGINT, LogicalType::BIGINT},
                            LogicalType::BIGINT, IterativeLengthFunction,
                            IterativeLengthFunctionData::IterativeLengthBind);
  return CreateScalarFunctionInfo(fun);
}

} // namespace duckdb
