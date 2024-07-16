/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include <algorithm>
#include "common/log/log.h"
#include "sql/operator/group_by_vec_physical_operator.h"
#include "sql/expr/aggregate_state.h"
#include "sql/expr/expression_tuple.h"
#include "sql/expr/composite_tuple.h"

using namespace std;
using namespace common;

GroupByVecPhysicalOperator::GroupByVecPhysicalOperator(
    std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions)
{
  group_by_expressions_ = std::move(group_by_exprs);
  aggregate_expressions_ = std::move(expressions);

  ranges::for_each(aggregate_expressions_, [this](Expression *expr) {
    auto *      aggregate_expr = static_cast<AggregateExpr *>(expr);
    Expression *child_expr     = aggregate_expr->child().get();
    ASSERT(child_expr != nullptr, "aggregation expression must have a child expression");
    value_expressions_.emplace_back(child_expr);
  });
}

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

  PhysicalOperator &child = *children_[0];
  RC                rc    = child.open(trx);
  if (OB_FAIL(rc)) {
    LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
    return rc;
  }

  aggregate_hash_table_ = make_unique<StandardAggregateHashTable>(aggregate_expressions_);
  while (OB_SUCC(rc = child.next(chunk_))) {
    // 分组列
    Chunk group_by_chunk;
    for (size_t group_by_idx = 0; group_by_idx < group_by_expressions_.size(); group_by_idx++) {
      Column column;
      group_by_expressions_[group_by_idx]->get_column(chunk_, column);
      std::unique_ptr<Column> c = make_unique<Column>();
      c->reference(column);
      group_by_chunk.add_column(std::move(c), group_by_idx);
    }
    // 聚合列
    Chunk aggregate_chunk;
    for (size_t aggr_idx = 0; aggr_idx < aggregate_expressions_.size(); aggr_idx++) {
      Column column;
      value_expressions_[aggr_idx]->get_column(chunk_, column);
      ASSERT(aggregate_expressions_[aggr_idx]->type() == ExprType::AGGREGATION, "expect aggregate expression");
      std::unique_ptr<Column> c = make_unique<Column>();
      c->reference(column);
      aggregate_chunk.add_column(std::move(c), aggr_idx);
    }
    aggregate_hash_table_->add_chunk(group_by_chunk, aggregate_chunk);
  }

  if (rc == RC::RECORD_EOF) {
    rc = RC::SUCCESS;
  }

  aggregate_hash_table_scanner_ = make_unique<StandardAggregateHashTable::Scanner>(aggregate_hash_table_.get());
  aggregate_hash_table_scanner_->open_scan();
  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  RC rc = aggregate_hash_table_scanner_->next(chunk);
  return rc;
}

RC GroupByVecPhysicalOperator::close()
{
  children_[0]->close();
  LOG_INFO("close group by operator");
  return RC::SUCCESS;
}
