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

suite("push_down_min_through_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set be_number_for_test=1"
    sql "set DISABLE_NEREIDS_RULES='ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT, ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI'"

    sql """
        DROP TABLE IF EXISTS min_t;
    """

    sql """
    CREATE TABLE IF NOT EXISTS min_t(
      `id` int(32),
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "insert into min_t values (1, 1, 'a')"
    sql "insert into min_t values (2, null, 'a')"
    sql "insert into min_t values (3, 1, null)"
    sql "insert into min_t values (4, 2, 'b')"
    sql "insert into min_t values (5, null, 'b')"
    sql "insert into min_t values (6, 2, null)"
    sql "insert into min_t values (7, 3, 'c')"
    sql "insert into min_t values (8, null, 'c')"
    sql "insert into min_t values (9, 3, null)"
    sql "insert into min_t values (10, null, null)"
    sql "analyze table min_t with sync;"
    qt_groupby_pushdown_basic """
        explain shape plan select min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_join """
        explain shape plan select min(t1.score) from min_t t1 left join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_right_join """
        explain shape plan select min(t1.score) from min_t t1 right join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_full_join """
        explain shape plan select min(t1.score) from min_t t1 full join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_semi_join """
        explain shape plan select min(t1.score) from min_t t1 inner join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_anti_join """
        explain shape plan select min(t1.score) from min_t t1 left anti join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_complex_conditions """
        explain shape plan select min(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_groupby_pushdown_with_aggregate """
        explain shape plan select min(t1.score), avg(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_subquery """
        explain shape plan select min(t1.score) from (select * from min_t where score > 10) t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_outer_join """
        explain shape plan select min(t1.score) from min_t t1 left join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_deep_subquery """
        explain shape plan select min(t1.score) from (select * from (select * from min_t) min_t where score > 10) t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_having """
        explain shape plan select min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name having min(t1.score) > 100;
    """

    qt_groupby_pushdown_mixed_aggregates """
        explain shape plan select min(t1.score), count(*), sum(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_multi_table_join """
        explain shape plan select min(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id join min_t t3 on t1.name = t3.name group by t1.name;
    """

    qt_groupby_pushdown_with_order_by """
        explain shape plan select min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_with_aggregate """
        explain shape plan select sum(t1.score), min(t2.score) from min_t t1 join min_t t2 on t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select t1.name, min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection_with_aggregate """
        explain shape plan select t1.name, min(t1.score), min(t2.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

     qt_groupby_pushdown_with_where_clause """
        explain shape plan select min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_groupby_pushdown_varied_aggregates """
        explain shape plan select min(t1.score), avg(t1.id), count(t2.name) from min_t t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_with_order_by_limit """
        explain shape plan select min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name order by min(t1.score) limit 10;
    """

    qt_groupby_pushdown_alias_multiple_equal_conditions """
        explain shape plan select min(t1_alias.score) from min_t t1_alias join min_t t2_alias on t1_alias.id = t2_alias.id and t1_alias.name = t2_alias.name group by t1_alias.name;
    """

    qt_groupby_pushdown_complex_join_condition """
        explain shape plan select min(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_groupby_pushdown_function_processed_columns """
        explain shape plan select min(LENGTH(t1.name)) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_nested_queries """
        explain shape plan select min(t1.score) from (select * from min_t where score > 20) t1 join (select * from min_t where id < 100) t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_basic """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 left join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_right_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 right join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_full_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 full join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_semi_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 inner join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_anti_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 left anti join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_complex_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score), avg(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_subquery """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from (select * from min_t where score > 10) t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_outer_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 left join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_deep_subquery """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from (select * from (select * from min_t) min_t where score > 10) t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_having """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name having min(t1.score) > 100;
    """

    qt_with_hint_groupby_pushdown_mixed_aggregates """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score), count(*), sum(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_multi_table_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id join min_t t3 on t1.name = t3.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_order_by """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_with_hint_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  sum(t1.score), min(t2.score) from min_t t1 join min_t t2 on t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  t1.name, min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_non_aggregate_selection_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  t1.name, min(t1.score), min(t2.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_where_clause """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_with_hint_groupby_pushdown_varied_aggregates """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score), avg(t1.id), count(t2.name) from min_t t1 join min_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_order_by_limit """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name order by min(t1.score) limit 10;
    """

    qt_with_hint_groupby_pushdown_alias_multiple_equal_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1_alias.score) from min_t t1_alias join min_t t2_alias on t1_alias.id = t2_alias.id and t1_alias.name = t2_alias.name group by t1_alias.name;
    """

    qt_with_hint_groupby_pushdown_complex_join_condition """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from min_t t1 join min_t t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_function_processed_columns """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(LENGTH(t1.name)) from min_t t1, min_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_nested_queries """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join_one_side) */  min(t1.score) from (select * from min_t where score > 20) t1 join (select * from min_t where id < 100) t2 on t1.id = t2.id group by t1.name;
    """
}
