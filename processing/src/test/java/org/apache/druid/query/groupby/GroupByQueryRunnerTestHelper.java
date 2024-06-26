/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.groupby;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.MetricsEmittingQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class GroupByQueryRunnerTestHelper
{
  public static <T> Iterable<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(populateResourceId(query)));
    return queryResult.toList();
  }

  public static <T> Iterable<T> runQueryWithEmitter(
      QueryRunnerFactory factory,
      QueryRunner runner,
      Query<T> query,
      ServiceEmitter serviceEmitter
  )
  {
    MetricsEmittingQueryRunner<ResultRow> metricsEmittingQueryRunner =
        new MetricsEmittingQueryRunner<ResultRow>(
            serviceEmitter,
            factory.getToolchest(),
            runner,
            (obj, lng) -> {},
            (metrics) -> {}
        ).withWaitMeasuredFromNow();
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(metricsEmittingQueryRunner)),
        toolChest
    );

    return theRunner.run(QueryPlus.wrap(populateResourceId(query))).toList();
  }

  public static ResultRow createExpectedRow(final GroupByQuery query, final String timestamp, Object... vals)
  {
    return createExpectedRow(query, DateTimes.of(timestamp), vals);
  }

  /**
   * Create a {@link ResultRow} for a given {@link GroupByQuery}. The size of the row will include space
   * for postaggregations.
   */
  public static ResultRow createExpectedRow(final GroupByQuery query, final DateTime timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    final ResultRow row = ResultRow.create(query.getResultRowSizeWithPostAggregators());

    if (query.getResultRowHasTimestamp()) {
      row.set(0, timestamp.getMillis());
    }

    for (int i = 0; i < vals.length; i += 2) {
      final int position = query.getResultRowSignature().indexOf(vals[i].toString());
      row.set(position, vals[i + 1]);
    }

    return row;
  }

  /**
   * Create a collection of {@link ResultRow} objects for a given {@link GroupByQuery}. The size of the rows will
   * include space for postaggregations.
   */
  public static List<ResultRow> createExpectedRows(
      final GroupByQuery query,
      final String[] columnNames,
      final Object[]... values
  )
  {
    final int timeIndex = Arrays.asList(columnNames).indexOf(ColumnHolder.TIME_COLUMN_NAME);
    Preconditions.checkArgument(timeIndex >= 0);

    List<ResultRow> expected = new ArrayList<>();
    for (Object[] value : values) {
      Preconditions.checkArgument(value.length == columnNames.length);
      ResultRow row = ResultRow.create(query.getResultRowSizeWithPostAggregators());
      for (int i = 0; i < columnNames.length; i++) {
        if (i != timeIndex) {
          final int position = query.getResultRowSignature().indexOf(columnNames[i]);
          row.set(position, value[i]);
        } else if (query.getResultRowHasTimestamp()) {
          row.set(0, new DateTime(value[i], ISOChronology.getInstanceUTC()).getMillis());
        }
      }
      expected.add(row);
    }
    return expected;
  }

  public static <T> QueryPlus<T> populateResourceId(QueryPlus<T> queryPlus)
  {
    return queryPlus.withQuery(populateResourceId(queryPlus.getQuery()));
  }

  public static <T> Query<T> populateResourceId(Query<T> query)
  {
    return query.withOverriddenContext(defaultResourceIdMap());
  }

  public static Map<String, Object> defaultResourceIdMap()
  {
    return Collections.singletonMap(QueryContexts.QUERY_RESOURCE_ID, UUID.randomUUID().toString());
  }
}
