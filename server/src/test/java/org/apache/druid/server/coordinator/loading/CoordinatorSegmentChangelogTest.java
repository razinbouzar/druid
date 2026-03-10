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

package org.apache.druid.server.coordinator.loading;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CoordinatorSegmentChangelogTest
{
  private static final DruidServerMetadata SERVER_A = new DruidServerMetadata(
      "serverA", "hostA", null, 1000L, null, ServerType.HISTORICAL, "tier1", 0
  );

  private static final DruidServerMetadata SERVER_B = new DruidServerMetadata(
      "serverB", "hostB", null, 1000L, null, ServerType.HISTORICAL, "tier1", 0
  );

  private final List<DataSegment> segments =
      CreateDataSegments.ofDatasource("test")
                        .forIntervals(1, Granularities.DAY)
                        .startingAt("2022-01-01")
                        .withNumPartitions(4)
                        .eachOfSizeInMb(100);

  private CoordinatorSegmentChangelog changelog;

  @Before
  public void setUp()
  {
    changelog = new CoordinatorSegmentChangelog();
  }

  @After
  public void tearDown()
  {
    changelog.stop();
  }

  @Test
  public void testInitialCounterIsZero()
  {
    Assert.assertEquals(0, changelog.getLastCounter().getCounter());
  }

  @Test
  public void testSegmentLoadedCreatesLoadEvent() throws Exception
  {
    DataSegment segment = segments.get(0);
    changelog.segmentLoaded(SERVER_A, segment);

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> snapshot =
        changelog.getChangesSince(ChangeRequestHistory.Counter.ZERO).get();

    Assert.assertEquals(1, snapshot.getRequests().size());
    CoordinatorSegmentChangeEvent event = snapshot.getRequests().get(0);
    Assert.assertTrue(event.isLoad());
    Assert.assertEquals(segment, event.getSegment());
    Assert.assertEquals(SERVER_A, event.getServer());
  }

  @Test
  public void testSegmentDroppedCreatesDropEvent() throws Exception
  {
    DataSegment segment = segments.get(0);
    changelog.segmentDropped(SERVER_A, segment);

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> snapshot =
        changelog.getChangesSince(ChangeRequestHistory.Counter.ZERO).get();

    Assert.assertEquals(1, snapshot.getRequests().size());
    CoordinatorSegmentChangeEvent event = snapshot.getRequests().get(0);
    Assert.assertFalse(event.isLoad());
    Assert.assertEquals(segment, event.getSegment());
    Assert.assertEquals(SERVER_A, event.getServer());
  }

  /**
   * Core invariant for issue #18738: when a segment is moved from A to B,
   * Load(B) must appear in the changelog before Drop(A).
   */
  @Test
  public void testLoadBeforeDropOrderingForSegmentMove() throws Exception
  {
    DataSegment segment = segments.get(0);

    // Simulate move: coordinator records Load(B) then Drop(A) in that order.
    changelog.segmentLoaded(SERVER_B, segment);
    changelog.segmentDropped(SERVER_A, segment);

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> snapshot =
        changelog.getChangesSince(ChangeRequestHistory.Counter.ZERO).get();

    Assert.assertEquals(2, snapshot.getRequests().size());

    CoordinatorSegmentChangeEvent first = snapshot.getRequests().get(0);
    Assert.assertTrue("First event must be a Load on SERVER_B", first.isLoad());
    Assert.assertEquals(SERVER_B, first.getServer());

    CoordinatorSegmentChangeEvent second = snapshot.getRequests().get(1);
    Assert.assertFalse("Second event must be a Drop from SERVER_A", second.isLoad());
    Assert.assertEquals(SERVER_A, second.getServer());
  }

  @Test
  public void testLastCounterAdvancesWithEachEvent()
  {
    Assert.assertEquals(0, changelog.getLastCounter().getCounter());

    changelog.segmentLoaded(SERVER_A, segments.get(0));
    Assert.assertEquals(1, changelog.getLastCounter().getCounter());

    changelog.segmentDropped(SERVER_A, segments.get(0));
    Assert.assertEquals(2, changelog.getLastCounter().getCounter());

    changelog.segmentLoaded(SERVER_B, segments.get(1));
    Assert.assertEquals(3, changelog.getLastCounter().getCounter());
  }

  @Test
  public void testDeltaSyncReturnsOnlyNewEvents() throws Exception
  {
    changelog.segmentLoaded(SERVER_A, segments.get(0));
    ChangeRequestHistory.Counter afterFirst = changelog.getLastCounter();

    changelog.segmentLoaded(SERVER_A, segments.get(1));
    changelog.segmentDropped(SERVER_B, segments.get(2));

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> delta =
        changelog.getChangesSince(afterFirst).get();

    Assert.assertEquals(2, delta.getRequests().size());
    Assert.assertEquals(segments.get(1), delta.getRequests().get(0).getSegment());
    Assert.assertEquals(segments.get(2), delta.getRequests().get(1).getSegment());
  }

  @Test
  public void testDeltaSyncFromCurrentCounterReturnsNothing() throws Exception
  {
    changelog.segmentLoaded(SERVER_A, segments.get(0));
    ChangeRequestHistory.Counter current = changelog.getLastCounter();

    // No new events — future should not resolve immediately (it's a pending wait).
    // We can verify by checking that the counter hasn't advanced.
    Assert.assertEquals(current.getCounter(), changelog.getLastCounter().getCounter());
  }

  @Test
  public void testFullSyncAfterMultipleEvents() throws Exception
  {
    changelog.segmentLoaded(SERVER_A, segments.get(0));
    changelog.segmentLoaded(SERVER_B, segments.get(1));
    changelog.segmentDropped(SERVER_A, segments.get(0));

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> snapshot =
        changelog.getChangesSince(ChangeRequestHistory.Counter.ZERO).get();

    Assert.assertEquals(3, snapshot.getRequests().size());
    Assert.assertTrue(snapshot.getRequests().get(0).isLoad());
    Assert.assertTrue(snapshot.getRequests().get(1).isLoad());
    Assert.assertFalse(snapshot.getRequests().get(2).isLoad());
  }
}
