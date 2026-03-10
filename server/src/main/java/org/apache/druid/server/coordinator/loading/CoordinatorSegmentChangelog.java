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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

/**
 * Maintains an ordered in-memory log of segment placement events confirmed by
 * the Coordinator. Events are appended atomically (inside the
 * {@link HttpLoadQueuePeon} response lock) so they always reflect the order in
 * which the Coordinator applied them: for a segment move from server A to
 * server B, {@code Load(B, segment)} is recorded before {@code Drop(A, segment)}.
 *
 * <p>Brokers poll this log via
 * {@link org.apache.druid.server.http.CoordinatorSegmentChangelogResource} and
 * replay events to keep their query timelines consistent, eliminating the race
 * condition caused by independent HTTP polling of each historical node.</p>
 *
 * <p>This class is a thin wrapper around {@link ChangeRequestHistory} and is
 * bound as a singleton in the Coordinator's Guice module.</p>
 */
public class CoordinatorSegmentChangelog
{
  private final ChangeRequestHistory<CoordinatorSegmentChangeEvent> history = new ChangeRequestHistory<>();

  /**
   * Records a confirmed segment load on {@code server}. Must be called while
   * holding the {@link HttpLoadQueuePeon} response lock so that the ordering
   * guarantee relative to subsequent drop events is maintained.
   */
  public void segmentLoaded(DruidServerMetadata server, DataSegment segment)
  {
    history.addChangeRequest(new CoordinatorSegmentChangeEvent.Load(server, segment));
  }

  /**
   * Records a confirmed segment drop from {@code server}. Must be called while
   * holding the {@link HttpLoadQueuePeon} response lock.
   */
  public void segmentDropped(DruidServerMetadata server, DataSegment segment)
  {
    history.addChangeRequest(new CoordinatorSegmentChangeEvent.Drop(server, segment));
  }

  /**
   * Returns a future that resolves with all events since {@code counter}.
   * Pass {@code counter} with {@link ChangeRequestHistory.Counter#getCounter()} &lt; 0
   * to indicate an initial request — callers should handle that case before
   * invoking this method (see
   * {@link org.apache.druid.server.http.CoordinatorSegmentChangelogResource}).
   */
  public ListenableFuture<ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent>> getChangesSince(
      ChangeRequestHistory.Counter counter
  )
  {
    return history.getRequestsSince(counter);
  }

  public ChangeRequestHistory.Counter getLastCounter()
  {
    return history.getLastCounter();
  }

  @LifecycleStop
  public void stop()
  {
    history.stop();
  }
}
