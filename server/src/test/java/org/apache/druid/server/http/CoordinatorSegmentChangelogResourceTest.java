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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.loading.CoordinatorSegmentChangeEvent;
import org.apache.druid.server.coordinator.loading.CoordinatorSegmentChangelog;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncListener;
import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class CoordinatorSegmentChangelogResourceTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();

  private static final DruidServerMetadata SERVER_META = new DruidServerMetadata(
      "server1", "host1", null, 1000L, null, ServerType.HISTORICAL, "tier1", 0
  );

  private final List<DataSegment> segments =
      CreateDataSegments.ofDatasource("test")
                        .forIntervals(1, Granularities.DAY)
                        .startingAt("2022-01-01")
                        .withNumPartitions(2)
                        .eachOfSizeInMb(100);

  private CoordinatorSegmentChangelog changelog;
  private CoordinatorServerView serverView;
  private CoordinatorSegmentChangelogResource resource;

  private HttpServletRequest mockRequest;
  private AsyncContext mockAsyncContext;
  private HttpServletResponse mockResponse;
  private TestServletOutputStream responseStream;

  @Before
  public void setUp() throws IOException
  {
    changelog = new CoordinatorSegmentChangelog();
    serverView = EasyMock.createMock(CoordinatorServerView.class);
    resource = new CoordinatorSegmentChangelogResource(JSON_MAPPER, JSON_MAPPER, changelog, serverView);

    responseStream = new TestServletOutputStream();
    mockRequest = EasyMock.createMock(HttpServletRequest.class);
    mockAsyncContext = EasyMock.createMock(AsyncContext.class);
    // Use a nice mock so that tearDown's changelog.stop() can resolve pending futures
    // (triggering sendError(500, ...)) without causing unexpected-call failures.
    mockResponse = EasyMock.createNiceMock(HttpServletResponse.class);

    EasyMock.expect(mockRequest.startAsync()).andReturn(mockAsyncContext).anyTimes();
    EasyMock.expect(mockRequest.getHeader("Accept")).andReturn("application/json").anyTimes();
    EasyMock.expect(mockAsyncContext.getResponse()).andReturn(mockResponse).anyTimes();
    mockAsyncContext.addListener(EasyMock.anyObject(AsyncListener.class));
    EasyMock.expectLastCall().anyTimes();
    mockAsyncContext.setTimeout(EasyMock.anyLong());
    EasyMock.expectLastCall().anyTimes();
    mockAsyncContext.complete();
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(mockResponse.getOutputStream()).andReturn(responseStream).anyTimes();
    mockResponse.setStatus(HttpServletResponse.SC_OK);
    EasyMock.expectLastCall().anyTimes();
  }

  @After
  public void tearDown()
  {
    changelog.stop();
  }

  @Test
  public void testZeroTimeoutReturnsBadRequest() throws IOException
  {
    mockResponse.sendError(EasyMock.eq(HttpServletResponse.SC_BAD_REQUEST), EasyMock.anyString());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockRequest, mockAsyncContext, mockResponse, serverView);

    resource.getSegmentChangelog(0, 0, 0, mockRequest);

    EasyMock.verify(mockRequest, mockAsyncContext, mockResponse, serverView);
  }

  @Test
  public void testNegativeTimeoutReturnsBadRequest() throws IOException
  {
    mockResponse.sendError(EasyMock.eq(HttpServletResponse.SC_BAD_REQUEST), EasyMock.anyString());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockRequest, mockAsyncContext, mockResponse, serverView);

    resource.getSegmentChangelog(0, 0, -1, mockRequest);

    EasyMock.verify(mockRequest, mockAsyncContext, mockResponse, serverView);
  }

  @Test
  public void testInitialFullSyncWithNoSegments() throws Exception
  {
    EasyMock.expect(serverView.getInventory()).andReturn(Collections.emptyList()).once();
    EasyMock.replay(mockRequest, mockAsyncContext, mockResponse, serverView);

    resource.getSegmentChangelog(-1, 0, 30_000, mockRequest);

    EasyMock.verify(mockRequest, mockAsyncContext, mockResponse, serverView);

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> result = JSON_MAPPER.readValue(
        responseStream.baos.toByteArray(),
        org.apache.druid.client.CoordinatorInventoryView.CHANGELOG_RESPONSE_TYPE
    );
    Assert.assertFalse(result.isResetCounter());
    Assert.assertTrue(result.getRequests().isEmpty());
  }

  @Test
  public void testInitialFullSyncIncludesAllCurrentSegments() throws Exception
  {
    DruidServer server = new DruidServer(SERVER_META);
    server.addDataSegment(segments.get(0));
    server.addDataSegment(segments.get(1));

    EasyMock.expect(serverView.getInventory()).andReturn(Collections.singletonList(server)).once();
    EasyMock.replay(mockRequest, mockAsyncContext, mockResponse, serverView);

    resource.getSegmentChangelog(-1, 0, 30_000, mockRequest);

    EasyMock.verify(mockRequest, mockAsyncContext, mockResponse, serverView);

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> result = JSON_MAPPER.readValue(
        responseStream.baos.toByteArray(),
        org.apache.druid.client.CoordinatorInventoryView.CHANGELOG_RESPONSE_TYPE
    );
    Assert.assertEquals(2, result.getRequests().size());
    Assert.assertTrue(result.getRequests().stream().allMatch(CoordinatorSegmentChangeEvent::isLoad));
  }

  @Test
  public void testFullSyncCounterAnchoredToChangelogHead() throws Exception
  {
    // Pre-populate changelog with an event so lastCounter > 0.
    changelog.segmentLoaded(SERVER_META, segments.get(0));
    long expectedCounter = changelog.getLastCounter().getCounter();

    EasyMock.expect(serverView.getInventory()).andReturn(Collections.emptyList()).once();
    EasyMock.replay(mockRequest, mockAsyncContext, mockResponse, serverView);

    resource.getSegmentChangelog(-1, 0, 30_000, mockRequest);

    ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> result = JSON_MAPPER.readValue(
        responseStream.baos.toByteArray(),
        org.apache.druid.client.CoordinatorInventoryView.CHANGELOG_RESPONSE_TYPE
    );
    Assert.assertEquals(expectedCounter, result.getCounter().getCounter());
  }

  @Test
  public void testDeltaSyncReturnsPendingFutureForNoNewEvents() throws IOException
  {
    // Record one event and anchor at that counter, then request delta — nothing new.
    changelog.segmentLoaded(SERVER_META, segments.get(0));
    long counter = changelog.getLastCounter().getCounter();
    long hash = changelog.getLastCounter().getHash();

    EasyMock.replay(mockRequest, mockAsyncContext, mockResponse, serverView);

    // The future won't resolve synchronously when there are no new events.
    // The async context timeout mechanism handles that case. We just verify no exception.
    resource.getSegmentChangelog(counter, hash, 30_000, mockRequest);

    EasyMock.verify(mockRequest, mockAsyncContext, mockResponse, serverView);
  }

  /**
   * Thin wrapper around ByteArrayOutputStream that satisfies the abstract ServletOutputStream contract.
   */
  private static class TestServletOutputStream extends ServletOutputStream
  {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();

    @Override
    public void write(int b)
    {
      baos.write(b);
    }

    @Override
    public boolean isReady()
    {
      return true;
    }

    @Override
    public void setWriteListener(WriteListener writeListener)
    {
    }
  }
}
