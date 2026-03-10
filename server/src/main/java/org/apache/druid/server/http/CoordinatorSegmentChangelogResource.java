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
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.CoordinatorInventoryView;
import org.apache.druid.client.CoordinatorServerView;
import org.apache.druid.client.DruidServer;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordinator.loading.CoordinatorSegmentChangeEvent;
import org.apache.druid.server.coordinator.loading.CoordinatorSegmentChangelog;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.timeline.DataSegment;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Exposes an ordered stream of confirmed segment load/drop events from the
 * Coordinator for brokers to poll. Unlike the per-node
 * {@link SegmentListerResource}, events here are emitted in the order the
 * Coordinator applied them, so a segment move from A to B always produces
 * {@code Load(B, segment)} before {@code Drop(A, segment)}, eliminating the
 * race condition described in
 * <a href="https://github.com/apache/druid/issues/18738">issue #18738</a>.
 *
 * <p>Brokers use {@link org.apache.druid.client.CoordinatorInventoryView} to
 * consume this endpoint instead of (or in addition to) polling individual
 * historical nodes.</p>
 *
 * <h3>Protocol</h3>
 * <ol>
 *   <li>Client sends {@code GET .../segmentChangelog?counter=-1&timeout=...} for the initial sync.
 *       The response contains all currently placed segments as Load events, plus
 *       a {@code (counter, hash)} pair representing the current head of the log.</li>
 *   <li>Client sends subsequent requests with the {@code counter} and {@code hash}
 *       received in the previous response. The server returns only new events since
 *       that position, long-polling until new events arrive or {@code timeout} elapses.</li>
 * </ol>
 */
@Path("/druid-internal/v1/coordinator/segmentChangelog")
@ResourceFilters(StateResourceFilter.class)
public class CoordinatorSegmentChangelogResource
{
  private static final EmittingLogger log = new EmittingLogger(CoordinatorSegmentChangelogResource.class);

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final CoordinatorSegmentChangelog changelog;
  private final CoordinatorServerView serverView;

  @Inject
  public CoordinatorSegmentChangelogResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      CoordinatorSegmentChangelog changelog,
      CoordinatorServerView serverView
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.changelog = changelog;
    this.serverView = serverView;
  }

  /**
   * Long-polling endpoint that returns ordered segment placement events from
   * the Coordinator's changelog.
   *
   * @param counter position in the changelog from the last response (pass -1 for initial sync)
   * @param hash    hash from the last response (ignored when counter &lt; 0)
   * @param timeout milliseconds to wait for new events before returning an empty response
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Void getSegmentChangelog(
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash,
      @QueryParam("timeout") long timeout,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (timeout <= 0) {
      sendErrorResponse(req, HttpServletResponse.SC_BAD_REQUEST, "timeout must be positive.");
      return null;
    }

    final ResponseContext context = createContext(req.getHeader("Accept"));
    final ListenableFuture<ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent>> future;

    if (counter < 0) {
      // Initial full-sync: build a snapshot of all currently placed segments.
      future = buildFullSyncFuture();
    } else {
      future = changelog.getChangesSince(new ChangeRequestHistory.Counter(counter, hash));
    }

    final AsyncContext asyncContext = req.startAsync();

    asyncContext.addListener(
        new AsyncListener()
        {
          @Override
          public void onComplete(AsyncEvent event)
          {
          }

          @Override
          public void onTimeout(AsyncEvent event)
          {
            future.cancel(true);
            event.getAsyncContext().complete();
          }

          @Override
          public void onError(AsyncEvent event)
          {
          }

          @Override
          public void onStartAsync(AsyncEvent event)
          {
          }
        }
    );

    Futures.addCallback(
        future,
        new FutureCallback<>()
        {
          @Override
          public void onSuccess(ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent> result)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              context.mapper.writerFor(CoordinatorInventoryView.CHANGELOG_RESPONSE_TYPE)
                            .writeValue(asyncContext.getResponse().getOutputStream(), result);
              asyncContext.complete();
            }
            catch (Exception ex) {
              log.debug(ex, "Request timed out or closed already.");
            }
          }

          @Override
          public void onFailure(Throwable th)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              if (th instanceof IllegalArgumentException) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, th.getMessage());
              } else {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, th.getMessage());
              }
              asyncContext.complete();
            }
            catch (Exception ex) {
              log.debug(ex, "Request timed out or closed already.");
            }
          }
        },
        MoreExecutors.directExecutor()
    );

    asyncContext.setTimeout(timeout);
    return null;
  }

  /**
   * Builds a full-state snapshot from the coordinator's current server view.
   * The snapshot counter is set to the changelog's current head so that
   * subsequent delta requests pick up exactly where this snapshot ends.
   */
  private ListenableFuture<ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent>> buildFullSyncFuture()
  {
    final List<CoordinatorSegmentChangeEvent> allEvents = new ArrayList<>();
    for (DruidServer server : serverView.getInventory()) {
      for (DataSegment segment : server.iterateAllSegments()) {
        allEvents.add(new CoordinatorSegmentChangeEvent.Load(server.getMetadata(), segment));
      }
    }

    final ChangeRequestHistory.Counter currentCounter = changelog.getLastCounter();
    final SettableFuture<ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent>> future = SettableFuture.create();
    future.set(ChangeRequestsSnapshot.success(currentCounter, allEvents));
    return future;
  }

  private void sendErrorResponse(HttpServletRequest req, int code, String error) throws IOException
  {
    AsyncContext asyncContext = req.startAsync();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
    response.sendError(code, error);
    asyncContext.complete();
  }

  private ResponseContext createContext(String requestType)
  {
    boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType);
    return new ResponseContext(isSmile ? smileMapper : jsonMapper);
  }

  private static class ResponseContext
  {
    private final ObjectMapper mapper;

    ResponseContext(ObjectMapper mapper)
    {
      this.mapper = mapper;
    }
  }
}
