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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.net.HostAndPort;
import com.google.inject.Inject;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordination.ChangeRequestHttpSyncer;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.loading.CoordinatorSegmentChangeEvent;
import org.apache.druid.server.http.CoordinatorSegmentChangelogResource;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * A {@link FilteredServerInventoryView} implementation that routes historical
 * segment placement notifications through the Coordinator's ordered changelog
 * instead of polling each historical node independently.
 *
 * <p>The Coordinator guarantees that for any segment move from server A to
 * server B, the {@code Load(B)} event is recorded in the changelog before the
 * {@code Drop(A)} event. Consuming the changelog in order therefore prevents
 * the broker from ever seeing a segment with zero replicas during a move, which
 * is the root cause of
 * <a href="https://github.com/apache/druid/issues/18738">issue #18738</a>.
 *
 * <h3>How it works</h3>
 * <ul>
 *   <li>Historical segment events come from
 *       {@link CoordinatorSegmentChangelogResource} via a single
 *       {@link ChangeRequestHttpSyncer} targeting the Coordinator leader.</li>
 *   <li>Realtime / non-historical segment events and all server lifecycle events
 *       are still forwarded from the underlying {@link HttpServerInventoryView}
 *       delegate, which handles discovery and realtime task announcement.</li>
 *   <li>The view is considered initialized only after <em>both</em> the delegate
 *       and the Coordinator syncer have completed their initial full sync.</li>
 * </ul>
 *
 * <h3>Activation</h3>
 * Set {@code druid.broker.segment.useCoordinatorChangelog=true} to enable.
 * When disabled (default), {@link HttpServerInventoryView} is used directly.
 */
public class CoordinatorInventoryView implements FilteredServerInventoryView
{
  public static final TypeReference<ChangeRequestsSnapshot<CoordinatorSegmentChangeEvent>> CHANGELOG_RESPONSE_TYPE =
      new TypeReference<>() {};

  private static final EmittingLogger log = new EmittingLogger(CoordinatorInventoryView.class);
  private static final String CHANGELOG_PATH = "druid-internal/v1/coordinator/segmentChangelog";
  private static final long SERVER_TIMEOUT_MS = 30_000L;

  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final DruidNodeDiscoveryProvider discoveryProvider;
  private final HttpServerInventoryView delegate;
  private final ScheduledExecutorService exec;

  private final ConcurrentHashMap<ServerView.SegmentCallback, Pair<Executor, Predicate<Pair<DruidServerMetadata, DataSegment>>>> segmentCallbacks =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<ServerView.ServerCallback, Executor> serverCallbacks =
      new ConcurrentHashMap<>();

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private volatile boolean delegateInitialized = false;
  private volatile boolean coordinatorInitialized = false;
  private volatile boolean viewInitializedFired = false;

  /**
   * All coordinator URLs discovered so far, in discovery order. Used to find
   * an alternative coordinator when the current one is removed.
   */
  private final CopyOnWriteArrayList<String> candidateCoordinatorUrls = new CopyOnWriteArrayList<>();

  @Nullable
  private volatile ChangeRequestHttpSyncer<CoordinatorSegmentChangeEvent> coordinatorSyncer;
  @Nullable
  private volatile String currentCoordinatorUrl;

  @Inject
  public CoordinatorInventoryView(
      @Smile ObjectMapper smileMapper,
      @EscalatedGlobal HttpClient httpClient,
      DruidNodeDiscoveryProvider discoveryProvider,
      HttpServerInventoryView delegate,
      ScheduledExecutorFactory executorFactory
  )
  {
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.discoveryProvider = discoveryProvider;
    this.delegate = delegate;
    this.exec = executorFactory.create(1, "CoordinatorInventoryView-%d");
  }

  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      throw new IllegalStateException("CoordinatorInventoryView already started.");
    }
    try {
      log.info("Starting CoordinatorInventoryView.");

      // Forward server lifecycle events from the delegate.
      delegate.registerServerCallback(
          exec,
          new ServerView.ServerCallback()
          {
            @Override
            public ServerView.CallbackAction serverAdded(DruidServer server)
            {
              runServerCallbacks(cb -> cb.serverAdded(server));
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public ServerView.CallbackAction serverRemoved(DruidServer server)
            {
              runServerCallbacks(cb -> cb.serverRemoved(server));
              return ServerView.CallbackAction.CONTINUE;
            }
          }
      );

      // Forward realtime (non-historical) segment events from the delegate.
      // Historical segment events come from the coordinator changelog instead.
      delegate.registerSegmentCallback(
          exec,
          new ServerView.BaseSegmentCallback()
          {
            @Override
            public ServerView.CallbackAction segmentAdded(DruidServerMetadata server, DataSegment segment)
            {
              if (server.getType() != ServerType.HISTORICAL) {
                runSegmentCallbacks(
                    cb -> cb.segmentAdded(server, segment),
                    new Pair<>(server, segment)
                );
              }
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public ServerView.CallbackAction segmentRemoved(DruidServerMetadata server, DataSegment segment)
            {
              if (server.getType() != ServerType.HISTORICAL) {
                runSegmentCallbacks(
                    cb -> cb.segmentRemoved(server, segment),
                    new Pair<>(server, segment)
                );
              }
              return ServerView.CallbackAction.CONTINUE;
            }

            @Override
            public ServerView.CallbackAction segmentViewInitialized()
            {
              delegateInitialized = true;
              maybeFireViewInitialized();
              return ServerView.CallbackAction.CONTINUE;
            }
          },
          pair -> true
      );

      // Watch for coordinator nodes; create a syncer for the leader.
      DruidNodeDiscovery coordinatorDiscovery = discoveryProvider.getForNodeRole(NodeRole.COORDINATOR);
      coordinatorDiscovery.registerListener(
          new DruidNodeDiscovery.Listener()
          {
            @Override
            public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
            {
              for (DiscoveryDruidNode node : nodes) {
                onCoordinatorNodeAdded(node);
              }
            }

            @Override
            public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
            {
              for (DiscoveryDruidNode node : nodes) {
                onCoordinatorNodeRemoved(node);
              }
            }

            @Override
            public void nodeViewInitialized()
            {
              // No coordinator discovered within the discovery window. This is
              // unusual but not fatal — the coordinator may come up later.
              if (coordinatorSyncer == null) {
                log.warn(
                    "No coordinator discovered during startup. "
                    + "CoordinatorInventoryView will wait for coordinator to become available."
                );
              }
            }
          }
      );

      delegate.start();
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      return;
    }
    try {
      log.info("Stopping CoordinatorInventoryView.");
      stopCurrentSyncer();
      delegate.stop();
    }
    finally {
      lifecycleLock.exitStop();
    }
  }

  @Override
  public void registerSegmentCallback(
      Executor exec,
      ServerView.SegmentCallback callback,
      Predicate<Pair<DruidServerMetadata, DataSegment>> filter
  )
  {
    segmentCallbacks.put(callback, new Pair<>(exec, filter));
  }

  @Override
  public void registerServerCallback(Executor exec, ServerView.ServerCallback callback)
  {
    serverCallbacks.put(callback, exec);
  }

  @Override
  @Nullable
  public DruidServer getInventoryValue(String serverKey)
  {
    return delegate.getInventoryValue(serverKey);
  }

  @Override
  public Collection<DruidServer> getInventory()
  {
    return delegate.getInventory();
  }

  @Override
  public boolean isStarted()
  {
    return lifecycleLock.isStarted();
  }

  @Override
  public boolean isSegmentLoadedByServer(String serverKey, DataSegment segment)
  {
    return delegate.isSegmentLoadedByServer(serverKey, segment);
  }

  private void onCoordinatorNodeAdded(DiscoveryDruidNode node)
  {
    if (!lifecycleLock.isStarted()) {
      return;
    }

    try {
      final String scheme = node.getDruidNode().getServiceScheme();
      final HostAndPort hostAndPort = HostAndPort.fromString(node.getDruidNode().getHostAndPortToUse());
      final URL baseUrl = new URL(scheme, hostAndPort.getHost(), hostAndPort.getPort(), "/");
      final String urlStr = baseUrl.toString();

      candidateCoordinatorUrls.addIfAbsent(urlStr);

      if (coordinatorSyncer != null) {
        // Already syncing with a coordinator; don't switch away unless it is removed
        // (see onCoordinatorNodeRemoved). In HA coordinator setups, if we happen to
        // connect to a follower coordinator, the syncer will fail with redirect errors
        // until that coordinator becomes the leader or is removed from discovery.
        return;
      }

      log.info("Discovered coordinator at [%s]. Starting changelog syncer.", urlStr);
      currentCoordinatorUrl = urlStr;
      startSyncer(baseUrl);
    }
    catch (Exception e) {
      log.error(e, "Failed to start syncer for coordinator node [%s].", node);
    }
  }

  private void onCoordinatorNodeRemoved(DiscoveryDruidNode node)
  {
    if (!lifecycleLock.isStarted()) {
      return;
    }

    try {
      final String scheme = node.getDruidNode().getServiceScheme();
      final HostAndPort hostAndPort = HostAndPort.fromString(node.getDruidNode().getHostAndPortToUse());
      final URL baseUrl = new URL(scheme, hostAndPort.getHost(), hostAndPort.getPort(), "/");
      final String urlStr = baseUrl.toString();

      candidateCoordinatorUrls.remove(urlStr);

      if (urlStr.equals(currentCoordinatorUrl)) {
        log.info("Coordinator [%s] removed from discovery. Stopping changelog syncer.", urlStr);
        stopCurrentSyncer();
        currentCoordinatorUrl = null;

        // Try another known coordinator if one is available.
        for (String candidateUrl : candidateCoordinatorUrls) {
          try {
            log.info("Switching changelog syncer to coordinator [%s].", candidateUrl);
            currentCoordinatorUrl = candidateUrl;
            startSyncer(new URL(candidateUrl));
            break;
          }
          catch (Exception e) {
            log.error(e, "Failed to start syncer for candidate coordinator [%s].", candidateUrl);
            currentCoordinatorUrl = null;
          }
        }
      }
    }
    catch (Exception e) {
      log.error(e, "Error handling coordinator node removal [%s].", node);
    }
  }

  private void startSyncer(URL baseUrl)
  {
    final ChangeRequestHttpSyncer<CoordinatorSegmentChangeEvent> syncer = new ChangeRequestHttpSyncer<>(
        smileMapper,
        httpClient,
        exec,
        baseUrl,
        CHANGELOG_PATH,
        CHANGELOG_RESPONSE_TYPE,
        SERVER_TIMEOUT_MS,
        SERVER_TIMEOUT_MS * 10,
        createSyncListener()
    );

    coordinatorSyncer = syncer;
    syncer.start();
  }

  private void stopCurrentSyncer()
  {
    final ChangeRequestHttpSyncer<CoordinatorSegmentChangeEvent> old = coordinatorSyncer;
    if (old != null) {
      coordinatorSyncer = null;
      try {
        old.stop();
      }
      catch (Exception e) {
        log.warn(e, "Error stopping coordinator changelog syncer.");
      }
    }
  }

  private ChangeRequestHttpSyncer.Listener<CoordinatorSegmentChangeEvent> createSyncListener()
  {
    return new ChangeRequestHttpSyncer.Listener<>()
    {
      @Override
      public void fullSync(List<CoordinatorSegmentChangeEvent> events)
      {
        log.info("Processing coordinator full sync with [%d] events.", events.size());
        for (CoordinatorSegmentChangeEvent event : events) {
          processEvent(event);
        }
        coordinatorInitialized = true;
        maybeFireViewInitialized();
      }

      @Override
      public void deltaSync(List<CoordinatorSegmentChangeEvent> events)
      {
        for (CoordinatorSegmentChangeEvent event : events) {
          processEvent(event);
        }
      }
    };
  }

  private void processEvent(CoordinatorSegmentChangeEvent event)
  {
    final DruidServerMetadata server = event.getServer();
    final DataSegment segment = event.getSegment();

    if (event.isLoad()) {
      runSegmentCallbacks(
          cb -> cb.segmentAdded(server, segment),
          new Pair<>(server, segment)
      );
    } else {
      runSegmentCallbacks(
          cb -> cb.segmentRemoved(server, segment),
          new Pair<>(server, segment)
      );
    }
  }

  private void maybeFireViewInitialized()
  {
    if (delegateInitialized && coordinatorInitialized && !viewInitializedFired) {
      synchronized (this) {
        if (!viewInitializedFired) {
          viewInitializedFired = true;
          runSegmentCallbacks(
              ServerView.SegmentCallback::segmentViewInitialized,
              null
          );
        }
      }
    }
  }

  private void runSegmentCallbacks(
      Consumer<ServerView.SegmentCallback> action,
      @Nullable Pair<DruidServerMetadata, DataSegment> filterPair
  )
  {
    for (ConcurrentHashMap.Entry<ServerView.SegmentCallback, Pair<Executor, Predicate<Pair<DruidServerMetadata, DataSegment>>>> entry
        : segmentCallbacks.entrySet()) {
      final ServerView.SegmentCallback callback = entry.getKey();
      final Executor cbExec = entry.getValue().lhs;
      final Predicate<Pair<DruidServerMetadata, DataSegment>> filter = entry.getValue().rhs;

      if (filterPair == null || filter.apply(filterPair)) {
        cbExec.execute(() -> action.accept(callback));
      }
    }
  }

  private void runServerCallbacks(Consumer<ServerView.ServerCallback> action)
  {
    for (ConcurrentHashMap.Entry<ServerView.ServerCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(() -> action.accept(entry.getKey()));
    }
  }
}
