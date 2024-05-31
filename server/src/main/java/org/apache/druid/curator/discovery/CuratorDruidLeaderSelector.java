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

package org.apache.druid.curator.discovery;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

public class CuratorDruidLeaderSelector implements DruidLeaderSelector
{
  private static final EmittingLogger log = new EmittingLogger(CuratorDruidLeaderSelector.class);

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final DruidNode self;
  private final CuratorFramework curator;
  private final String latchPath;

  private ExecutorService listenerExecutor;

  private DruidLeaderSelector.Listener listener = null;
  private final AtomicReference<LeaderLatch> leaderLatch = new AtomicReference<>();

  private volatile boolean leader = false;
  private volatile int term = 0;

  public CuratorDruidLeaderSelector(CuratorFramework curator, @Self DruidNode self, String latchPath)
  {
    this.curator = curator;
    this.self = self;
    this.latchPath = latchPath;
    this.leaderLatch.set(createNewLeaderLatch());

    // Adding ConnectionStateListener to handle session changes using a method reference
    curator.getConnectionStateListenable().addListener(this::handleConnectionStateChanged);
  }

  private LeaderLatch createNewLeaderLatch()
  {
    return new LeaderLatch(curator, latchPath, self.getServiceScheme() + "://" + self.getHostAndPortToUse());
  }

  private LeaderLatch createNewLeaderLatchWithListener()
  {
    final LeaderLatch newLeaderLatch = createNewLeaderLatch();
    newLeaderLatch.addListener(new LeaderLatchListener()
    {
      @Override
      public void isLeader()
      {
        try {
          if (leader) {
            log.warn("I'm being asked to become leader. But I am already the leader. Ignored event.");
            return;
          }

          leader = true;
          term++;
          listener.becomeLeader();
        }
        catch (Exception ex) {
          log.makeAlert(ex, "listener becomeLeader() failed. Unable to become leader").emit();

          // give others a chance to become leader.
          CloseableUtils.closeAndSuppressExceptions(
              createNewLeaderLatchWithListener(),
              e -> log.warn("Could not close old leader latch; continuing with new one anyway.")
          );

          leader = false;
          try {
            // Small delay before starting the latch so that others waiting are chosen to become leader.
            Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 5000));
            leaderLatch.get().start();
          }
          catch (Exception e) {
            // If an exception gets thrown out here, then the node will zombie out 'cause it won't be looking for
            // the latch anymore.  I don't believe it's actually possible for an Exception to throw out here, but
            // Curator likes to have "throws Exception" on methods so it might happen...
            log.makeAlert(e, "I am a zombie").emit();
          }
        }
      }

      @Override
      public void notLeader()
      {
        try {
          if (!leader) {
            log.warn("I'm being asked to stop being leader. But I am not the leader. Ignored event.");
            return;
          }

          leader = false;
          listener.stopBeingLeader();
        }
        catch (Exception ex) {
          log.makeAlert(ex, "listener.stopBeingLeader() failed. Unable to stopBeingLeader").emit();
        }
      }
    }, listenerExecutor);

    return leaderLatch.getAndSet(newLeaderLatch);
  }

  @Nullable
  @Override
  public String getCurrentLeader()
  {
    try {
      final LeaderLatch latch = leaderLatch.get();

      Participant participant = latch.getLeader();
      if (participant.isLeader()) {
        return participant.getId();
      }

      return null;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isLeader()
  {
    return leader;
  }

  @Override
  public int localTerm()
  {
    return term;
  }

  @Override
  public void registerListener(DruidLeaderSelector.Listener listener)
  {
    Preconditions.checkArgument(listener != null, "listener is null.");

    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      this.listener = listener;
      this.listenerExecutor = Execs.singleThreaded(
          StringUtils.format(
              "LeaderSelector[%s]",
              StringUtils.encodeForFormat(latchPath)
          )
      );

      createNewLeaderLatchWithListener();
      leaderLatch.get().start();

      lifecycleLock.started();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void unregisterListener()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    CloseableUtils.closeAndSuppressExceptions(leaderLatch.get(), e -> log.warn(e, "Failed to close LeaderLatch."));
    listenerExecutor.shutdownNow();
  }

  // Method to handle connection state changes
  private void handleConnectionStateChanged(CuratorFramework client, ConnectionState newState)
  {
    switch (newState) {
      case SUSPENDED:
      case LOST:
        recreateLeaderLatch();
        break;
      case RECONNECTED:
        // Connection reestablished, no action needed here
        break;
      default:
        // Do nothing for other states
        break;
    }
  }

  private void recreateLeaderLatch()
  {
    // Close existing leader latch
    CloseableUtils.closeAndSuppressExceptions(leaderLatch.get(), e -> log.warn(e, "Failed to close LeaderLatch."));

    // Create and start a new leader latch
    LeaderLatch newLeaderLatch = createNewLeaderLatchWithListener();
    try {
      newLeaderLatch.start();
    }
    catch (Exception ex) {
      throw new RuntimeException("Failed to start new LeaderLatch after session change", ex);
    }
    leaderLatch.set(newLeaderLatch);
  }
}
