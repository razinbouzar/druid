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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Predicates;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;

import javax.validation.constraints.NotNull;

/**
 * Provider for {@link CoordinatorInventoryView}, selected when
 * {@code druid.serverview.type=coordinator}.
 *
 * <p>This routes historical segment placement notifications through the
 * Coordinator's ordered changelog, eliminating the race condition described
 * in <a href="https://github.com/apache/druid/issues/18738">issue #18738</a>.</p>
 *
 * <p>The underlying {@link HttpServerInventoryView} is still created and used
 * as a delegate for server lifecycle events and realtime segment tracking.</p>
 *
 * @see CoordinatorInventoryView
 */
public class FilteredCoordinatorInventoryViewProvider implements FilteredServerInventoryViewProvider
{
  @JacksonInject
  @NotNull
  @EscalatedClient
  HttpClient historicalHttpClient;

  @JacksonInject
  @NotNull
  @EscalatedGlobal
  HttpClient coordinatorHttpClient;

  @JacksonInject
  @NotNull
  @Smile
  ObjectMapper smileMapper;

  @JacksonInject
  @NotNull
  HttpServerInventoryViewConfig config;

  @JacksonInject
  @NotNull
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  @JacksonInject
  @NotNull
  private ScheduledExecutorFactory executorFactory;

  @JacksonInject
  @NotNull
  private ServiceEmitter serviceEmitter;

  @Override
  public CoordinatorInventoryView get()
  {
    final HttpServerInventoryView delegate = new HttpServerInventoryView(
        smileMapper,
        historicalHttpClient,
        druidNodeDiscoveryProvider,
        Predicates.alwaysFalse(),
        config,
        serviceEmitter,
        executorFactory,
        "CoordinatorInventoryViewDelegate"
    );

    return new CoordinatorInventoryView(
        smileMapper,
        coordinatorHttpClient,
        druidNodeDiscoveryProvider,
        delegate,
        executorFactory
    );
  }
}
