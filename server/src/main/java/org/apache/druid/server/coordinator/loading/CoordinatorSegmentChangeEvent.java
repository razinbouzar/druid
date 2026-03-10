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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.timeline.DataSegment;

/**
 * Represents a single segment placement event confirmed by the Coordinator.
 * These events are appended to the {@link CoordinatorSegmentChangelog} in the
 * order they are confirmed, guaranteeing that for a segment move from server A
 * to server B, the Load(B) event always precedes the Drop(A) event.
 *
 * @see CoordinatorSegmentChangelog
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = CoordinatorSegmentChangeEvent.Load.class, name = "load"),
    @JsonSubTypes.Type(value = CoordinatorSegmentChangeEvent.Drop.class, name = "drop")
})
public abstract class CoordinatorSegmentChangeEvent
{
  public abstract DataSegment getSegment();

  public abstract DruidServerMetadata getServer();

  public abstract boolean isLoad();

  public static final class Load extends CoordinatorSegmentChangeEvent
  {
    private final DruidServerMetadata server;
    private final DataSegment segment;

    @JsonCreator
    public Load(
        @JsonProperty("server") DruidServerMetadata server,
        @JsonProperty("segment") DataSegment segment
    )
    {
      this.server = server;
      this.segment = segment;
    }

    @Override
    @JsonProperty
    public DruidServerMetadata getServer()
    {
      return server;
    }

    @Override
    @JsonProperty
    public DataSegment getSegment()
    {
      return segment;
    }

    @Override
    public boolean isLoad()
    {
      return true;
    }

    @Override
    public String toString()
    {
      return "Load{server=" + server.getName() + ", segment=" + segment.getId() + "}";
    }
  }

  public static final class Drop extends CoordinatorSegmentChangeEvent
  {
    private final DruidServerMetadata server;
    private final DataSegment segment;

    @JsonCreator
    public Drop(
        @JsonProperty("server") DruidServerMetadata server,
        @JsonProperty("segment") DataSegment segment
    )
    {
      this.server = server;
      this.segment = segment;
    }

    @Override
    @JsonProperty
    public DruidServerMetadata getServer()
    {
      return server;
    }

    @Override
    @JsonProperty
    public DataSegment getSegment()
    {
      return segment;
    }

    @Override
    public boolean isLoad()
    {
      return false;
    }

    @Override
    public String toString()
    {
      return "Drop{server=" + server.getName() + ", segment=" + segment.getId() + "}";
    }
  }
}
