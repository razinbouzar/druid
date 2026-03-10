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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class CoordinatorSegmentChangeEventTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  private static final DruidServerMetadata SERVER = new DruidServerMetadata(
      "server1", "host1", null, 1000L, null, ServerType.HISTORICAL, "tier1", 0
  );

  private static final List<DataSegment> SEGMENTS =
      CreateDataSegments.ofDatasource("test")
                        .forIntervals(1, Granularities.DAY)
                        .startingAt("2022-01-01")
                        .withNumPartitions(2)
                        .eachOfSizeInMb(100);

  @Test
  public void testLoadEventIsLoad()
  {
    CoordinatorSegmentChangeEvent event = new CoordinatorSegmentChangeEvent.Load(SERVER, SEGMENTS.get(0));
    Assert.assertTrue(event.isLoad());
    Assert.assertEquals(SERVER, event.getServer());
    Assert.assertEquals(SEGMENTS.get(0), event.getSegment());
  }

  @Test
  public void testDropEventIsNotLoad()
  {
    CoordinatorSegmentChangeEvent event = new CoordinatorSegmentChangeEvent.Drop(SERVER, SEGMENTS.get(0));
    Assert.assertFalse(event.isLoad());
    Assert.assertEquals(SERVER, event.getServer());
    Assert.assertEquals(SEGMENTS.get(0), event.getSegment());
  }

  @Test
  public void testLoadEventSerdeRoundtrip() throws Exception
  {
    CoordinatorSegmentChangeEvent original = new CoordinatorSegmentChangeEvent.Load(SERVER, SEGMENTS.get(0));
    String json = MAPPER.writeValueAsString(original);
    CoordinatorSegmentChangeEvent deserialized = MAPPER.readValue(json, CoordinatorSegmentChangeEvent.class);

    Assert.assertTrue(deserialized.isLoad());
    Assert.assertEquals(original.getServer(), deserialized.getServer());
    Assert.assertEquals(original.getSegment(), deserialized.getSegment());
  }

  @Test
  public void testDropEventSerdeRoundtrip() throws Exception
  {
    CoordinatorSegmentChangeEvent original = new CoordinatorSegmentChangeEvent.Drop(SERVER, SEGMENTS.get(0));
    String json = MAPPER.writeValueAsString(original);
    CoordinatorSegmentChangeEvent deserialized = MAPPER.readValue(json, CoordinatorSegmentChangeEvent.class);

    Assert.assertFalse(deserialized.isLoad());
    Assert.assertEquals(original.getServer(), deserialized.getServer());
    Assert.assertEquals(original.getSegment(), deserialized.getSegment());
  }

  @Test
  public void testLoadTypePropertyInJson() throws Exception
  {
    String json = MAPPER.writeValueAsString(new CoordinatorSegmentChangeEvent.Load(SERVER, SEGMENTS.get(0)));
    Assert.assertTrue("JSON must contain type:load", json.contains("\"type\":\"load\""));
  }

  @Test
  public void testDropTypePropertyInJson() throws Exception
  {
    String json = MAPPER.writeValueAsString(new CoordinatorSegmentChangeEvent.Drop(SERVER, SEGMENTS.get(0)));
    Assert.assertTrue("JSON must contain type:drop", json.contains("\"type\":\"drop\""));
  }

  @Test
  public void testLoadToStringContainsKeyFields()
  {
    String s = new CoordinatorSegmentChangeEvent.Load(SERVER, SEGMENTS.get(0)).toString();
    Assert.assertTrue(s.contains("Load"));
    Assert.assertTrue(s.contains(SERVER.getName()));
  }

  @Test
  public void testDropToStringContainsKeyFields()
  {
    String s = new CoordinatorSegmentChangeEvent.Drop(SERVER, SEGMENTS.get(0)).toString();
    Assert.assertTrue(s.contains("Drop"));
    Assert.assertTrue(s.contains(SERVER.getName()));
  }
}
