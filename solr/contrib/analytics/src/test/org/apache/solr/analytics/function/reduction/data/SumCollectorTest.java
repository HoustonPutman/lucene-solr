/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.analytics.function.reduction.data;

import java.util.ArrayList;

import org.apache.solr.analytics.function.reduction.data.SumCollector.SumData;
import org.apache.solr.analytics.stream.reservation.ReductionDataReservation;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.junit.Test;

public class SumCollectorTest extends AbstractReductionCollectorTest {
  
  @Test
  public void metadataTest() {
    SumCollector collector1 = new SumCollector(new TestDoubleValueStream());
    assertEquals("sum(test_double_value_stream)", collector1.getExpressionStr());
    assertEquals("sum", collector1.getName());

    SumCollector collector2 = new SumCollector(new TestDoubleValue(ExpressionType.UNREDUCED_MAPPING));
    assertEquals("sum(test_double_value)", collector2.getExpressionStr());
    assertEquals("sum", collector2.getName());

    SumCollector collector3 = new SumCollector(new TestFloatValue());
    assertEquals("sum(test_float_value)", collector3.getExpressionStr());
    assertEquals("sum", collector3.getName());

    SumCollector collector4 = new SumCollector(new TestLongValue(ExpressionType.FIELD));
    assertEquals("sum(test_long_value)", collector4.getExpressionStr());
    assertEquals("sum", collector3.getName());
  }

  @Test
  public void setDataTest() {
    SumCollector collector = new SumCollector(new TestDoubleValueStream());
    SumData data = collector.newData();
    
    // Doesn't exist;
    data.exists = false;
    collector.setData(data);
    assertFalse(collector.exists());
    
    // Does exist
    data.exists = true;
    data.sum = 20.3214;
    collector.setData(data);
    assertTrue(collector.exists());
    assertEquals(20.3214, collector.sum(), .00000001);

    data.exists = true;
    data.sum = -23458.34;
    collector.setData(data);
    assertTrue(collector.exists());
    assertEquals(-23458.34, collector.sum(), .00000001);
  }

  @Test
  public void setMergedDataTest() {
    SumCollector collector = new SumCollector(new TestDoubleValueStream());
    SumData data = collector.newData();
    
    // Doesn't exist;
    data.exists = false;
    collector.setMergedData(data);
    assertFalse(collector.exists());
    
    // Does exist
    data.exists = true;
    data.sum = 20.3214;
    collector.setMergedData(data);
    assertTrue(collector.exists());
    assertEquals(20.3214, collector.sum(), .00000001);

    data.exists = true;
    data.sum = -23458.34;
    collector.setMergedData(data);
    assertTrue(collector.exists());
    assertEquals(-23458.34, collector.sum(), .00000001);
  }
  
  @Test
  public void shardCommunicationTest() {
    SumCollector receiver = new SumCollector(new TestDoubleValueStream());

    SumCollector shard1 = new SumCollector(new TestDoubleValueStream());
    SumCollector shard2 = new SumCollector(new TestDoubleValueStream());
    SumCollector shard3 = new SumCollector(new TestDoubleValueStream());
    
    // Setting receiver data
    SumData data = receiver.newDataIO();
    
    // Filling shard1
    SumData data1 = shard1.newDataIO();
    data1.exists = true;
    data1.sum = 103.45;
    
    
    ArrayList<ReductionDataReservation<?,?>> reservations = new ArrayList<>();
    shard1.submitReservations( resv -> reservations.add(resv) );

    // Filling shard2
    SumData data2 = shard2.newDataIO();
    data2.exists = false;
    data2.sum = -1234123.32;

    // Filling shard3
    SumData data3 = shard3.newData();
    data3.exists = true;
    data3.sum = 9546845.4333;
    shard3.dataIO(data3);
    
    streamShards(receiver, shard1, shard2, shard3);
    
    assertTrue(data.exists);
    assertEquals(9546948.8833, data.sum, .0000001);
    
    // None of the shards have existing data
    data1 = shard1.newDataIO();
    data2 = shard2.newDataIO();
    data3 = shard3.newDataIO();
    
    data = receiver.newDataIO();
    
    streamShards(receiver, shard1, shard2, shard3);
    
    assertFalse(data.exists);
  }
  
  @Test
  public void collectAndApplyTargetsTest() {
    TestDoubleValueStream val = new TestDoubleValueStream();
    SumCollector collector = new SumCollector(val);
    
    SumData lasting1 = collector.newData();
    SumData lasting2 = collector.newData();

    SumData data1 = collector.newData();
    SumData data2 = collector.newData();
    SumData data3 = collector.newData();
    
    collector.addLastingCollectTarget(lasting1);
    val.setValues(1, 5, 7.43);
    collector.collectAndApply();
    
    collector.addCollectTarget(data1);
    collector.addCollectTarget(data2);
    val.setValues(-10);
    collector.collectAndApply();
    
    collector.addLastingCollectTarget(lasting2);
    collector.addCollectTarget(data3);
    val.setValues();
    collector.collectAndApply();

    collector.addCollectTarget(data1);
    val.setValues(103, -24.23);
    collector.collectAndApply();
    
    collector.clearLastingCollectTargets();
    collector.addCollectTarget(data2);
    val.setValues(5, 1032);
    collector.collectAndApply();
    
    collector.addLastingCollectTarget(lasting2);
    val.setValues(3);
    collector.collectAndApply();
    
    assertTrue(lasting1.exists);
    assertEquals(82.2, lasting1.sum, .000001);

    assertTrue(lasting2.exists);
    assertEquals(81.77, lasting2.sum, .000001);

    assertTrue(data1.exists);
    assertEquals(68.77, data1.sum, .000001);

    assertTrue(data2.exists);
    assertEquals(1027, data2.sum, .000001);

    assertFalse(data3.exists);
  }
}
