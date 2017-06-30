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
package org.apache.solr.analytics.value;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.FillableTestValue.TestBooleanValue;
import org.junit.Test;

public class CastingBooleanValueTest extends SolrTestCaseJ4 {

  @Test
  public void stringCastingTest() {
    TestBooleanValue val = new TestBooleanValue();
    
    assertTrue(val instanceof StringValue);
    StringValue casted = (StringValue)val;

    val.setValue(false).setExists(true);
    assertEquals("false", casted.getString());
    assertTrue(casted.exists());

    val.setValue(true).setExists(true);
    assertEquals("true", casted.getString());
    assertTrue(casted.exists());
  }

  @Test
  public void objectCastingTest() {
    TestBooleanValue val = new TestBooleanValue();
    
    assertTrue(val instanceof AnalyticsValue);
    AnalyticsValue casted = (AnalyticsValue)val;

    val.setValue(false).setExists(true);
    assertEquals(new Boolean(false), casted.getObject());
    assertTrue(casted.exists());

    val.setValue(true).setExists(true);
    assertEquals(new Boolean(true), casted.getObject());
    assertTrue(casted.exists());
  }

  @Test
  public void booleanStreamCastingTest() {
    TestBooleanValue val = new TestBooleanValue();
    
    assertTrue(val instanceof BooleanValueStream);
    BooleanValueStream casted = (BooleanValueStream)val;
    
    // No values
    val.setExists(false);
    casted.streamBooleans( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(false).setExists(true);
    Iterator<Boolean> values = Arrays.asList(false).iterator();
    casted.streamBooleans( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void stringStreamCastingTest() {
    TestBooleanValue val = new TestBooleanValue();
    
    assertTrue(val instanceof StringValueStream);
    StringValueStream casted = (StringValueStream)val;
    
    // No values
    val.setExists(false);
    casted.streamStrings( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(false).setExists(true);
    Iterator<String> values = Arrays.asList("false").iterator();
    casted.streamStrings( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }

  @Test
  public void objectStreamCastingTest() {
    TestBooleanValue val = new TestBooleanValue();
    
    assertTrue(val instanceof AnalyticsValueStream);
    AnalyticsValueStream casted = (AnalyticsValueStream)val;
    
    // No values
    val.setExists(false);
    casted.streamObjects( value -> {
      assertTrue("There should be no values to stream", false);
    });

    // Multiple Values
    val.setValue(false).setExists(true);
    Iterator<Object> values = Arrays.<Object>asList(new Boolean(false)).iterator();
    casted.streamObjects( value -> {
      assertTrue(values.hasNext());
      assertEquals(values.next(), value);
    });
    assertFalse(values.hasNext());
  }
}
