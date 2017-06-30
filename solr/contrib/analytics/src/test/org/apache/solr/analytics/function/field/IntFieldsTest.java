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
package org.apache.solr.analytics.function.field;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class IntFieldsTest extends AbstractAnalyticsFieldTest {

  @Test
  public void singleValuedTrieIntTest() throws IOException {
    IntField valueField = new IntField("int_i_t");
    Set<Integer> values = new HashSet<>();
    values.add(0);
    values.add(1);
    values.add(2);
    values.add(3);
    values.add(4);
    
    emptyValueFound = false;
    
    testFieldValues(valueField, () -> {
      int value = valueField.getInt();
      if (valueField.exists()) {
        assertTrue("Incorrect or duplicate value found", values.remove(value));
      } else {
        assertFalse("Multiple missing values found", emptyValueFound);
        emptyValueFound = true;
      }
    });
    assertTrue("Missing value not found", emptyValueFound);
    assertEquals("Not all values found", 0, values.size());
  }

  @Test
  public void singleValuedPointIntTest() throws IOException {
    IntField valueField = new IntField("int_i_p");
    Set<Integer> values = new HashSet<>();
    values.add(0);
    values.add(1);
    values.add(2);
    values.add(3);
    values.add(4);
    
    emptyValueFound = false;
    
    testFieldValues(valueField, () -> {
      int value = valueField.getInt();
      if (valueField.exists()) {
        assertTrue("Incorrect or duplicate value found", values.remove(value));
      } else {
        assertFalse("Multiple missing values found", emptyValueFound);
        emptyValueFound = true;
      }
    });
    assertTrue("Missing value not found", emptyValueFound);
    assertEquals("Not all values found", 0, values.size());
  }

  /*@Test
  public void multiValuedTrieIntTest() throws IOException {
    IntField valueField = new IntField("int_im_t");
    Set<Set<Integer>> values = new HashSet<>();
    Set<Integer> documentValues = new HashSet<>();
    documentValues.add(0);
    documentValues.add(10);
    documentValues.add(20);
    values.add(2);
    values.add(3);
    values.add(4);
    
    emptyValueFound = false;
    
    testFieldValues(valueField, () -> {
      Set<Integer> foundValues = new HashSet<>();
      valueField.streamInts(value -> foundValues.add(value));
      if (foundValues.size() > 0) {
        assertTrue("Incorrect or duplicate value found", values.remove(foundValues));
      } else {
        assertFalse("Multiple missing values found", emptyValueFound);
        emptyValueFound = true;
      }
    });
    assertTrue("Missing value not found", emptyValueFound);
    assertEquals("Not all values found", 0, values.size());
  }

  @Test
  public void multiValuedPointIntTest() throws IOException {
    IntField valueField = new IntField("int_im_p");
    Set<Integer> values = new HashSet<>();
    values.add(0);
    values.add(1);
    values.add(2);
    values.add(3);
    values.add(4);
    
    emptyValueFound = false;
    
    testFieldValues(valueField, () -> {
      int value = valueField.getInt();
      if (valueField.exists()) {
        assertTrue("Incorrect or duplicate value found", values.remove(value));
      } else {
        assertFalse("Multiple missing values found", emptyValueFound);
        emptyValueFound = true;
      }
    });
    assertTrue("Missing value not found", emptyValueFound);
    assertEquals("Not all values found", 0, values.size());
  }*/
}
