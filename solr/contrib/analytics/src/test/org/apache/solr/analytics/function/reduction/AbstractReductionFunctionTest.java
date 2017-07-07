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
package org.apache.solr.analytics.function.reduction;

import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import org.apache.solr.analytics.function.reduction.data.ReductionDataCollector;
import org.junit.BeforeClass;

public abstract class AbstractReductionFunctionTest implements UnaryOperator<ReductionDataCollector<?>> {
  public static Map<String, ReductionDataCollector<?>> testCollectors;
  
  @BeforeClass
  public static void createTestCollectors() {
    testCollectors = new HashMap<>();
  }
  
  /*
   * This method is used to replace the real ReductionDataCollectors with test versions that we control.
   * This is so that we can just test the ReductionFunctions and not their ReductionDataCollectors.
   */
  public ReductionDataCollector<?> getTestCollector(ReductionDataCollector<?> collector) {
    return testCollectors.get(collector.getExpressionStr());
  }
}
