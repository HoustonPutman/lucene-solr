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

import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.analytics.value.AnalyticsValueStream;
import org.apache.solr.analytics.value.AnalyticsValueStream.ExpressionType;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValue;
import org.apache.solr.analytics.value.FillableTestValue.TestDoubleValueStream;
import org.apache.solr.analytics.value.FillableTestValue.TestFloatValue;
import org.apache.solr.analytics.value.FillableTestValue.TestLongValue;
import org.junit.Test;

public class SumFunctionTest extends SolrTestCaseJ4 {
  
  @Test
  public void metadataTest() {
    AnalyticsValueStream func1 = SumFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDoubleValueStream()});
    assertEquals("sum(test_double_value_stream)", func1.getExpressionStr());
    assertEquals("sum", func1.getName());
    assertEquals(ExpressionType.REDUCTION, func1.getExpressionType());

    AnalyticsValueStream func2 =  SumFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestDoubleValue(ExpressionType.UNREDUCED_MAPPING)});
    assertEquals("sum(test_double_value)", func2.getExpressionStr());
    assertEquals("sum", func2.getName());
    assertEquals(ExpressionType.REDUCTION, func2.getExpressionType());

    AnalyticsValueStream func3 =  SumFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestFloatValue()});
    assertEquals("sum(test_float_value)", func3.getExpressionStr());
    assertEquals("sum", func3.getName());
    assertEquals(ExpressionType.REDUCTION, func3.getExpressionType());

    AnalyticsValueStream func4 =  SumFunction.creatorFunction.apply(new AnalyticsValueStream[] {new TestLongValue(ExpressionType.FIELD)});
    assertEquals("sum(test_long_value)", func4.getExpressionStr());
    assertEquals("sum", func4.getName());
    assertEquals(ExpressionType.REDUCTION, func4.getExpressionType());
  }
}
