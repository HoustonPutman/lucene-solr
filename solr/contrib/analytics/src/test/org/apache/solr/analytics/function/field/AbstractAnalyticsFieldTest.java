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
import java.util.List;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.Filter;
import org.apache.solr.search.QueryWrapperFilter;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.RefCounted;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class AbstractAnalyticsFieldTest extends SolrTestCaseJ4 {

  private static SolrIndexSearcher searcher;
  private static RefCounted<SolrIndexSearcher> ref;
  
  @BeforeClass
  public static void createSchemaAndFields() throws Exception {
    initCore("solrconfig-analytics.xml","schema-analytics.xml");
    
    assertU(adoc("id", "-1"));
    for (int i = 0; i < 5; ++i) {
      assertU(adoc(
          "id", "" + i, 
          
          "int_i_t", "" + i,
          "int_im_t", "" + i,
          "int_im_t", "" + (i + 10),
          "int_im_t", "" + (i + 10),
          "int_im_t", "" + (i + 20),
          
          "int_i_p", "" + i,
          "int_im_p", "" + i,
          "int_im_p", "" + (i + 10),
          "int_im_p", "" + (i + 10),
          "int_im_p", "" + (i + 20),
          
          "long_l_t", "" + i,
          "long_lm_t", "" + i,
          "long_lm_t", "" + (i + 10),
          "long_lm_t", "" + (i + 10),
          "long_lm_t", "" + (i + 20),
          
          "long_l_p", "" + i,
          "long_lm_p", "" + i,
          "long_lm_p", "" + (i + 10),
          "long_lm_p", "" + (i + 10),
          "long_lm_p", "" + (i + 20),
          
          "float_f_t", "" + (i + .75f),
          "float_fm_t", "" + (i + .75f),
          "float_fm_t", "" + (i + 10.75f),
          "float_fm_t", "" + (i + 10.75f),
          "float_fm_t", "" + (i + 20.75f),
          
          "float_f_p", "" + (i + .75f),
          "float_fm_p", "" + (i + .75f),
          "float_fm_p", "" + (i + 10.75f),
          "float_fm_p", "" + (i + 10.75f),
          "float_fm_p", "" + (i + 20.75f),
          
          "double_d_t", "" + (i + .5),
          "double_dm_t", "" + (i + .5),
          "double_dm_t", "" + (i + 10.5),
          "double_dm_t", "" + (i + 10.5),
          "double_dm_t", "" + (i + 20.5),
          
          "double_d_p", "" + (i + .5),
          "double_dm_p", "" + (i + .5),
          "double_dm_p", "" + (i + 10.5),
          "double_dm_p", "" + (i + 10.5),
          "double_dm_p", "" + (i + 20.5),
          
          "date_dt_t", "180" + i + "-12-31T23:59:59Z",
          "date_dtm_t", "180" + i + "-12-31T23:59:59Z",
          "date_dtm_t", "18" + (i + 10) + "-12-31T23:59:59Z",
          "date_dtm_t", "18" + (i + 10) + "-12-31T23:59:59Z",
          "date_dtm_t", "18" + (i + 20) + "-12-31T23:59:59Z",
          
          "date_dt_p", "180" + i + "-12-31T23:59:59Z",
          "date_dtm_p", "180" + i + "-12-31T23:59:59Z",
          "date_dtm_p", "18" + (i + 10) + "-12-31T23:59:59Z",
          "date_dtm_p", "18" + (i + 10) + "-12-31T23:59:59Z",
          "date_dtm_p", "18" + (i + 20) + "-12-31T23:59:59Z",
          
          "string_s", "abc" + i,
          "string_sm", "abc" + i,
          "string_sm", "def" + i,
          "string_sm", "def" + i,
          "string_sm", "ghi" + i,
          
          "boolean_b", "true",
          "boolean_bm", "false",
          "boolean_bm", "true",
          "boolean_bm", "false"
      ));
    }
    assertU(commit());
    
    ref = h.getCore().getSearcher();
    searcher = ref.get();
  }
  
  @AfterClass
  public static void closeSearcher() throws IOException {
    ref.decref();
  }
  
  protected boolean emptyValueFound;
  
  protected void testFieldValues(AnalyticsField testField, Runnable fieldFilled) throws IOException {
    Filter filter = new QueryWrapperFilter(new MatchAllDocsQuery());
    
    List<LeafReaderContext> contexts = searcher.getTopReaderContext().leaves();
    for (int leafNum = 0; leafNum < contexts.size(); leafNum++) {
      LeafReaderContext context = contexts.get(leafNum);
      DocIdSet dis = filter.getDocIdSet(context, null); // solr docsets already exclude any deleted docs
      if (dis == null) {
        continue;
      }
      DocIdSetIterator disi = dis.iterator();
      if (disi != null) {
        testField.doSetNextReader(context);
        int doc = disi.nextDoc();
        while( doc != DocIdSetIterator.NO_MORE_DOCS){
          // Add a document to the statistics being generated
          testField.collect(doc);
          fieldFilled.run();
          doc = disi.nextDoc();
        }
      }
    }
  }
}
