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
package org.apache.solr.analytics;

import java.io.IOException;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.QueryRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.AbstractDistribZkTestBase;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

public class AbstractAnalyticsCloudTest extends SolrCloudTestCase {
  
  protected static final String COLLECTIONORALIAS = "collection1";
  protected static final int TIMEOUT = DEFAULT_TIMEOUT;
  protected static final String id = "id";

  public static void setupCluster() throws Exception {
    configureCluster(4)
        .addConfig("conf", configset("cloud-analytics"))
        .configure();

    CollectionAdminRequest.createCollection(COLLECTIONORALIAS, "conf", 2, 1).process(cluster.getSolrClient());
    AbstractDistribZkTestBase.waitForRecoveriesToFinish(COLLECTIONORALIAS, cluster.getSolrClient().getZkStateReader(),
        false, true, TIMEOUT);
    cleanIndex();
  }

  public static void cleanIndex() throws Exception {
    new UpdateRequest()
        .deleteByQuery("*:*")
        .commit(cluster.getSolrClient(), COLLECTIONORALIAS);
  }

  protected NamedList<Object> queryCloudAnalytics(String[] testParams) throws SolrServerException, IOException, InterruptedException {
    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("q", "*:*");
    params.set("indent", "true");
    params.set("olap", "true");
    params.set("rows", "0");
    for (int i = 0; i + 1 < testParams.length;) {
      params.add(testParams[i++], testParams[i++]);
    }
    cluster.waitForAllNodes(10000);
    QueryRequest qreq = new QueryRequest(params);
    QueryResponse resp = qreq.process(cluster.getSolrClient(), COLLECTIONORALIAS);
    return resp.getResponse();
  }
}
