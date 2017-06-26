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
package org.apache.solr.handler.component;

import java.io.IOException;

import org.apache.solr.analytics.AnalyticsDriver;
import org.apache.solr.analytics.AnalyticsRequestParser;
import org.apache.solr.analytics.ExpressionFactory;
import org.apache.solr.analytics.stream.AnalyticsShardRequestManager;
import org.apache.solr.analytics.util.OldAnalyticsParams;
import org.apache.solr.analytics.util.OldAnalyticsRequestConverter;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.analytics.util.AnalyticsResponseHeadings;

/**
 * Computes analytics requests.
 */
public class AnalyticsComponent extends SearchComponent {
  public static final String COMPONENT_NAME = "analytics";
  
  @Override
  public void init(NamedList args) {
    AnalyticsRequestParser.init();
  }

  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    // First check to see if there is an analytics request using the current format
    String analyticsRequest = rb.req.getParams().get(AnalyticsRequestParser.analyticsParamName);
    rb._isOlapAnalytics = false;
    rb.doAnalytics = false;
    if (analyticsRequest != null) {
      rb.doAnalytics = true;
      rb._analyticsRequestManager = AnalyticsRequestParser.parse(analyticsRequest, new ExpressionFactory(rb.req.getSchema()), rb.isDistrib);
    }
    // If there is no request in the current format, check for the old olap-style format
    else if (rb.req.getParams().getBool(OldAnalyticsParams.OLD_ANALYTICS,false)) {
      rb._analyticsRequestManager = AnalyticsRequestParser.parse(OldAnalyticsRequestConverter.convert(rb.req.getParams()), new ExpressionFactory(rb.req.getSchema()), rb.isDistrib);
      rb._isOlapAnalytics = true;
      rb.doAnalytics = true;
    }
    
    if (rb.doAnalytics) {
      rb._analyticsRequestManager.sendShards = false;
      
      // Check to see if the request is distributed
      if (rb.isDistrib) {
        rb._analyticsRequestManager.sendShards = true;
        rb._analyticsRequestManager.shardStream = new AnalyticsShardRequestManager(rb.req.getParams(), rb._analyticsRequestManager);
      } else {
        rb.setNeedDocSet( true );
      }
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    if (!rb.doAnalytics) {
      return;
    }
    
    // Collect the data and generate a response
    AnalyticsDriver.drive(rb._analyticsRequestManager, rb.req.getSearcher(), rb.getResults().docSet.getTopFilter(), rb.req);
    
    if (rb._isOlapAnalytics) {
      rb.rsp.add(AnalyticsResponseHeadings.COMPLETED_OLD_HEADER, rb._analyticsRequestManager.createOldResponse());
    } else {
      rb.rsp.add(AnalyticsResponseHeadings.COMPLETED_HEADER, rb._analyticsRequestManager.createResponse());
    }

    rb.doAnalytics = false;
  }
  
  
  @Override
  public int distributedProcess(ResponseBuilder rb) throws IOException {
    if (!rb.doAnalytics || !rb._analyticsRequestManager.sendShards || rb.stage != ResponseBuilder.STAGE_EXECUTE_QUERY) {
      return ResponseBuilder.STAGE_DONE;
    }
    
    // Send out a request to each shard and merge the responses into our AnalyticsRequestManager
    rb._analyticsRequestManager.shardStream.sendRequests(rb.req.getCore().getCoreDescriptor().getCollectionName(),
        rb.req.getCore().getCoreContainer().getZkController().getZkServerAddress());
    
    rb._analyticsRequestManager.sendShards = false;
    
    return ResponseBuilder.STAGE_DONE;
  }
  
  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    // We don't want the shard requests to compute analytics, since we send
    // separate requests for that in distributedProcess() to the AnalyticsHandler
    sreq.params.remove(AnalyticsRequestParser.analyticsParamName);
    sreq.params.remove(OldAnalyticsParams.OLD_ANALYTICS);
    
    super.modifyRequest(rb, who, sreq);
  }
  
  @Override
  public void handleResponses(ResponseBuilder rb, ShardRequest sreq) {
    
    // NO-OP since analytics shard responses are handled through the AnalyticsResponseParser
    
    super.handleResponses(rb, sreq);
  }
 
  @Override
  public void finishStage(ResponseBuilder rb) {
    if (rb.doAnalytics && rb.stage == ResponseBuilder.STAGE_GET_FIELDS) {
      // Generate responses from the merged shard data
      if (rb._isOlapAnalytics) {
        rb.rsp.add(AnalyticsResponseHeadings.COMPLETED_OLD_HEADER, rb._analyticsRequestManager.createOldResponse());
      } else {
        rb.rsp.add(AnalyticsResponseHeadings.COMPLETED_HEADER, rb._analyticsRequestManager.createResponse());
      }
    }
    
    super.finishStage(rb);
  }
  
  
  @Override
  public String getName() {
    return COMPONENT_NAME;
  }
  
  @Override
  public String getDescription() {
    return "Perform analytics";
  }

  /*@Override
  public NamedList getStatistics() {
    return analyticsCollector.getStatistics();
  }*/
}
