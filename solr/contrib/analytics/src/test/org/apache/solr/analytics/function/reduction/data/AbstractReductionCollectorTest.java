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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.LinkedList;

import org.apache.solr.SolrTestCaseJ4;

public abstract class AbstractReductionCollectorTest extends SolrTestCaseJ4 {
  
  /**
   * This method is used to test shard communication for Reduction Collectors.
   * After the method is called, the receiver IO data should contain merged data from all of the shards.
   * 
   * @param receiver the collector to receive shard data
   * @param shards the shard collectors to send shard reduction data
   */
  protected void streamShards(ReductionDataCollector<?> receiver, ReductionDataCollector<?>... shards) {
    for (ReductionDataCollector<?> shard : shards) {
      final LinkedList<Integer> bytes = new LinkedList<>();
      
      DataOutputStream output =  new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          bytes.add(b);
        }
      });
      DataInputStream input =  new DataInputStream(new InputStream() {
        @Override
        public int read() throws IOException {
          return bytes.remove();
        }
      });

      // Creating shard Writers
      shard.submitReservations( resv -> {
        try {
          resv.createWriteStream(output).write();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } );
      
      // Creating receiver Readers
      receiver.submitReservations( resv -> {
        try {
          resv.createReadStream(input).read();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      } );
    }
  }
}
