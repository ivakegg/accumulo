/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.test.functional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.FileNotFoundException;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.impl.ClientContext;
import org.apache.accumulo.core.client.impl.Credentials;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.impl.KeyExtent;
import org.apache.accumulo.fate.util.UtilWaitThread;
import org.apache.accumulo.minicluster.impl.MiniAccumuloConfigImpl;
import org.apache.accumulo.server.master.state.MetaDataTableScanner;
import org.apache.accumulo.server.master.state.TabletLocationState;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class AssignLocationModeIT extends ConfigurableMacBase {

  @Override
  public void configure(MiniAccumuloConfigImpl cfg, Configuration fsConf) {
    cfg.setProperty(Property.TSERV_LAST_LOCATION_MODE, "assignment");
  }

  @Test
  public void test() throws Exception {
    Connector c = getConnector();
    String tableName = super.getUniqueNames(1)[0];
    c.tableOperations().create(tableName);
    String tableId = c.tableOperations().tableIdMap().get(tableName);
    // wait for the table to be online
    TabletLocationState newTablet;
    do {
      UtilWaitThread.sleep(250);
      newTablet = getTabletLocationState(c, tableId);
    } while (newTablet.current == null);
    // this would be null if the mode was not "assign"
    assertEquals(newTablet.current, newTablet.last);
    assertNull(newTablet.future);

    // put something in it
    try (BatchWriter bw = c.createBatchWriter(tableName, new BatchWriterConfig())) {
      Mutation m = new Mutation("a");
      m.put("b", "c", "d");
      bw.addMutation(m);
    }
    // assert that the default mode is "assignment"
    assertEquals("assignment", c.instanceOperations().getSystemConfiguration()
        .get(Property.TSERV_LAST_LOCATION_MODE.getKey()));

    // last location should be set
    TabletLocationState unflushed = getTabletLocationState(c, tableId);
    assertEquals(newTablet.current, unflushed.current);
    assertEquals(newTablet.current, unflushed.last);
    assertNull(unflushed.future);

    // take the tablet offline
    c.tableOperations().offline(tableName, true);
    TabletLocationState offline = getTabletLocationState(c, tableId);
    assertNull(offline.future);
    assertNull(offline.current);
    assertEquals(unflushed.current, offline.last);

    // put it back online, should have the same last location
    c.tableOperations().online(tableName, true);
    TabletLocationState online = getTabletLocationState(c, tableId);
    assertNull(online.future);
    assertNotNull(online.current);
    assertEquals(online.current, online.last);
  }

  private TabletLocationState getTabletLocationState(Connector c, String tableId)
      throws FileNotFoundException, ConfigurationException {
    Credentials creds = new Credentials("root", new PasswordToken(ROOT_PASSWORD));
    ClientContext context =
        new ClientContext(c.getInstance(), creds, getCluster().getClientConfig());
    MetaDataTableScanner s =
        new MetaDataTableScanner(context, new Range(KeyExtent.getMetadataEntry(tableId, null)));
    TabletLocationState tlState = s.next();
    s.close();
    return tlState;
  }
}
