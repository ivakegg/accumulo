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
package org.apache.accumulo.server.master.recovery;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.util.CachedConfiguration;
import org.apache.accumulo.server.fs.ViewFSUtils;
import org.apache.accumulo.server.fs.VolumeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;

public class HadoopForcingLogCloser implements LogCloser {

  private static final Logger log = LoggerFactory.getLogger(HadoopForcingLogCloser.class);

  @Override
  public long close(AccumuloConfiguration conf, VolumeManager fs, Path source) throws IOException {
    FileSystem ns = fs.getVolumeByPath(source).getFileSystem();

    // check if the file exists.  If not then look to see if we made a copy
    if (!ns.exists(source)) {
      Path copy = getNewLogFile(source, "copy");
      if (ns.exists(copy)) {
        ns.rename(copy, source);
      }
    }

    // if path points to a viewfs path, then resolve to underlying filesystem
    if (ViewFSUtils.isViewFS(ns)) {
      Path newSource = ns.resolvePath(source);
      if (!newSource.equals(source) && newSource.toUri().getScheme() != null) {
        ns = newSource.getFileSystem(CachedConfiguration.getInstance());
        source = newSource;
      }
    }

    if (ns instanceof DistributedFileSystem) {
      DistributedFileSystem dfs = (DistributedFileSystem) ns;
      try {
        if (!dfs.recoverLease(source)) {
          log.info("Waiting for file to be closed " + source.toString());
          return conf.getTimeInMillis(Property.MASTER_LEASE_RECOVERY_WAITING_PERIOD);
        }
        log.info("Recovered lease on " + source.toString());
      } catch (FileNotFoundException ex) {
        throw ex;
      } catch (Exception ex) {
        log.warn("Error recovering lease on " + source.toString(), ex);
        try {
          ns.append(source).close();
          log.info("Recovered lease on " + source.toString() + " using append");
        } catch (Exception ex2) {
          // so at this point we have attempted to recover the lease, and we tried to append and close it.
          // lets now copy this file out
          Path copy = getNewLogFile(source, "copy");

          // if the copy exists, already (previous copy attempt), then remove it
          int retry = 10;
          while(ns.exists(copy) && retry-- > 0) {
            if (!ns.delete(copy, false)) {
              // wait a moment
              try {
                Thread.sleep(100);
              } catch (InterruptedException ie) {
                throw new RuntimeException("Thread interrupted", ie);
              }
            }
          }
          if (ns.exists(copy)) {
            throw new IOException("Cannot delete previous copy of " + source);
          }

          // now copy the source to the copy
          FileUtil.copy(ns, source, ns, copy, true, ns.getConf());

          // move the source out of the way
          Path original = getNewLogFile(source, "orig");
          ns.rename(source, original);

          // right here is a hole....if the master dies here then the next time we recover
          // the code at the beginning of this method will hopefully find
          // Instead we may need the temp file to be registered as a wal to recover and then simply delete source

          // rename the copy to the source
          ns.rename(copy, source);

          // right here is another hole....if the master dies here then we will leave the original around but under
          // a different name.  This should only be a problem if this happens a lot

          // and finally delete the original
          ns.delete(original, false);
        }
      }
    } else if (ns instanceof LocalFileSystem || ns instanceof RawLocalFileSystem) {
      // ignore
    } else {
      throw new IllegalStateException(
          "Don't know how to recover a lease for " + ns.getClass().getName());
    }
    return 0;
  }

  Path getNewLogFile(Path origLogFile, String suffix) {
      return new Path(origLogFile.getParent(), origLogFile.getName()+"." + suffix);
  }

}
