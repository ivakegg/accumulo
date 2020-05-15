/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.server.fs;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.accumulo.core.spi.common.ServiceEnvironment;
import org.apache.accumulo.core.volume.Volume;
import org.apache.accumulo.server.fs.VolumeChooserEnvironment.ChooserScope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PreferredVolumeChooser} that limits its choices from a given set of
 * options to the subset of those options preferred for a particular table (or
 * non-table) and then for the tserver host making the choice.  If no
 * configuration for the tserver host is defined, then this class delegates to
 * the PreferredVolumeChooser to make the choice.
 *
 * For tables, the configuration of a hostgroup regex is defined as follows
 * {properties later in the list override properties earlier in the list}:
 *
 * general.custom.volume.hostregex.{group}
 * table.custom.volume.hostregex.{group}
 *
 * Then the volumes for a given hostgroup are defined using the following
 * properties:
 *
 * general.custom.volume.preferred.default.{group}
 * table.custom.volume.preferred.{group}
 *
 * For non-tables (e.g. the logger scope), the configuration of a hostgroup
 * regex is defined using the following:
 *
 * general.custom.volume.hostregex.{group}
 * general.custom.volume.hostregex.{scope}.{group}
 *
 * Then the volumes for a given hostgroup are defined using the following:
 *
 * general.custom.volume.preferred.default.{group}
 * general.custom.volume.preferred.{scope}.{group}
 *
 */
public class HostRegexVolumeChooser extends PreferredVolumeChooser {
  private static final Logger log = LoggerFactory.getLogger(HostRegexVolumeChooser.class);

  private static final String TABLE_CUSTOM_HOSTREGEX_SUFFIX = "volume.hostregex.";

  private static final String getCustomPropertySuffix(ChooserScope scope, String group) {
    return TABLE_CUSTOM_HOSTREGEX_SUFFIX + scope.name().toLowerCase() + '.' + group;
  }

  @Override
  public String choose(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    log.trace("{}.choose", getClass().getSimpleName());
    // Randomly choose the volume from the preferred volumes
    String choice = super.choose(env, getPreferredVolumes(env, options));
    log.trace("Choice = {}", choice);
    return choice;
  }

  @Override
  public Set<String> choosable(VolumeChooserEnvironment env, Set<String> options)
      throws VolumeChooserException {
    return getPreferredVolumes(env, options);
  }

  // visible (not private) for testing
  @Override
  Set<String> getPreferredVolumes(VolumeChooserEnvironment env, Set<String> options) {
    if (env.getScope() == ChooserScope.TABLE) {
      return getPreferredVolumesForTableAndHost(env, options);
    }
    return getPreferredVolumesForScopeAndHost(env, options);
  }

  private Set<String> getPreferredVolumesForTableAndHost(VolumeChooserEnvironment env,
      Set<String> options) {

    // first lets determine the visible host groups for this table
    ServiceEnvironment.Configuration tableConfiguration =
        env.getServiceEnv().getConfiguration(env.getTableId());

    tableConfiguration.getCustom()

    log.trace("Looking up property {} + for Table id: {}", TABLE_CUSTOM_SUFFIX, env.getTableId());

    String preferredVolumes =
        env.getServiceEnv().getConfiguration(env.getTableId()).getTableCustom(TABLE_CUSTOM_SUFFIX);

    // fall back to global default scope, so setting only one default is necessary, rather than a
    // separate default for TABLE scope than other scopes
    if (preferredVolumes == null || preferredVolumes.isEmpty()) {
      preferredVolumes =
          env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_PREFERRED_VOLUMES);
    }

    // throw an error if volumes not specified or empty
    if (preferredVolumes == null || preferredVolumes.isEmpty()) {
      String msg = "Property " + TABLE_CUSTOM_SUFFIX + " or " + DEFAULT_SCOPED_PREFERRED_VOLUMES
          + " must be a subset of " + options + " to use the " + getClass().getSimpleName();
      throw new VolumeChooserException(msg);
    }

    return parsePreferred(TABLE_CUSTOM_SUFFIX, preferredVolumes, options);
  }

  private Set<String> getPreferredVolumesForScopeAndHost(VolumeChooserEnvironment env,
      Set<String> options) {
    ChooserScope scope = env.getScope();
    String property = getCustomPropertySuffix(scope);
    log.trace("Looking up property {} for scope: {}", property, scope);

    String preferredVolumes = env.getServiceEnv().getConfiguration().getCustom(property);

    // fall back to global default scope if this scope isn't configured (and not already default
    // scope)
    if ((preferredVolumes == null || preferredVolumes.isEmpty()) && scope != ChooserScope.DEFAULT) {
      log.debug("{} not found; using {}", property, DEFAULT_SCOPED_PREFERRED_VOLUMES);
      preferredVolumes =
          env.getServiceEnv().getConfiguration().getCustom(DEFAULT_SCOPED_PREFERRED_VOLUMES);

      // only if the custom property is not set to we fall back to the default scoped preferred
      // volumes
      if (preferredVolumes == null || preferredVolumes.isEmpty()) {
        String msg = "Property " + property + " or " + DEFAULT_SCOPED_PREFERRED_VOLUMES
            + " must be a subset of " + options + " to use the " + getClass().getSimpleName();
        throw new VolumeChooserException(msg);
      }

      property = DEFAULT_SCOPED_PREFERRED_VOLUMES;
    }

    return parsePreferred(property, preferredVolumes, options);
  }

  private Set<String> parsePreferred(String property, String preferredVolumes,
      Set<String> options) {
    log.trace("Found {} = {}", property, preferredVolumes);

    Set<String> preferred =
        Arrays.stream(preferredVolumes.split(",")).map(String::trim).collect(Collectors.toSet());
    if (preferred.isEmpty()) {
      String msg = "No volumes could be parsed from '" + property + "', which had a value of '"
          + preferredVolumes + "'";
      throw new VolumeChooserException(msg);
    }
    // preferred volumes should also exist in the original options (typically, from
    // instance.volumes)
    if (Collections.disjoint(preferred, options)) {
      String msg = "Some volumes in " + preferred + " are not valid volumes from " + options;
      throw new VolumeChooserException(msg);
    }

    return preferred;
  }
}
