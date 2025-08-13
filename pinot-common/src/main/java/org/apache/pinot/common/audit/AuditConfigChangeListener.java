/**
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
package org.apache.pinot.common.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.spi.config.provider.PinotClusterConfigChangeListener;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.utils.CommonConstants.Server.*;


/**
 * Internal cluster config change listener for audit configuration updates.
 * This is a package-private class used internally by AuditConfigManager.
 */
final class AuditConfigChangeListener implements PinotClusterConfigChangeListener {

  private static final Logger LOG = LoggerFactory.getLogger(AuditConfigChangeListener.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private static final Set<String> AUDIT_CONFIG_KEYS =
      Set.of(CONFIG_OF_AUDIT_ENABLED, CONFIG_OF_AUDIT_CAPTURE_REQUEST_PAYLOAD, CONFIG_OF_AUDIT_EXCLUDED_ENDPOINTS,
          CONFIG_OF_AUDIT_CAPTURE_REQUEST_HEADERS, CONFIG_OF_AUDIT_MAX_PAYLOAD_SIZE, CONFIG_OF_AUDIT_LOGGER_NAME);

  private final AuditConfigManager _configManager;

  AuditConfigChangeListener(AuditConfigManager configManager) {
    _configManager = configManager;
  }

  @Override
  public void onChange(Set<String> changedConfigs, Map<String, String> clusterConfigs) {
    if (!hasAuditConfigChanges(changedConfigs)) {
      LOG.debug("ChangedConfigs: {} does not contain audit configs. Skipping updates", changedConfigs);
      return;
    }

    LOG.info("Audit configuration changed. ChangedConfigs: {}", changedConfigs);

    try {
      updateAuditConfiguration(clusterConfigs);
      LOG.info("Successfully updated audit configuration");
    } catch (Exception e) {
      LOG.error("Failed to update audit configuration", e);
    }
  }

  private boolean hasAuditConfigChanges(Set<String> changedConfigs) {
    return changedConfigs.stream().anyMatch(AUDIT_CONFIG_KEYS::contains);
  }

  private void updateAuditConfiguration(Map<String, String> clusterConfigs) {
    // Validate the new configuration first
    AuditConfigValidator.ValidationResult validationResult = AuditConfigValidator.validate(clusterConfigs);

    if (!validationResult.isValid()) {
      LOG.warn("Invalid audit configuration detected, keeping previous configuration: {}",
          validationResult.getErrorMessage());
      return;
    }

    // Build new configuration from cluster configs
    AuditConfig newConfig = buildConfigFromCluster(clusterConfigs);

    LOG.info("Updating audit configuration: {}", newConfig);
    _configManager.updateConfiguration(newConfig);
  }

  private AuditConfig buildConfigFromCluster(Map<String, String> clusterConfigs) {
    return mapPrefixedConfigToObject(clusterConfigs, "pinot.audit", AuditConfig.class);
  }

  /**
   * Maps cluster configuration properties with a common prefix to a POJO using Jackson.
   * Uses PinotConfiguration.subset() to extract properties with the given prefix and
   * Jackson's convertValue() for automatic object mapping.
   */
  private static <T> T mapPrefixedConfigToObject(Map<String, String> clusterConfigs,
                                                String prefix, Class<T> configClass) {
    MapConfiguration mapConfig = new MapConfiguration(clusterConfigs);
    PinotConfiguration pinotConfig = new PinotConfiguration(mapConfig);
    PinotConfiguration subsetConfig = pinotConfig.subset(prefix);
    Map<String, Object> configMap = subsetConfig.toMap();
    return OBJECT_MAPPER.convertValue(configMap, configClass);
  }

  /**
   * Checks if the given endpoint should be excluded from audit logging.
   * Supports simple wildcard matching with '*' character.
   */
  static boolean isEndpointExcluded(String endpoint, String excludedEndpointsString) {
    if (StringUtils.isBlank(endpoint) || StringUtils.isBlank(excludedEndpointsString)) {
      return false;
    }

    Set<String> excludedEndpoints = parseExcludedEndpoints(excludedEndpointsString);
    if (excludedEndpoints.isEmpty()) {
      return false;
    }

    // Check for exact matches first
    if (excludedEndpoints.contains(endpoint)) {
      return true;
    }

    // Check for wildcard matches
    for (String excluded : excludedEndpoints) {
      if (excluded.contains("*")) {
        if (matchesWildcard(endpoint, excluded)) {
          return true;
        }
      }
    }

    return false;
  }

  private static Set<String> parseExcludedEndpoints(String excludedEndpointsString) {
    Set<String> excludedEndpoints = new HashSet<>();
    if (StringUtils.isNotBlank(excludedEndpointsString)) {
      String[] endpoints = excludedEndpointsString.split(",");
      for (String endpoint : endpoints) {
        String trimmed = endpoint.trim();
        if (StringUtils.isNotBlank(trimmed)) {
          excludedEndpoints.add(trimmed);
        }
      }
    }
    return excludedEndpoints;
  }

  private static boolean matchesWildcard(String endpoint, String pattern) {
    if (pattern.equals("*")) {
      return true;
    }
    if (pattern.endsWith("/*")) {
      String prefix = pattern.substring(0, pattern.length() - 2);
      return endpoint.startsWith(prefix);
    }
    if (pattern.startsWith("*/")) {
      String suffix = pattern.substring(2);
      return endpoint.endsWith(suffix);
    }
    return false;
  }
}
