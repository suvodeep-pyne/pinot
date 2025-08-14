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

import org.apache.pinot.spi.config.provider.PinotClusterConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;


/**
 * Thread-safe configuration manager for audit logging settings.
 * Handles dynamic configuration updates from cluster configuration changes.
 * Self-registers with the provided cluster config provider.
 */
public final class AuditConfigManager {

  private static final Logger LOG = LoggerFactory.getLogger(AuditConfigManager.class);

  private volatile AuditConfig _currentConfig;

  /**
   * Creates a new AuditConfigManager and registers with the cluster config provider.
   *
   * @param clusterConfigProvider the cluster config provider to register with
   */
  public AuditConfigManager(PinotClusterConfigProvider clusterConfigProvider) {
    requireNonNull(clusterConfigProvider, "Cluster config provider cannot be null");

    // Initialize with default configuration
    _currentConfig = new AuditConfig();

    // Create and register the config change listener
    AuditConfigChangeListener configChangeListener = new AuditConfigChangeListener(this);
    boolean registered = clusterConfigProvider.registerClusterConfigChangeListener(configChangeListener);

    if (registered) {
      LOG.info("Successfully registered audit config change listener with cluster config provider");
    } else {
      LOG.error("Failed to register audit config change listener with cluster config provider");
    }
  }

  /**
   * Gets the current audit configuration.
   * This method is thread-safe and lock-free.
   *
   * @return the current audit configuration
   */
  public AuditConfig getCurrentConfig() {
    return _currentConfig;
  }

  /**
   * Updates the current configuration atomically.
   * This method is called by the config change listener and should not be called directly.
   *
   * @param newConfig the new configuration to apply
   */
  void updateConfiguration(AuditConfig newConfig) {
    requireNonNull(newConfig, "New config cannot be null");

    AuditConfig oldConfig = _currentConfig;
    _currentConfig = newConfig;

    LOG.info("Audit configuration updated from {} to {}", oldConfig, newConfig);
  }

  /**
   * Checks if audit logging is currently enabled.
   * Convenience method that delegates to the current configuration.
   *
   * @return true if audit logging is enabled
   */
  public boolean isEnabled() {
    return _currentConfig.isEnabled();
  }

  /**
   * Checks if the given endpoint should be excluded from audit logging.
   * Uses the utility method from AuditConfigChangeListener.
   *
   * @param endpoint the endpoint path to check
   * @return true if the endpoint should be excluded
   */
  public boolean isEndpointExcluded(String endpoint) {
    return AuditConfigChangeListener.isEndpointExcluded(endpoint, _currentConfig.getExcludedEndpoints());
  }

  @Override
  public String toString() {
    return "AuditConfigManager{currentConfig=" + _currentConfig + "}";
  }
}
