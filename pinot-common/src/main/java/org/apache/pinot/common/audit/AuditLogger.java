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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Utility class for audit logging in Pinot components.
 * Uses SLF4J with structured JSON logging format and supports dynamic configuration.
 */
public final class AuditLogger {

  // Default Pinot logger. For logging failures in audit logging itself
  private static final Logger LOG = LoggerFactory.getLogger(AuditLogger.class);
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Configuration manager for dynamic configuration support
  private static volatile AuditConfigManager _configManager;

  // Cache for current logger to avoid repeated LoggerFactory calls
  private static volatile Logger _currentLogger;

  private AuditLogger() {
    // Utility class - prevent instantiation
  }

  /**
   * Sets the configuration manager for dynamic audit configuration.
   * This should be called during component startup.
   *
   * @param configManager the audit configuration manager
   */
  public static void setConfigManager(AuditConfigManager configManager) {
    _configManager = configManager;
    updateCurrentLogger();
    LOG.info("AuditLogger initialized with config manager");
  }

  /**
   * Logs an audit event as structured JSON at INFO level.
   * Implements graceful degradation - audit logging failures will not propagate
   * and will be logged separately for monitoring.
   *
   * @param auditEvent the audit event to log
   */
  public static void log(AuditEvent auditEvent) {
    if (auditEvent == null) {
      return;
    }

    // Check if audit logging is enabled
    if (!isEnabled()) {
      return;
    }

    try {
      String jsonLog = OBJECT_MAPPER.writeValueAsString(auditEvent);
      Logger currentLogger = getCurrentLogger();
      currentLogger.info(jsonLog);
    } catch (Exception e) {
      // Graceful degradation: Never let audit logging failures affect the main request
      LOG.warn("Failed to write audit log entry for endpoint: {} method: {}", auditEvent.getEndpoint(),
          auditEvent.getMethod(), e);
    }
  }

  /**
   * Checks if audit logging is enabled.
   * Considers both configuration settings and logger level.
   * Can be used to avoid expensive request payload processing when audit logging is disabled.
   *
   * @return true if audit logging is enabled
   */
  public static boolean isEnabled() {
    // If no config manager is set, fall back to logger check only (backward compatibility)
    if (_configManager == null) {
      Logger fallbackLogger = LoggerFactory.getLogger("audit");
      return fallbackLogger.isInfoEnabled();
    }

    // Check configuration first
    if (!_configManager.isEnabled()) {
      return false;
    }

    // Also check that the logger is enabled at INFO level
    Logger currentLogger = getCurrentLogger();
    return currentLogger != null && currentLogger.isInfoEnabled();
  }

  /**
   * Gets the current logger based on configuration.
   * Updates the logger if the configuration has changed.
   */
  private static Logger getCurrentLogger() {
    if (_configManager == null) {
      // Fallback for backward compatibility
      return LoggerFactory.getLogger("audit");
    }

    // Update logger if needed (configuration might have changed)
    updateCurrentLogger();
    return _currentLogger;
  }

  /**
   * Updates the current logger based on the current configuration.
   */
  private static void updateCurrentLogger() {
    if (_configManager == null) {
      _currentLogger = LoggerFactory.getLogger("audit");
      return;
    }

    AuditConfig config = _configManager.getCurrentConfig();
    String loggerName = config.getLoggerName();

    // Only update if the logger name has changed
    if (_currentLogger == null || !_currentLogger.getName().equals(loggerName)) {
      _currentLogger = LoggerFactory.getLogger(loggerName);
      LOG.debug("Updated audit logger to: {}", loggerName);
    }
  }
}
