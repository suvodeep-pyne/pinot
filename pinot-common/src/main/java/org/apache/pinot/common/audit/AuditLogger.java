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
 * Utility class for audit logging in Pinot Controller.
 * Uses SLF4J with structured JSON logging format as specified in Phase 1 audit logging requirements.
 */
public final class AuditLogger {

  private static final Logger AUDIT_LOGGER = LoggerFactory.getLogger("audit.controller");
  private static final Logger ERROR_LOGGER = LoggerFactory.getLogger(AuditLogger.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private AuditLogger() {
    // Utility class - prevent instantiation
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

    try {
      String jsonLog = OBJECT_MAPPER.writeValueAsString(auditEvent);
      AUDIT_LOGGER.info(jsonLog);
    } catch (Exception e) {
      // Graceful degradation: Never let audit logging failures affect the main request
      ERROR_LOGGER.warn("Failed to write audit log entry for endpoint: {} method: {}", auditEvent.getEndpoint(),
          auditEvent.getMethod(), e);
    }
  }

  /**
   * Checks if audit logging is enabled (INFO level on audit.controller logger).
   * Can be used to avoid expensive request payload processing when audit logging is disabled.
   *
   * @return true if audit logging is enabled
   */
  public static boolean isEnabled() {
    return AUDIT_LOGGER.isInfoEnabled();
  }
}