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

import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

import static org.apache.pinot.spi.utils.CommonConstants.AuditLog.*;
import static org.testng.Assert.*;


public class AuditConfigChangeListenerTest {

  @Test
  public void testConfigurationKeyMappingComplete() {
    // Test all configuration keys map correctly to AuditConfig properties
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(CONFIG_OF_AUDIT_ENABLED, "true");
    clusterConfigs.put(CONFIG_OF_AUDIT_CAPTURE_REQUEST_PAYLOAD, "false");
    clusterConfigs.put(CONFIG_OF_AUDIT_CAPTURE_REQUEST_HEADERS, "false");
    clusterConfigs.put(CONFIG_OF_AUDIT_MAX_PAYLOAD_SIZE, "8192");
    clusterConfigs.put(CONFIG_OF_AUDIT_LOGGER_NAME, "custom.audit.logger");
    clusterConfigs.put(CONFIG_OF_AUDIT_EXCLUDED_ENDPOINTS, "/health,/metrics");

    // Build config using the same method as the listener
    AuditConfig config = buildConfigFromCluster(clusterConfigs);

    // Verify all properties were correctly mapped
    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertFalse(config.isCaptureRequestPayload());
    assertFalse(config.isCaptureRequestHeaders());
    assertEquals(config.getMaxPayloadSize(), 8192);
    assertEquals(config.getLoggerName(), "custom.audit.logger");
    assertEquals(config.getExcludedEndpoints(), "/health,/metrics");
  }

  @Test
  public void testJacksonPropertyMappingDirect() {
    // Test Jackson mapping directly to ensure @JsonProperty annotations work correctly
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put("pinot.audit.enabled", "true");
    clusterConfigs.put("pinot.audit.capture.request.payload", "false");
    clusterConfigs.put("pinot.audit.capture.request.headers", "true");
    clusterConfigs.put("pinot.audit.max.payload.size", "5120");
    clusterConfigs.put("pinot.audit.logger.name", "test.logger");
    clusterConfigs.put("pinot.audit.excluded.endpoints", "/debug/*,/admin");

    // Build config using the same method as the listener
    AuditConfig config = buildConfigFromCluster(clusterConfigs);

    // Verify Jackson correctly mapped the dotted property names
    assertTrue(config.isEnabled());
    assertFalse(config.isCaptureRequestPayload());
    assertTrue(config.isCaptureRequestHeaders());
    assertEquals(config.getMaxPayloadSize(), 5120);
    assertEquals(config.getLoggerName(), "test.logger");
    assertEquals(config.getExcludedEndpoints(), "/debug/*,/admin");
  }

  @Test
  public void testPartialConfiguration() {
    // Test that partial configuration works with defaults
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(CONFIG_OF_AUDIT_ENABLED, "true");
    clusterConfigs.put(CONFIG_OF_AUDIT_LOGGER_NAME, "partial.logger");

    // Build config using the same method as the listener
    AuditConfig config = buildConfigFromCluster(clusterConfigs);

    // Verify enabled and logger name are set, others use defaults
    assertNotNull(config);
    assertTrue(config.isEnabled());
    assertEquals(config.getLoggerName(), "partial.logger");
    // Verify defaults are maintained
    assertTrue(config.isCaptureRequestPayload()); // default is true
    assertTrue(config.isCaptureRequestHeaders()); // default is true
    assertEquals(config.getMaxPayloadSize(), 10240); // default
    assertEquals(config.getExcludedEndpoints(), ""); // default
  }

  @Test
  public void testEmptyConfiguration() {
    // Test that empty configuration uses all defaults
    Map<String, String> clusterConfigs = new HashMap<>();

    // Build config using the same method as the listener
    AuditConfig config = buildConfigFromCluster(clusterConfigs);

    // Verify all defaults are used
    assertNotNull(config);
    assertFalse(config.isEnabled()); // default is false
    assertTrue(config.isCaptureRequestPayload()); // default is true
    assertTrue(config.isCaptureRequestHeaders()); // default is true
    assertEquals(config.getMaxPayloadSize(), 10240); // default
    assertEquals(config.getLoggerName(), "audit"); // default
    assertEquals(config.getExcludedEndpoints(), ""); // default
  }

  @Test
  public void testNonAuditConfigurationIgnored() {
    // Test that non-audit configuration properties are ignored during mapping
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put("pinot.controller.host", "localhost");
    clusterConfigs.put("pinot.controller.port", "9000");
    clusterConfigs.put(CONFIG_OF_AUDIT_ENABLED, "true");

    // Build config using the same method as the listener
    AuditConfig config = buildConfigFromCluster(clusterConfigs);

    // Verify only audit configuration was processed
    assertNotNull(config);
    assertTrue(config.isEnabled()); // audit config was processed
    // Non-audit configs are ignored and defaults are used for other audit properties
    assertTrue(config.isCaptureRequestPayload()); // default
    assertEquals(config.getLoggerName(), "audit"); // default
  }

  @Test
  public void testEndpointExclusionExactMatch() {
    // Test exact endpoint matching
    String excludedEndpoints = "/health,/metrics,/debug";

    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/health", excludedEndpoints));
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/metrics", excludedEndpoints));
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/debug", excludedEndpoints));
    assertFalse(AuditConfigChangeListener.isEndpointExcluded("/api/v1/tables", excludedEndpoints));
    assertFalse(AuditConfigChangeListener.isEndpointExcluded("/healthcheck", excludedEndpoints));
  }

  @Test
  public void testEndpointExclusionWildcardMatch() {
    // Test wildcard endpoint matching
    String excludedEndpoints = "/debug/*,*/internal,/admin/status";

    // Test prefix wildcard - /debug/* matches anything starting with /debug
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/debug/config", excludedEndpoints));
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/debug/threads", excludedEndpoints));
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/debug", excludedEndpoints)); // exact also matches prefix

    // Test suffix wildcard
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/api/internal", excludedEndpoints));
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/system/internal", excludedEndpoints));

    // Test exact match still works
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/admin/status", excludedEndpoints));

    // Test non-matches
    assertFalse(AuditConfigChangeListener.isEndpointExcluded("/api/v1/tables", excludedEndpoints));
  }

  @Test
  public void testEndpointExclusionEdgeCases() {
    // Test edge cases for endpoint exclusion
    assertFalse(AuditConfigChangeListener.isEndpointExcluded("", "/health"));
    assertFalse(AuditConfigChangeListener.isEndpointExcluded("/health", ""));
    assertFalse(AuditConfigChangeListener.isEndpointExcluded(null, "/health"));
    assertFalse(AuditConfigChangeListener.isEndpointExcluded("/health", null));

    // Test universal wildcard
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/anything", "*"));

    // Test comma handling
    String excludedEndpoints = " /health , /metrics , ";
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/health", excludedEndpoints));
    assertTrue(AuditConfigChangeListener.isEndpointExcluded("/metrics", excludedEndpoints));
  }

  @Test
  public void testConfigurationKeyConstants() {
    // Verify that all expected configuration keys are defined and have correct values
    assertEquals(CONFIG_OF_AUDIT_ENABLED, "pinot.audit.enabled");
    assertEquals(CONFIG_OF_AUDIT_CAPTURE_REQUEST_PAYLOAD, "pinot.audit.capture.request.payload");
    assertEquals(CONFIG_OF_AUDIT_CAPTURE_REQUEST_HEADERS, "pinot.audit.capture.request.headers");
    assertEquals(CONFIG_OF_AUDIT_MAX_PAYLOAD_SIZE, "pinot.audit.max.payload.size");
    assertEquals(CONFIG_OF_AUDIT_LOGGER_NAME, "pinot.audit.logger.name");
    assertEquals(CONFIG_OF_AUDIT_EXCLUDED_ENDPOINTS, "pinot.audit.excluded.endpoints");
  }

  @Test
  public void testInvalidConfigurationHandling() {
    // Test that invalid configuration values cause Jackson conversion to fail
    Map<String, String> clusterConfigs = new HashMap<>();
    clusterConfigs.put(CONFIG_OF_AUDIT_ENABLED, "invalid_boolean");
    clusterConfigs.put(CONFIG_OF_AUDIT_MAX_PAYLOAD_SIZE, "not_a_number");

    // Build config - Jackson should throw exception for invalid numeric values
    try {
      AuditConfig config = buildConfigFromCluster(clusterConfigs);
      fail("Expected Jackson to throw exception for invalid numeric value");
    } catch (IllegalArgumentException e) {
      // Expected - Jackson conversion should fail for invalid numeric values
      assertTrue(e.getMessage().contains("Cannot deserialize"));
    }
  }

  private AuditConfig buildConfigFromCluster(Map<String, String> clusterConfigs) {
    return AuditConfigChangeListener.buildConfigFromCluster(clusterConfigs);
  }
}
