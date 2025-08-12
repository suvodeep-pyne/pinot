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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Pure data class for audit logging configuration.
 * Uses Jackson annotations for automatic JSON mapping.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class AuditConfig {

  @JsonProperty("enabled")
  private boolean enabled = false;

  @JsonProperty("captureRequestPayload")
  private boolean captureRequestPayload = true;

  @JsonProperty("captureRequestHeaders")
  private boolean captureRequestHeaders = true;

  @JsonProperty("maxPayloadSize")
  private int maxPayloadSize = 10240;

  @JsonProperty("loggerName")
  private String loggerName = "audit";

  @JsonProperty("excludedEndpoints")
  private String excludedEndpoints = "";

  public AuditConfig() {
    // Default constructor for Jackson
  }

  public boolean isEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public boolean isCaptureRequestPayload() {
    return captureRequestPayload;
  }

  public void setCaptureRequestPayload(boolean captureRequestPayload) {
    this.captureRequestPayload = captureRequestPayload;
  }

  public boolean isCaptureRequestHeaders() {
    return captureRequestHeaders;
  }

  public void setCaptureRequestHeaders(boolean captureRequestHeaders) {
    this.captureRequestHeaders = captureRequestHeaders;
  }

  public int getMaxPayloadSize() {
    return maxPayloadSize;
  }

  public void setMaxPayloadSize(int maxPayloadSize) {
    this.maxPayloadSize = maxPayloadSize;
  }

  public String getLoggerName() {
    return loggerName;
  }

  public void setLoggerName(String loggerName) {
    this.loggerName = loggerName;
  }

  public String getExcludedEndpoints() {
    return excludedEndpoints;
  }

  public void setExcludedEndpoints(String excludedEndpoints) {
    this.excludedEndpoints = excludedEndpoints;
  }


  @Override
  public String toString() {
    return "AuditConfig{" + "enabled=" + enabled + ", captureRequestPayload=" + captureRequestPayload
        + ", captureRequestHeaders=" + captureRequestHeaders + ", maxPayloadSize=" + maxPayloadSize + ", loggerName='"
        + loggerName + '\'' + ", excludedEndpoints='" + excludedEndpoints + "'}";
  }
}