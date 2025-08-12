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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

import static org.apache.pinot.spi.utils.CommonConstants.Server.*;


/**
 * Utility class for validating audit configuration values.
 */
public final class AuditConfigValidator {

  private AuditConfigValidator() {
    // Utility class - prevent instantiation
  }

  /**
   * Validates audit configuration values from cluster config.
   *
   * @param configs the cluster configuration map
   * @return validation result containing any errors found
   */
  public static ValidationResult validate(Map<String, String> configs) {
    List<String> errors = new ArrayList<>();

    // Validate boolean values
    validateBoolean(configs, CONFIG_OF_AUDIT_ENABLED, errors);
    validateBoolean(configs, CONFIG_OF_AUDIT_CAPTURE_REQUEST_PAYLOAD, errors);
    validateBoolean(configs, CONFIG_OF_AUDIT_CAPTURE_REQUEST_HEADERS, errors);

    // Validate integer values
    validateIntegerRange(configs, CONFIG_OF_AUDIT_MAX_PAYLOAD_SIZE, 0, 1048576, errors); // 0 to 1MB

    // Validate string values
    validateLoggerName(configs, CONFIG_OF_AUDIT_LOGGER_NAME, errors);

    return new ValidationResult(errors);
  }

  private static void validateBoolean(Map<String, String> configs, String key, List<String> errors) {
    String value = configs.get(key);
    if (value != null && !isBooleanValue(value)) {
      errors.add("Invalid boolean value for '" + key + "': '" + value + "'. Expected 'true' or 'false'.");
    }
  }

  private static void validateIntegerRange(Map<String, String> configs, String key, int min, int max,
      List<String> errors) {
    String value = configs.get(key);
    if (value != null) {
      try {
        int intValue = Integer.parseInt(value);
        if (intValue < min || intValue > max) {
          errors.add("Value for '" + key + "' is out of range [" + min + ", " + max + "]: " + intValue);
        }
      } catch (NumberFormatException e) {
        errors.add("Invalid integer value for '" + key + "': '" + value + "'");
      }
    }
  }

  private static void validateLoggerName(Map<String, String> configs, String key, List<String> errors) {
    String value = configs.get(key);
    if (value != null) {
      if (StringUtils.isBlank(value)) {
        errors.add("Logger name cannot be empty for '" + key + "'");
      } else if (value.contains(" ")) {
        errors.add("Logger name cannot contain spaces for '" + key + "': '" + value + "'");
      }
    }
  }

  private static boolean isBooleanValue(String value) {
    return "true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value);
  }

  /**
   * Result of configuration validation.
   */
  public static final class ValidationResult {
    private final List<String> _errors;

    public ValidationResult(List<String> errors) {
      _errors = new ArrayList<>(errors);
    }

    /**
     * @return true if validation passed (no errors)
     */
    public boolean isValid() {
      return _errors.isEmpty();
    }

    /**
     * @return list of validation error messages
     */
    public List<String> getErrors() {
      return new ArrayList<>(_errors);
    }

    /**
     * @return formatted error message containing all validation errors
     */
    public String getErrorMessage() {
      if (_errors.isEmpty()) {
        return "";
      }
      return "Audit configuration validation failed: " + String.join("; ", _errors);
    }

    @Override
    public String toString() {
      return isValid() ? "ValidationResult{valid}" : "ValidationResult{errors=" + _errors + "}";
    }
  }
}