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
package org.apache.pinot.controller.audit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.common.audit.AuditLogger;
import org.glassfish.grizzly.http.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Jersey filter for audit logging of Controller API requests.
 * Captures request metadata and payload for all Controller REST endpoints
 * as specified in Phase 1 audit logging requirements.
 */
@javax.ws.rs.ext.Provider
public class ControllerAuditFilter implements ContainerRequestFilter {

  public static final String ANONYMOUS = "anonymous";
  private static final Logger LOG = LoggerFactory.getLogger(ControllerAuditFilter.class);
  // TODO spyne remove Hard-coded service identifier
  private static final String SERVICE_ID = "Query Console";

  private static final AU

  @Inject
  Provider<Request> _requestProvider;

  @Context
  HttpHeaders _httpHeaders;



  @Override
  public void filter(ContainerRequestContext requestContext)
      throws IOException {
    // Skip audit logging if it's not enabled to avoid unnecessary processing
    if (!AuditLogger.isEnabled()) {
      return;
    }

    new
    try {
      // Extract request metadata
      Request grizzlyRequest = _requestProvider.get();
      UriInfo uriInfo = requestContext.getUriInfo();

      String endpoint = uriInfo.getPath();
      String method = requestContext.getMethod();
      String originIpAddress = extractClientIpAddress(_httpHeaders, grizzlyRequest.getRemoteAddr());
      String userId = extractUserId(_httpHeaders);

      // Capture request payload (hard-coded full capture for Phase 1)
      Object requestPayload = captureRequestPayload(requestContext);

      // Log the audit event
      AuditLogger.log(SERVICE_ID, endpoint, method, originIpAddress, userId, requestPayload);
    } catch (Exception e) {
      // Graceful degradation: Never let audit logging failures affect the main request
      LOG.warn("Failed to process audit logging for request", e);
    }
  }

  /**
   * Extracts the client IP address from the request.
   * Checks common proxy headers before falling back to remote address.
   */
  private String extractClientIpAddress(HttpHeaders headers, String remoteAddr) {
    try {
      // Check for proxy headers first
      String xForwardedFor = headers.getHeaderString("X-Forwarded-For");
      if (StringUtils.isNotBlank(xForwardedFor)) {
        // X-Forwarded-For can contain multiple IPs, take the first one
        return xForwardedFor.split(",")[0].trim();
      }

      String xRealIp = headers.getHeaderString("X-Real-IP");
      if (StringUtils.isNotBlank(xRealIp)) {
        return xRealIp.trim();
      }

      // Fall back to remote address
      return remoteAddr;
    } catch (Exception e) {
      LOG.debug("Failed to extract client IP address", e);
      return "unknown";
    }
  }

  /**
   * Extracts user ID from request headers.
   * Looks for common authentication headers.
   */
  private String extractUserId(HttpHeaders headers) {
    try {
      // Check for common user identification headers
      String authHeader = headers.getHeaderString("Authorization");
      if (StringUtils.isNotBlank(authHeader)) {
        // For basic auth, extract username; for bearer tokens, use a placeholder
        if (authHeader.startsWith("Basic ")) {
          // Could decode basic auth to get username, but for security keep it as placeholder
          return "basic-auth-user";
        } else if (authHeader.startsWith("Bearer ")) {
          return "bearer-token-user";
        }
      }

      return ANONYMOUS;
    } catch (Exception e) {
      LOG.debug("Failed to extract user ID", e);
      return ANONYMOUS;
    }
  }

  /**
   * Captures the complete request payload for audit logging.
   * Phase 1 hard-coded to capture full payload for ALL requests.
   */
  private Object captureRequestPayload(ContainerRequestContext requestContext) {
    Map<String, Object> payload = new HashMap<>();

    try {
      // Capture query parameters
      UriInfo uriInfo = requestContext.getUriInfo();
      MultivaluedMap<String, String> queryParams = uriInfo.getQueryParameters();
      if (!queryParams.isEmpty()) {
        Map<String, Object> queryMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : queryParams.entrySet()) {
          List<String> values = entry.getValue();
          if (values.size() == 1) {
            queryMap.put(entry.getKey(), values.get(0));
          } else {
            queryMap.put(entry.getKey(), values);
          }
        }
        payload.put("queryParameters", queryMap);
      }

      // Capture request body for POST/PUT requests
      if (requestContext.hasEntity()) {
        String requestBody = readRequestBody(requestContext);
        if (StringUtils.isNotBlank(requestBody)) {
          payload.put("body", requestBody);
        }
      }

      // Capture relevant headers (excluding sensitive ones)
      MultivaluedMap<String, String> headers = requestContext.getHeaders();
      if (!headers.isEmpty()) {
        Map<String, String> headerMap = new HashMap<>();
        for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
          String headerName = entry.getKey().toLowerCase();
          // Skip sensitive headers
          if (!headerName.contains("auth") && !headerName.contains("password") && !headerName.contains("token")
              && !headerName.contains("secret")) {
            List<String> values = entry.getValue();
            if (!values.isEmpty()) {
              headerMap.put(entry.getKey(), values.get(0));
            }
          }
        }
        if (!headerMap.isEmpty()) {
          payload.put("headers", headerMap);
        }
      }
    } catch (Exception e) {
      LOG.debug("Failed to capture request payload", e);
      payload.put("error", "Failed to capture payload: " + e.getMessage());
    }

    return payload.isEmpty() ? null : payload;
  }

  /**
   * Reads the request body from the entity input stream.
   * Restores the input stream for downstream processing.
   */
  private String readRequestBody(ContainerRequestContext requestContext) {
    try {
      InputStream entityStream = requestContext.getEntityStream();
      if (entityStream == null) {
        return null;
      }

      // Read the stream content
      ByteArrayOutputStream buffer = new ByteArrayOutputStream();
      byte[] data = new byte[8192];
      int bytesRead;
      while ((bytesRead = entityStream.read(data, 0, data.length)) != -1) {
        buffer.write(data, 0, bytesRead);
      }

      byte[] requestBodyBytes = buffer.toByteArray();
      String requestBody = new String(requestBodyBytes, StandardCharsets.UTF_8);

      // Restore the input stream for downstream processing
      requestContext.setEntityStream(new java.io.ByteArrayInputStream(requestBodyBytes));

      return requestBody;
    } catch (IOException e) {
      LOG.debug("Failed to read request body", e);
      return "Failed to read request body: " + e.getMessage();
    }
  }
}