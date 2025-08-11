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

import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Provider;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import org.apache.pinot.common.audit.AuditEvent;
import org.apache.pinot.common.audit.AuditLogger;
import org.apache.pinot.common.audit.AuditRequestProcessor;
import org.glassfish.grizzly.http.server.Request;


/**
 * Jersey filter for audit logging of Controller API requests.
 * Delegates to JerseyRequestAuditor for all audit data extraction and logging.
 */
@javax.ws.rs.ext.Provider
public class ControllerAuditFilter implements ContainerRequestFilter {

  private static final AuditRequestProcessor AUDITOR = new AuditRequestProcessor();

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

    // Extract the remote address and delegate to the auditor
    final Request grizzlyRequest = _requestProvider.get();
    final String remoteAddr = grizzlyRequest.getRemoteAddr();

    final AuditEvent auditEvent = AUDITOR.processRequest(requestContext, _httpHeaders, remoteAddr);
    AuditLogger.log(auditEvent);
  }
}