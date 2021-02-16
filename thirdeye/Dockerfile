#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

FROM adoptopenjdk/openjdk8:alpine

ENV TE_HOME=/opt/thirdeye

COPY bin ${TE_HOME}/bin
COPY config ${TE_HOME}/config
COPY thirdeye-pinot/target/thirdeye-pinot-*-SNAPSHOT.jar ${TE_HOME}/bin/thirdeye-pinot.jar

RUN addgroup -S thirdeye && \
  adduser -S thirdeye -G thirdeye && \
  chown -R thirdeye:thirdeye ${TE_HOME}

EXPOSE 1426 1427
WORKDIR ${TE_HOME}

ENTRYPOINT ["./bin/start-thirdeye.sh"]
