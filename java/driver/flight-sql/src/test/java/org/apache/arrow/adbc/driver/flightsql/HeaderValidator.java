/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.adbc.driver.flightsql;

import java.util.ArrayList;
import org.apache.arrow.flight.CallHeaders;
import org.apache.arrow.flight.CallInfo;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightServerMiddleware;
import org.apache.arrow.flight.RequestContext;

public class HeaderValidator implements FlightServerMiddleware {
  public static final Key<HeaderValidator> KEY = Key.of("HeaderValidator");

  @Override
  public void onBeforeSendingHeaders(CallHeaders callHeaders) {}

  @Override
  public void onCallCompleted(CallStatus callStatus) {}

  @Override
  public void onCallErrored(Throwable throwable) {}

  public static class Factory implements FlightServerMiddleware.Factory<HeaderValidator> {

    private final ArrayList<CallHeaders> headersReceived = new ArrayList<>();

    @Override
    public HeaderValidator onCallStarted(
        CallInfo callInfo, CallHeaders callHeaders, RequestContext requestContext) {
      CallHeaders cloneHeaders = cloneHeaders(callHeaders);
      headersReceived.add(cloneHeaders);
      return new HeaderValidator();
    }

    public CallHeaders getHeadersReceivedAtRequest(int request) {
      return cloneHeaders(headersReceived.get(request));
    }

    private static CallHeaders cloneHeaders(CallHeaders headers) {
      FlightCallHeaders cloneHeaders = new FlightCallHeaders();
      for (String key : headers.keys()) {
        for (String value : headers.getAll(key)) {
          cloneHeaders.insert(key, value);
        }
      }
      return cloneHeaders;
    }
  }
}
