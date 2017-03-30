/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.dht.server;

import com.facebook.presto.dht.common.Request;
import com.facebook.presto.dht.common.Response;
import io.airlift.log.Logger;

import java.util.Optional;

public abstract class AbstractDhtProcessor
        implements DhtRequestProcessor
{
    private static final Logger log = Logger.get(AbstractDhtProcessor.class);

    @Override
    public Optional<Response> process(Request request)
    {
        byte[] key = request.getKey();
        byte[] value = request.getValue();
        byte[] payload = getResponse(key, value);

        log.info("Got request of id: %d", request.getId());

        if (sendResponse()) {
            return Optional.of(new Response(request.getId(), payload));
        }
        else {
            return Optional.empty();
        }
    }

    protected abstract byte[] getResponse(byte[] key, byte[] value);

    protected abstract boolean sendResponse();
}
