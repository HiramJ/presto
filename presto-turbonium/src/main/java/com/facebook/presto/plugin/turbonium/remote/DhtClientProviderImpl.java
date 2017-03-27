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
package com.facebook.presto.plugin.turbonium.remote;

import com.facebook.presto.plugin.turbonium.TurboniumConfig;
import com.facebook.swift.fbcode.smc.Service;
import com.facebook.swift.fbcode.smc.ServiceException;
import com.facebook.swift.fbcode.smc.Tier;
import com.facebook.swift.fbcode.smc2.Smc2;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.stream.Collectors;

public class DhtClientProviderImpl
        implements DhtClientProvider
{
    private static final Logger log = Logger.get(DhtClientProviderImpl.class);
    private final Provider<Smc2> smcClientProvider;
    private final String dhtTierName;
    private final List<DhtClient> clients;

    @Inject
    public DhtClientProviderImpl(Provider<Smc2> smcClientProvider, TurboniumConfig config)
    {
        this.smcClientProvider = smcClientProvider;
        this.dhtTierName = config.getDhtTier();
        this.clients = getClients();
    }

    @Override
    public DhtClient getClient(int hashId)
    {
        return clients.get(hashId % clients.size());
    }

    private List<DhtClient> getClients()
    {
        Smc2 smc = smcClientProvider.get();
        try {
            Tier tier = smc.getTierByName(dhtTierName);
            List<Service> services = tier.getServices();
            List<DhtClient> clients = services.stream().map(s -> {
                InetSocketAddress address = new InetSocketAddress(s.getHostname(), s.getPort());
                log.info("Added Dht server: %s:%s" + address.getHostName() + address.getPort());
                return new DhtClientImpl(address);
            }).collect(Collectors.toList());

            return clients;
        }
        catch (ServiceException e) {
            log.error(e, "Failed to get services from smc tier: %s", dhtTierName);

            throw new IllegalArgumentException(e);
        }
    }
}
