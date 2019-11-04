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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spark.spi.Configuration;
import com.facebook.presto.spark.spi.Service;
import com.facebook.presto.spark.spi.ServiceFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;

public class PrestoSparkServiceFactory
        implements ServiceFactory
{
    private final Logger log = Logger.get(PrestoSparkServiceFactory.class);

    @Override
    public Service createService(Configuration configuration)
    {
        System.setProperty("config", configuration.getConfigFilePath());

        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put("plugin.dir", configuration.getPluginsDirectoryPath());
        properties.put("plugin.config-dir", configuration.getPluginsConfigDirectoryPath());
        properties.putAll(configuration.getExtraProperties());

        PrestoSparkInjectorFactory prestoSparkInjectorFactory = new PrestoSparkInjectorFactory(properties.build(), ImmutableList.of());

        Injector injector = prestoSparkInjectorFactory.create();
        PrestoSparkService prestoSparkService = injector.getInstance(PrestoSparkService.class);
        log.info("Initialized");
        return prestoSparkService;
    }
}
