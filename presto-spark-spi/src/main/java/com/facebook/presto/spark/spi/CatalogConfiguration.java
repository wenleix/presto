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
package com.facebook.presto.spark.spi;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public class CatalogConfiguration
        implements Serializable
{
    private final String pluginName;
    private final String catalogName;
    private final Map<String, String> catalogConfiguration;

    public CatalogConfiguration(String pluginName, String catalogName, Map<String, String> catalogConfiguration)
    {
        this.pluginName = pluginName;
        this.catalogName = catalogName;
        this.catalogConfiguration = unmodifiableMap(new HashMap<>(catalogConfiguration));
    }

    public String getPluginName()
    {
        return pluginName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }

    public Map<String, String> getCatalogConfiguration()
    {
        return catalogConfiguration;
    }
}
