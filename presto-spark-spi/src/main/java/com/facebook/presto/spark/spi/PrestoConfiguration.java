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
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;

public class PrestoConfiguration
        implements Serializable
{
    private final List<String> plugins;
    private final List<CatalogConfiguration> catalogs;

    public PrestoConfiguration(List<String> plugins, List<CatalogConfiguration> catalogs)
    {
        this.plugins = unmodifiableList(new ArrayList<>(plugins));
        this.catalogs = unmodifiableList(new ArrayList<>(catalogs));
    }

    public List<String> getPlugins()
    {
        return plugins;
    }

    public List<CatalogConfiguration> getCatalogs()
    {
        return catalogs;
    }
}
