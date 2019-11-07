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

import com.facebook.airlift.bootstrap.Bootstrap;
import com.facebook.airlift.json.JsonModule;
import com.facebook.presto.eventlistener.EventListenerManager;
import com.facebook.presto.eventlistener.EventListenerModule;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.metadata.StaticCatalogStore;
import com.facebook.presto.metadata.StaticFunctionNamespaceStore;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.security.AccessControlModule;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.server.SessionPropertyDefaults;
import com.facebook.presto.server.security.PasswordAuthenticatorManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.server.PrestoSystemRequirements.verifySystemTimeIsReasonable;
import static java.util.Objects.requireNonNull;

public class PrestoSparkInjectorFactory
{
    private final Map<String, String> properties;
    private final List<Module> additionalModules;

    public PrestoSparkInjectorFactory(Map<String, String> properties, List<Module> additionalModules)
    {
        this.properties = ImmutableMap.copyOf(requireNonNull(properties, "properties is null"));
        this.additionalModules = ImmutableList.copyOf(requireNonNull(additionalModules, "additionalModules is null"));
    }

    public Injector create()
    {
//        verifyJvmRequirements();
        verifySystemTimeIsReasonable();

        ImmutableList.Builder<Module> modules = ImmutableList.builder();
        modules.add(
                new AccessControlModule(),
                new JsonModule(),
                new EventListenerModule(),
                new PrestoSparkModule());

        modules.addAll(additionalModules);

        Bootstrap app = new Bootstrap(modules.build());

        // Stream redirect doesn't work well with spark logging
        app.doNotInitializeLogging();

        app.setRequiredConfigurationProperties(properties);

        Injector injector = app.strictConfig().initialize();

        try {
            injector.getInstance(PluginManager.class).loadPlugins();
            injector.getInstance(StaticCatalogStore.class).loadCatalogs();
            injector.getInstance(StaticFunctionNamespaceStore.class).loadFunctionNamespaceManagers();
            injector.getInstance(SessionPropertyDefaults.class).loadConfigurationManager();
            injector.getInstance(ResourceGroupManager.class).loadConfigurationManager();
            injector.getInstance(AccessControlManager.class).loadSystemAccessControl();
            injector.getInstance(PasswordAuthenticatorManager.class).loadPasswordAuthenticator();
            injector.getInstance(EventListenerManager.class).loadConfiguredEventListener();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return injector;
    }
}
