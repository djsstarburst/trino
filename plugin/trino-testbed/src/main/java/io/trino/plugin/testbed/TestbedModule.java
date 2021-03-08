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
package io.trino.plugin.testbed;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.trino.spi.NodeManager;
import io.trino.spi.type.TypeManager;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class TestbedModule
        implements Module
{
    private final TypeManager typeManager;
    private final NodeManager nodeManager;

    public TestbedModule(TypeManager typeManager, NodeManager nodeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(TypeManager.class).toInstance(typeManager);
        binder.bind(NodeManager.class).toInstance(nodeManager);

        binder.bind(TestbedConnector.class).in(Scopes.SINGLETON);
        binder.bind(TestbedMetadata.class).in(Scopes.SINGLETON);
        binder.bind(TestbedSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(TestbedPagesStore.class).in(Scopes.SINGLETON);
        binder.bind(TestbedPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(TestbedPageSinkProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(TestbedConfig.class);
    }
}
