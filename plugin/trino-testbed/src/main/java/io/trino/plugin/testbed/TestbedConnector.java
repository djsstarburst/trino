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

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorPageSinkProvider;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;

import javax.inject.Inject;

public class TestbedConnector
        implements Connector
{
    private final TestbedMetadata metadata;
    private final TestbedSplitManager splitManager;
    private final TestbedPageSourceProvider pageSourceProvider;
    private final TestbedPageSinkProvider pageSinkProvider;

    @Inject
    public TestbedConnector(
            TestbedMetadata metadata,
            TestbedSplitManager splitManager,
            TestbedPageSourceProvider pageSourceProvider,
            TestbedPageSinkProvider pageSinkProvider)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.pageSourceProvider = pageSourceProvider;
        this.pageSinkProvider = pageSinkProvider;
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return TestbedTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return metadata;
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return pageSourceProvider;
    }

    @Override
    public ConnectorPageSinkProvider getPageSinkProvider()
    {
        return pageSinkProvider;
    }
}
