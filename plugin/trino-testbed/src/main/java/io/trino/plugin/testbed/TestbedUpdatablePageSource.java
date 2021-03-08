package io.trino.plugin.testbed;

import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.UpdatablePageSource;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class TestbedUpdatablePageSource
        implements UpdatablePageSource
{
    private final ConnectorPageSource delegate;
    private volatile boolean closed;

    public TestbedUpdatablePageSource(ConnectorPageSource delegate)
    {
        this.delegate = delegate;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return false;
    }

    @Override
    public Page getNextPage()
    {
        Page page = delegate.getNextPage();
        if (page == null) {
            close();
            return null;
        }
//        if (transaction.isUpdate()) {
//            HiveUpdateProcessor updateProcessor = transaction.getUpdateProcessor().orElseThrow(() -> new IllegalArgumentException("updateProcessor not present"));
//            List<Integer> channels = dependencyChannels.orElseThrow(() -> new IllegalArgumentException("dependencyChannels not present"));
//            return updateProcessor.removeNonDependencyColumns(page, channels);
//        }
        return page;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            delegate.close();
        }
        catch (Exception e) {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, e);
        }
    }

    @Override
    public void deleteRows(Block rowIds)
    {

    }

    @Override
    public void updateRows(Page page, List<Integer> columnValueAndRowIdChannels)
    {

    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return null;
    }
}
