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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.resourcegroups.QueryType;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public final class TestbedTableHandle
        implements ConnectorTableHandle
{
    private final long id;
    private final OptionalLong limit;
    private final OptionalDouble sampleRatio;
    private final Optional<QueryType> queryType;

    public TestbedTableHandle(long id)
    {
        this(id, OptionalLong.empty(), OptionalDouble.empty(), Optional.empty());
    }

    @JsonCreator
    public TestbedTableHandle(
            @JsonProperty("id") long id,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sampleRatio") OptionalDouble sampleRatio,
            @JsonProperty("queryType") Optional<QueryType> queryType)
    {
        this.id = id;
        this.limit = requireNonNull(limit, "limit is null");
        this.sampleRatio = requireNonNull(sampleRatio, "sampleRatio is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
    }

    @JsonProperty
    public long getId()
    {
        return id;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    @JsonProperty
    public OptionalDouble getSampleRatio()
    {
        return sampleRatio;
    }

    @JsonProperty
    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    TestbedTableHandle withQueryType(QueryType queryType)
    {
        requireNonNull(queryType, "queryType is null");
        return new TestbedTableHandle(id, limit, sampleRatio, Optional.of(queryType));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TestbedTableHandle that = (TestbedTableHandle) o;
        return id == that.id &&
                limit.equals(that.limit) &&
                sampleRatio.equals(that.sampleRatio);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, limit, sampleRatio);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(id);
        limit.ifPresent(value -> builder.append("(limit:" + value + ")"));
        sampleRatio.ifPresent(value -> builder.append("(sampleRatio:" + value + ")"));
        return builder.toString();
    }
}
