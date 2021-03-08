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
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.HostAddress;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class TestbedDataFragment
{
    private static final JsonCodec<TestbedDataFragment> MEMORY_DATA_FRAGMENT_CODEC = jsonCodec(TestbedDataFragment.class);

    private final HostAddress hostAddress;
    private final long rows;

    @JsonCreator
    public TestbedDataFragment(
            @JsonProperty("hostAddress") HostAddress hostAddress,
            @JsonProperty("rows") long rows)
    {
        this.hostAddress = requireNonNull(hostAddress, "hostAddress is null");
        checkArgument(rows >= 0, "Rows number cannot be negative");
        this.rows = rows;
    }

    @JsonProperty
    public HostAddress getHostAddress()
    {
        return hostAddress;
    }

    @JsonProperty
    public long getRows()
    {
        return rows;
    }

    public Slice toSlice()
    {
        return Slices.wrappedBuffer(MEMORY_DATA_FRAGMENT_CODEC.toJsonBytes(this));
    }

    public static TestbedDataFragment fromSlice(Slice fragment)
    {
        return MEMORY_DATA_FRAGMENT_CODEC.fromJson(fragment.getBytes());
    }

    public static TestbedDataFragment merge(TestbedDataFragment a, TestbedDataFragment b)
    {
        checkArgument(a.getHostAddress().equals(b.getHostAddress()), "Cannot merge fragments from different hosts");
        return new TestbedDataFragment(a.getHostAddress(), a.getRows() + b.getRows());
    }
}
