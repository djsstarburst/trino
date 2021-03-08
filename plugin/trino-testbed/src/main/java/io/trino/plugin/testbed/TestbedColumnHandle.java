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
import io.trino.spi.connector.ColumnHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class TestbedColumnHandle
        implements ColumnHandle
{
    public static TestbedColumnHandle ROW_ID_HANDLE = new TestbedColumnHandle(-1, ColumnKind.ROWID);
    private final int columnIndex;
    private final ColumnKind columnKind;

    @JsonCreator
    public TestbedColumnHandle(
            @JsonProperty("columnIndex") int columnIndex,
            @JsonProperty("columnKind") ColumnKind columnKind)
    {
        this.columnIndex = columnIndex;
        this.columnKind = requireNonNull(columnKind, "columnKind is null");
    }

    public TestbedColumnHandle(int columnIndex)
    {
        this(columnIndex, ColumnKind.REGULAR);
    }

    @JsonProperty
    public int getColumnIndex()
    {
        return columnIndex;
    }

    @JsonProperty
    public ColumnKind getColumnKind()
    {
        return columnKind;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columnIndex);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TestbedColumnHandle other = (TestbedColumnHandle) obj;
        return Objects.equals(this.columnIndex, other.columnIndex);
    }

    @Override
    public String toString()
    {
        return Integer.toString(columnIndex);
    }

    public enum ColumnKind
    {
        REGULAR,
        ROWID,
    }
}
