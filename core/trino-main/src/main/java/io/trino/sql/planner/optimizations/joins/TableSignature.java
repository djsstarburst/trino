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
package io.trino.sql.planner.optimizations.joins;

import com.google.common.collect.ImmutableSet;
import io.trino.metadata.TableHandle;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.sql.planner.optimizations.joins.TableSignature.SignatureState.EMPTY;
import static io.trino.sql.planner.optimizations.joins.TableSignature.SignatureState.HAS_GROUP_BY;
import static io.trino.sql.planner.optimizations.joins.TableSignature.SignatureState.NO_GROUP_BY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class TableSignature
{
    public static final TableSignature EMPTY_SIGNATURE = new TableSignature(EMPTY, ImmutableSet.of(), "EMPTY");

    private final SignatureState signatureState;
    private final Set<TableHandle> tableHandles;

    // nodeDescription is not used to determine TableSignature equality - -
    // it is only used to identify signatures during testing, and will likely
    // be removed.
    private final String nodeDescription;

    public TableSignature(SignatureState signatureState, Set<TableHandle> tableHandles, String nodeDescription)
    {
        this.signatureState = requireNonNull(signatureState, "signatureState is null");
        this.tableHandles = requireNonNull(tableHandles, "tableHandles is null");
        this.nodeDescription = requireNonNull(nodeDescription, "nodeDescription is null");
    }

    public SignatureState getSignatureState()
    {
        return signatureState;
    }

    public Set<TableHandle> getTableHandles()
    {
        return tableHandles;
    }

    public String getNodeDescription()
    {
        return nodeDescription;
    }

    public static TableSignature union(String description, List<TableSignature> sourceSignatures)
    {
        if (sourceSignatures.stream().anyMatch(element -> element.getSignatureState() != NO_GROUP_BY)) {
            return EMPTY_SIGNATURE;
        }
        else {
            Set<TableHandle> unionSet = sourceSignatures.stream()
                    .map(TableSignature::getTableHandles)
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet());
            return new TableSignature(NO_GROUP_BY, unionSet, description);
        }
    }

    public TableSignature withDescription(String description)
    {
        return new TableSignature(signatureState, tableHandles, description);
    }

    @Override
    public String toString()
    {
        if (signatureState == EMPTY) {
            return format("%s[EMPTY]", nodeDescription);
        }
        else {
            String tablesString = tableHandles.stream()
                    .map(handle -> handle.getConnectorHandle().toString())
                    .collect(joining(","));
            return format("%s[%s; %s]",
                    nodeDescription,
                    signatureState == HAS_GROUP_BY ? "T" : "F",
                    tablesString);
        }
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
        TableSignature signature = (TableSignature) o;
        return signatureState == signature.signatureState && tableHandles.equals(signature.tableHandles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(signatureState, tableHandles);
    }

    public enum SignatureState
    {
        EMPTY,
        HAS_GROUP_BY,
        NO_GROUP_BY,
    }
}
