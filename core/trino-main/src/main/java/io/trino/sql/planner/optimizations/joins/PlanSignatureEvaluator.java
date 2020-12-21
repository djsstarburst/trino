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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.ExchangeNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.JoinNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ProjectNode;
import io.trino.sql.planner.plan.TableScanNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.planner.optimizations.joins.TableSignature.EMPTY_SIGNATURE;
import static io.trino.sql.planner.optimizations.joins.TableSignature.SignatureState.EMPTY;
import static io.trino.sql.planner.optimizations.joins.TableSignature.SignatureState.NO_GROUP_BY;
import static java.util.Objects.requireNonNull;

/**
 * This class generates and provides access to a map from {@link  PlanNodeId} to
 * a {@link TableSignature}.  TableSignatures bubble up from TableScans.
 */
public class PlanSignatureEvaluator
{
    private final Map<PlanNodeId, NodeSignature> nodeSignatures;

    public PlanSignatureEvaluator(List<PlanNode> plans)
    {
        requireNonNull(plans, "plans is null");
        Map<PlanNodeId, NodeSignature> signatures = new HashMap<>();
        plans.forEach(node -> generateSignature(node, signatures));
        this.nodeSignatures = ImmutableMap.copyOf(signatures);
    }

    private static TableSignature generateSignature(PlanNode node, Map<PlanNodeId, NodeSignature> signatures)
    {
        requireNonNull(node, "node is null");
        requireNonNull(signatures, "signatures is null");

        TableSignature signature = null;
        if (node instanceof TableScanNode) {
            signature = new TableSignature(NO_GROUP_BY, ImmutableSet.of(((TableScanNode) node).getTable()), "scan");
        }
        else if (node instanceof ProjectNode) {
            signature = generateSignature(((ProjectNode) node).getSource(), signatures).withDescription("project");
        }
        else if (node instanceof ExchangeNode) {
            ExchangeNode exchangeNode = (ExchangeNode) node;
            signature = TableSignature.union(
                    "exchange",
                    exchangeNode.getSources().stream()
                            .map(source -> generateSignature(source, signatures))
                            .collect(toImmutableList()));
        }
        else if (node instanceof AggregationNode) {
            signature = generateSignature(((AggregationNode) node).getSource(), signatures).withDescription("agg");
        }
        else if (node instanceof FilterNode) {
            signature = generateSignature(((FilterNode) node).getSource(), signatures).withDescription("filter");
        }
        else if (node instanceof JoinNode) {
            signature = TableSignature.union("join", ImmutableList.of(
                    generateSignature(((JoinNode) node).getLeft(), signatures),
                    generateSignature(((JoinNode) node).getRight(), signatures)));
        }

        if (signature != null) {
            if (signature.getSignatureState() != EMPTY) {
                signatures.put(node.getId(), new NodeSignature(node, signature));
            }
            return signature;
        }
        else {
            int index = 0;
            TableSignature first = EMPTY_SIGNATURE;
            for (PlanNode child : node.getSources()) {
                signature = generateSignature(child, signatures);
                if (index == 0) {
                    first = signature;
                }
                index++;
            }
            return first;
        }
    }

    public Optional<NodeSignature> getTableSignature(PlanNodeId id)
    {
        return Optional.ofNullable(nodeSignatures.get(id));
    }

    public Map<TableSignature, List<NodeSignature>> getSignaturesPlanNodeIdsForDescription(Optional<String> description)
    {
        Map<TableSignature, List<NodeSignature>> signatureNodes = new HashMap<>();
        nodeSignatures.forEach((id, nodeSignature) -> {
            if (description.isEmpty() || nodeSignature.getSignature().getNodeDescription().equals(description.get())) {
                List<NodeSignature> list = signatureNodes.computeIfAbsent(nodeSignature.getSignature(), sig -> new ArrayList<>());
                list.add(nodeSignature);
            }
        });
        return signatureNodes;
    }

    public static class NodeSignature
    {
        private final PlanNode node;
        private final TableSignature signature;

        public NodeSignature(PlanNode node, TableSignature signature)
        {
            this.node = node;
            this.signature = signature;
        }

        public PlanNode getNode()
        {
            return node;
        }

        public TableSignature getSignature()
        {
            return signature;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("signature", signature)
                    .toString();
        }
    }
}
