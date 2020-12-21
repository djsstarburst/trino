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
import io.trino.metadata.TableHandle;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.optimizations.joins.PlanSignatureEvaluator.NodeSignature;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.AggregationNode.Aggregation;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.sql.planner.plan.PlanVisitor;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CommonSubplanExtractor
{
    public CommonSubplanExtractor()
    {
    }

    private static class AggregationRollupVisitor
            extends PlanVisitor<ExtractContext, ExtractContext>
    {
        @Override
        protected ExtractContext visitPlan(PlanNode node, ExtractContext context)
        {
            if (node instanceof AggregationNode) {
                return visitAggregation((AggregationNode) node, context);
            }
            if (node instanceof FilterNode) {
                return visitFilter((FilterNode) node, context);
            }
            return visitChildren(node, context);
        }

        @Override
        public ExtractContext visitAggregation(AggregationNode node, ExtractContext context)
        {
            ExtractContext childContext = visitChildren(node, context);
            return childContext.withAddedAggregation(node, node.getAggregations());
        }

        @Override
        public ExtractContext visitFilter(FilterNode node, ExtractContext context)
        {
            ExtractContext childContext = visitChildren(node, context);
            return childContext.withAddedPredicate(node);
        }

        private ExtractContext visitChildren(PlanNode node, ExtractContext context)
        {
            ExtractContext updatedContext = context;
            for (PlanNode childNode : node.getSources()) {
                updatedContext = visitPlan(childNode, updatedContext);
            }
            return updatedContext;
        }
    }

    public List<ExtractedSubplan> getSubplansForSignature(TableHandle handle, List<NodeSignature> nodeSignatures)
    {
        return nodeSignatures.stream()
                .map(signature -> extractedSubplan(handle, signature))
                .collect(toImmutableList());
    }

    private ExtractedSubplan extractedSubplan(TableHandle handle, NodeSignature signature)
    {
        ExtractContext context = new AggregationRollupVisitor().visitPlan(signature.getNode(), new ExtractContext());
        return new ExtractedSubplan(signature.getNode(), context.getMissingPredicates(), context.getMissingAggregations());
    }

    private static class ExtractContext
    {
        List<PlanNode> missingPredicates;
        List<PlanAggregation> missingAggregations;

        public ExtractContext()
        {
            this(ImmutableList.of(), ImmutableList.of());
        }

        public ExtractContext(List<PlanNode> missingPredicates, List<PlanAggregation> missingAggregations)
        {
            this.missingPredicates = requireNonNull(missingPredicates, "missingPredicates is null");
            this.missingAggregations = requireNonNull(missingAggregations, "missingAggregations is null");
        }

        public List<PlanNode> getMissingPredicates()
        {
            return missingPredicates;
        }

        public List<PlanAggregation> getMissingAggregations()
        {
            return missingAggregations;
        }

        public ExtractContext withAddedPredicate(PlanNode predicate)
        {
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builderWithExpectedSize(missingPredicates.size() + 1);
            builder.addAll(missingPredicates);
            builder.add(predicate);
            return new ExtractContext(builder.build(), missingAggregations);
        }

        public ExtractContext withAddedAggregation(AggregationNode aggregationNode, Map<Symbol, Aggregation> aggregate)
        {
            ImmutableList.Builder<PlanAggregation> builder = ImmutableList.builderWithExpectedSize(missingAggregations.size() + 1);
            builder.addAll(missingAggregations);
            builder.add(new PlanAggregation(aggregationNode, aggregate));
            return new ExtractContext(missingPredicates, builder.build());
        }
    }

    public static class ExtractedSubplan
    {
        PlanNode originalNode;
        List<PlanNode> missingPredicates;
        List<PlanAggregation> missingAggregations;

        public ExtractedSubplan(PlanNode originalNode, List<PlanNode> missingPredicates, List<PlanAggregation> missingAggregations)
        {
            this.originalNode = originalNode;
            this.missingPredicates = missingPredicates;
            this.missingAggregations = missingAggregations;
        }

        public PlanNode getOriginalNode()
        {
            return originalNode;
        }

        public List<PlanNode> getMissingPredicates()
        {
            return missingPredicates;
        }

        public List<PlanAggregation> getMissingAggregations()
        {
            return missingAggregations;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("missingPredicates", missingPredicates)
                    .add("missingAggregations", missingAggregations)
                    .toString();
        }
    }

    public static class PlanAggregation
    {
        private final AggregationNode aggregationNode;
        private final Map<Symbol, Aggregation> aggregation;

        public PlanAggregation(AggregationNode aggregationNode, Map<Symbol, Aggregation> aggregation)
        {
            this.aggregationNode = aggregationNode;
            this.aggregation = aggregation;
        }

        public AggregationNode getAggregationNode()
        {
            return aggregationNode;
        }

        public Map<Symbol, Aggregation> getAggregation()
        {
            return aggregation;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("aggregationNode", aggregationNode)
                    .add("aggregation", aggregation)
                    .toString();
        }
    }
}
