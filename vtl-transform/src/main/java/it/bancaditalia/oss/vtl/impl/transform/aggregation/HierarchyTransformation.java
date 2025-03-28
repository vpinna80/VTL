/*
 * Copyright © 2020 Banca D'Italia
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 */
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyInput.DATASET;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyInput.RULE;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_NULL;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_ZERO;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyOutput.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyOutput.COMPUTED;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.lang.Math.round;
import static java.util.Collections.emptyList;
import static java.util.Collections.min;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageExternal;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.Rule;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.Utils;

public class HierarchyTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(HierarchyTransformation.class);
	private static final Lineage LINEAGE_MISSING = LineageExternal.of("hierarchy-generated-missing");

	private final Transformation operand;
	private final VTLAlias rulesetID;
	private final List<VTLAlias> conditions;
	private final VTLAlias id;
	private final HierarchyMode mode;
	private final HierarchyInput input;
	private final HierarchyOutput output;
	
	public enum HierarchyMode
	{
		NON_NULL, NON_ZERO, PARTIAL_NULL, PARTIAL_ZERO, ALWAYS_NULL, ALWAYS_ZERO;
		
		public boolean isZero()
		{
			return this == NON_ZERO || this == PARTIAL_ZERO || this == ALWAYS_ZERO;
		}
		
		public boolean isPartial()
		{
			return this == PARTIAL_NULL || this == PARTIAL_ZERO;
		}
		
		public boolean isAlways()
		{
			return this == ALWAYS_NULL || this == ALWAYS_ZERO;
		}
	}
	
	public enum HierarchyInput
	{
		DATASET, RULE, RULE_PRIORITY;
	}
	
	public enum HierarchyOutput
	{
		COMPUTED, ALL;
	}

	public HierarchyTransformation(Transformation operand, VTLAlias rulesetID, List<VTLAlias> conditions, VTLAlias id, HierarchyMode mode, HierarchyInput input, HierarchyOutput output)
	{
		this.operand = operand;
		this.rulesetID = requireNonNull(rulesetID);
		this.conditions = coalesce(conditions, emptyList());
		
		this.id = id;
		this.mode = coalesce(mode, NON_NULL);
		this.input = coalesce(input, RULE);
		this.output = coalesce(output, COMPUTED);
		
		if (!this.conditions.isEmpty())
			throw new UnsupportedOperationException("hierarchy conditioning components not implemented.");
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);
		
		DataSetMetadata structure = (DataSetMetadata) getMetadata(scheme);
		DataStructureComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		
		// Store code values that can be computed, to determine the input behavior 
		HierarchicalRuleSet<?, ?, ?, ?> ruleset = scheme.findHierarchicalRuleset(rulesetID);
		Set<? extends CodeItem<?, ?, ?, ?>> computedCodes = ruleset.getComputedCodes();
		Set<? extends CodeItem<?, ?, ?, ?>> nonComputedCodes = ruleset.getRules().stream()
				.map(Rule::getRightCodeItems)
				.flatMap(Set::stream)
				.filter(not(computedCodes::contains))
				.collect(toSet());
		
		// All ids excluding the code id
		Set<DataStructureComponent<Identifier, ?, ?>> noCodeIds = new HashSet<>(dataset.getMetadata().getIDs());
		DataStructureComponent<?, ?, ?> codeId = (ruleset.getType() == VALUE_DOMAIN ? dataset.getComponent(id) : dataset.getComponent(ruleset.getRuleId()))
				.orElseThrow(() -> new VTLMissingComponentsException(id, noCodeIds));
		noCodeIds.remove(codeId);
		
		ScalarValue<?, ?, ?, ?> missingValue;
		if (mode.isZero())
			missingValue = INTEGERDS.isAssignableFrom(measure.getVariable().getDomain())
					? IntegerValue.of(0L)
					: createNumberValue(0.0);
		else
			missingValue = NullValue.instanceFrom(measure);

		// Keep here all datapoints that go into the output, for subsequent streaming
		Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint> results = new ConcurrentHashMap<>();
		
		return new FunctionDataSet<DataSet>(structure, ds -> {
			LOGGER.debug("hierarchy(): classifying source datapoints");
			Map<? extends Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, ? extends Map<CodeItem<?, ?, ?, ?>, DataPoint>> grouped;
			try (Stream<DataPoint> stream = ds.stream())
			{
				Stream<DataPoint> stream2 = stream;
				if (output == ALL)
					stream2 = stream.peek(dp -> results.put(dp.getValues(Identifier.class), dp));
				grouped = stream2.collect(groupingByConcurrent(dp -> dp.getValues(noCodeIds), toMapWithKeys(dp -> (CodeItem<?, ?, ?, ?>) dp.get(codeId))));
			}

			// group computations are independent of each other.
			Utils.getStream(grouped.keySet()).forEach(keyValues -> {
				Map<CodeItem<?, ?, ?, ?>, DataPoint> originalDpGroup = new HashMap<>(grouped.remove(keyValues));
				Map<CodeItem<?, ?, ?, ?>, DataPoint> computedDpGroup = new HashMap<>();
				LOGGER.debug("hierarchy(): Start processing group {}", keyValues);
				
				// Add fictious datapoints with a measure value of 0 for 
				// non-computed codes, so that computation may proceed in any case
				for (CodeItem<?, ?, ?, ?> code: nonComputedCodes)
					if (!originalDpGroup.containsKey(code))
						originalDpGroup.put(code, new DataPointBuilder(keyValues, DONT_SYNC)
								.add(codeId, code)
								.add(measure, missingValue)
								.build(LINEAGE_MISSING, structure));

				// Start with codes that can be computed from the start, then add them on the way.
				Queue<CodeItem<?, ?, ?, ?>> toCompute = new LinkedList<>();
				for (CodeItem<?, ?, ?, ?> code: computedCodes)
					if (canBeComputedNow(code, ruleset, measure, computedCodes, originalDpGroup, computedDpGroup))
						toCompute.add(code);

				while (!toCompute.isEmpty())
				{
					CodeItem<?, ?, ?, ?> code = toCompute.remove();
					LOGGER.trace("Processing code {} in group {}", code, keyValues);
					
					// TODO: assuming only 1 rule per code.
					Rule<?, ?, ?> rule = ruleset.getRulesFor(code).get(0);
					CodeItem<?, ?, ?, ?>[] rightItems = rule.getRightCodeItems().toArray(CodeItem<?, ?, ?, ?>[]::new);
					
					Map<DataStructureComponent<?, ?, ?>, List<ScalarValue<?, ?, ?, ?>>> virals = new HashMap<>();
					
					// Retrieve the datapoints corresponding to the right-side codes of the rule
					Lineage[] lineages = new Lineage[rightItems.length];
					ScalarValue<?, ?, ?, ?>[] values = new ScalarValue<?, ?, ?, ?>[rightItems.length];
					for (int i = 0; i < rightItems.length; i++)
					{
						CodeItem<?, ?, ?, ?> rightCode = rightItems[i];

						DataPoint dpRight;
						if (input == DATASET)
							dpRight = originalDpGroup.get(rightCode);
						else if (input == RULE)
							dpRight = (computedDpGroup.containsKey(rightCode) ? computedDpGroup : originalDpGroup).get(rightCode);
						else // if (input == RULE_PRIORITY)
						{
							dpRight = computedDpGroup.get(rightCode);
							if (dpRight == null || dpRight.get(measure).isNull())
								dpRight = originalDpGroup.get(rightCode);
						}
						
						lineages[i] = dpRight.getLineage();
						values[i] = dpRight.get(measure);
						for (DataStructureComponent<ViralAttribute, ?, ?> viral: structure.getComponents(ViralAttribute.class))
							virals.computeIfAbsent(viral, k -> new ArrayList<>()).add(dpRight.get(viral));
					}
					
					// Perform the calculation
					double accumulator = 0.0;
					boolean allIsNonNull = true;
					boolean allIsMissing = true;
					for (int i = 0; i < rightItems.length; i++)
					{
						if (values[i].isNull())
							allIsNonNull = false;
						else
						{
							double value = ((Number) values[i].get()).doubleValue();
							if (!rule.isPlusSign(rightItems[i]))
								value *= -1;	
							accumulator += value;
						}

						if (lineages[i] != LINEAGE_MISSING)
							allIsMissing = false;
					}
					
					ScalarValue<?, ?, ?, ?> aggResult;
					if (!allIsNonNull)
						aggResult = NullValue.instanceFrom(measure);
					else
						aggResult = INTEGERDS.isAssignableFrom(measure.getVariable().getDomain())
								? IntegerValue.of(round(accumulator))
								: createNumberValue(accumulator);
					
					DataPointBuilder builder = new DataPointBuilder(keyValues, DONT_SYNC)
							.add(codeId, code)
							.add(measure, aggResult);
					for (DataStructureComponent<ViralAttribute, ?, ?> viral: structure.getComponents(ViralAttribute.class))
						builder = builder.add(viral, computeViral(virals.get(viral)));
					
					DataPoint dp = builder.build(LineageNode.of(this, lineages), structure);

					// Depending on mode, store the computed dp for use by other rules
					if (mode == NON_NULL && !aggResult.isNull()
							|| mode == NON_ZERO && !allIsMissing
							|| mode.isPartial() && !allIsMissing
							|| mode.isAlways())
					{
						computedDpGroup.put(code, dp);
						
						// Mode codes could now be computed with this new code result
						Set<? extends CodeItem<?, ?, ?, ?>> dependingCodes = ruleset.getDependingRules(code).stream().map(Rule::getLeftCodeItem).collect(toSet());
						for (CodeItem<?, ?, ?, ?> rightCode: dependingCodes)
							if (!computedDpGroup.containsKey(rightCode)
									&& canBeComputedNow(rightCode, ruleset, measure, computedCodes, originalDpGroup, computedDpGroup))
								toCompute.add(rightCode);
					}
					
					// Output the datapoint if the case
					if (mode == NON_NULL && allIsNonNull
						|| mode == NON_ZERO && accumulator != 0.0
						|| mode.isPartial() && !allIsMissing
						|| mode.isAlways())
					{
						LOGGER.trace("Created output datapoint {}", dp);
						results.put(dp.getValues(Identifier.class), dp);
					}
				}
			}); // forEach lambda end
			
			return Utils.getStream(results.values()).filter(Objects::nonNull);
		}, dataset);
	}

	// TODO: This is only a sample implementation tailored to the examples
	private static ScalarValue<?, ?, ?, ?> computeViral(List<ScalarValue<?, ?, ?, ?>> list)
	{
		return min(list, (v1, v2) -> {
			if (v1.isNull())
				return -1;
			else if (v2.isNull())
				return 1;
			else
				return v1.compareTo(v2);
		});
	}

	private boolean canBeComputedNow(CodeItem<?, ?, ?, ?> code, HierarchicalRuleSet<?, ?, ?, ?> ruleset, DataStructureComponent<Measure, ?, ?> measure,
			Set<? extends CodeItem<?, ?, ?, ?>> computedCodes, Map<CodeItem<?, ?, ?, ?>, DataPoint> originalDpGroup, Map<CodeItem<?, ?, ?, ?>, DataPoint> computedDpGroup)
	{
		Rule<?, ?, ?> rule = ruleset.getRulesFor(code).get(0);
		Set<? extends CodeItem<?, ?, ?, ?>> rightItems = rule.getRightCodeItems();
		
		boolean canBeComputedNow = false;
		if (input == DATASET)
			canBeComputedNow = rightItems.stream().allMatch(originalDpGroup::containsKey);
		else if (input == RULE)
			canBeComputedNow = rightItems.stream().allMatch(c -> computedCodes.contains(c) ? computedDpGroup.containsKey(c) : originalDpGroup.containsKey(c));
		else // if (input == RULE_PRIORITY)
		{
			SerPredicate<? super CodeItem<?, ?, ?, ?>> predicate = c -> {
				if (computedCodes.contains(c))
					return computedDpGroup.containsKey(c) && !computedDpGroup.get(c).getValue(measure).isNull() || originalDpGroup.containsKey(c);
				else
					return originalDpGroup.containsKey(c);
			};
			canBeComputedNow = rightItems.stream().allMatch(predicate);
		}
		return canBeComputedNow;
	}		

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (metadata.isDataSet())
		{
			DataSetMetadata opMeta = (DataSetMetadata) metadata;

			if (opMeta.getMeasures().size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, NUMBERDS, opMeta);
			
			DataStructureComponent<Measure, ?, ?> measure = opMeta.getMeasures().iterator().next();
			if (!NUMBERDS.isAssignableFrom(measure.getVariable().getDomain()))
				throw new VTLIncompatibleTypesException("hierarchy", measure, NUMBERDS);
			
			HierarchicalRuleSet<?, ?, ?, ?> ruleset = scheme.findHierarchicalRuleset(rulesetID);
			if (ruleset != null)
			{
				if (ruleset.getType() == VALUE_DOMAIN && id == null)
					throw new VTLException("A rule variable is required when using a ruleset defined on a valuedomain.");
				
				DataStructureComponent<?, ?, ?> idComp = (ruleset.getType() == VALUE_DOMAIN ? opMeta.getComponent(id) : opMeta.getComponent(ruleset.getRuleId()))
						.orElseThrow(() -> new VTLMissingComponentsException(id, opMeta.getIDs()));
				
				Set<CodeItem<?, ?, ?, ?>> uniqueLeft = new HashSet<>();
				for (Rule<?, ?, ?> rule: ruleset.getRules())
					if (!uniqueLeft.add(rule.getLeftCodeItem()))
						throw new UnsupportedOperationException("Multiple rules for the same code not implemented");
						
				if (!ruleset.getDomain().isAssignableFrom(idComp.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException("hierarchy", idComp, ruleset.getDomain());
			}
			else
				throw new VTLException("Hierarchical ruleset " + rulesetID + " not found.");
			
			return new DataStructureBuilder(opMeta)
					.removeComponents(opMeta.getComponents(Attribute.class))
					.addComponents(opMeta.getComponents(ViralAttribute.class))
					.build();
		}
		else
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);
	}
	
	@Override
	public String toString()
	{
		return 	"hierarchy(" + operand + ", " + rulesetID + (conditions.isEmpty() ? "" : " condition " + String.join(", ", conditions.toString())) + (id == null ? "" : " rule " + id)
				+ " " + mode.toString().toLowerCase() + " " + input.toString().toLowerCase() + " " + output.toString().toLowerCase() + "\")";
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operand.getTerminals();
	}
}
