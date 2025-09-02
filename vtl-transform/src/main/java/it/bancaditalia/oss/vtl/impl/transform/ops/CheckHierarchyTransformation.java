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
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_NULL;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_ZERO;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Input.DATASET;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Input.DATASET_PRIORITY;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.ALL_MEASURES;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.INVALID;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORCODE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORLEVEL;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.IMBALANCE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.RULEID;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIFF;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VARIABLE;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.ORDERED;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.lang.Math.round;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode;
import it.bancaditalia.oss.vtl.impl.transform.util.ResolvedHierarchicalRuleset;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.statement.HierarchicalRuleSetImpl;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.Utils;

public class CheckHierarchyTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(HierarchyTransformation.class);

	public enum Input implements Serializable
	{
		DATASET, DATASET_PRIORITY;
	}

	public enum Output implements Serializable
	{
		INVALID, ALL, ALL_MEASURES;
	}

	private final Transformation operand;
	private final VTLAlias rulesetID;
	private final List<VTLAlias> conditions;
	private final VTLAlias id;
	private final HierarchyMode mode;
	private final Input input;
	private final Output output;

	public CheckHierarchyTransformation(Transformation operand, VTLAlias rulesetID, List<VTLAlias> conditions, VTLAlias id, HierarchyMode mode, Input input, Output output)
	{
		this.operand = operand;
		this.rulesetID = requireNonNull(rulesetID);
		this.conditions = coalesce(conditions, new ArrayList<VTLAlias>()).stream().collect(toList());
		
		this.id = id;
		this.mode = coalesce(mode, NON_NULL);
		this.input = coalesce(input, DATASET);
		this.output = coalesce(output, INVALID);
		
		if (!this.conditions.isEmpty())
			throw new UnsupportedOperationException("check_hierarchy conditioning components not implemented.");
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);

		DataSetComponent<Measure, ?, ?> measure = dataset.getMetadata().getSingleton(Measure.class);
		DataSetStructure newStructure = (DataSetStructure) getMetadata(scheme);
		
		// Store code values that can be computed, to determine the input behavior 
		HierarchicalRuleSet ruleset = (HierarchicalRuleSetImpl) scheme.findHierarchicalRuleset(rulesetID);
		ResolvedHierarchicalRuleset resolved = new ResolvedHierarchicalRuleset(scheme.getRepository(), ruleset);
		Set<? extends CodeItem<?, ?, ?, ?>> computedCodes = resolved.getComputedCodes();
		Set<? extends CodeItem<?, ?, ?, ?>> leafCodes = resolved.getLeafCodes();

		// All ids excluding the code id
		Set<DataSetComponent<Identifier, ?, ?>> noCodeIds = new HashSet<>(dataset.getMetadata().getIDs());
		DataSetComponent<?, ?, ?> codeId = (ruleset.getType() == VALUE_DOMAIN ? dataset.getComponent(id) : dataset.getComponent(ruleset.getRuleComponent()))
				.orElseThrow(() -> new VTLMissingComponentsException(noCodeIds, id));
		noCodeIds.remove(codeId);

		// Determine which missing value to use
		ScalarValue<?, ?, ?, ?> missingValue;
		if (mode.isZero())
			missingValue = INTEGERDS.isAssignableFrom(measure.getDomain())
					? IntegerValue.of(0L)
					: createNumberValue(0.0);
		else
			missingValue = NullValue.instanceFrom(measure);

		return new FunctionDataSet<>(newStructure, ds -> {
			LOGGER.debug("check_hierarchy(): classifying source datapoints");
			Map<? extends Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, ? extends Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>>> grouped;
			try (Stream<DataPoint> stream = dataset.stream())
			{
				grouped = stream.collect(groupingByConcurrent(dp -> dp.getValues(noCodeIds), 
						toConcurrentMap(dp -> resolved.cast(dp.get(codeId)), dp -> dp.get(measure))));
			}
			
			// Examine each group separately. A group may also produce an empty stream if no rule was computed.
			return Utils.getStream(grouped.keySet())
				.map(keyValues -> {
					Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>> originalDpGroup = grouped.get(keyValues);
					Set<CodeItem<?, ?, ?, ?>> missingCodes = new HashSet<>();
					Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>> computedDpGroup = new HashMap<>();
					LOGGER.debug("check_hierarchy(): Start processing group {}", keyValues);
					boolean integerComputation = INTEGERDS.isAssignableFrom(measure.getDomain());
					List<DataPoint> results = new ArrayList<>();
	
					// Add missing values for non-computed codes, so that computation may proceed in any case
					for (CodeItem<?, ?, ?, ?> code: leafCodes)
						if (!originalDpGroup.containsKey(code))
						{
							missingCodes.add(code);
							originalDpGroup.put(code, missingValue);
						}
	
					// Start with rules that can be computed from the start
					Queue<HierarchicalRule> toCompute = new LinkedList<>();
					for (HierarchicalRule rule: ruleset.getRules())
						if (canBeComputedNow(resolved, rule, computedCodes, missingCodes, originalDpGroup, computedDpGroup))
							toCompute.add(rule);
					
					for (HierarchicalRule rule = toCompute.poll(); rule != null; rule = toCompute.poll())
					{
						CodeItem<?, ?, ?, ?> code = resolved.mapCode(rule.getLeftCodeItem());
						ScalarValue<?, ?, ?, ?> originalLeftValue = originalDpGroup.get(code);
						if (input == DATASET_PRIORITY && originalLeftValue.isNull() && computedDpGroup.containsKey(code))
							originalLeftValue = computedDpGroup.get(code);
						if (originalLeftValue == null)
							originalLeftValue = missingValue;
							
						LOGGER.trace("Processing code {} in group {}", code, keyValues);
						
						// Perform the calculation
						double accumulator = 0.0;
						boolean allIsNonNull = true;
						boolean allIsMissing = true;
						for (String rightCodeString: rule.getRightCodeItems())
						{
							CodeItem<?, ?, ?, ?> rightCode = resolved.mapCode(rightCodeString);
									
							ScalarValue<?, ?, ?, ?> value = originalDpGroup.get(rightCode);
							if (input == DATASET_PRIORITY && (value == null || value.isNull()) && computedDpGroup.containsKey(rightCode))
								value = computedDpGroup.get(rightCode);
	
							if (value == null || value.isNull())
							{
								allIsNonNull = false;
								if (!missingCodes.contains(rightCode))
									allIsMissing = false;
							}
							else
							{
								double n = ((Number) value.get()).doubleValue();
								if (!rule.isPlusSign(rightCodeString))
									n *= -1;	
								accumulator += n;
								allIsMissing = false;
							}
						}
						
						ScalarValue<?, ?, ?, ?> aggResult;
						if (allIsNonNull)
							aggResult = integerComputation
									? IntegerValue.of(round(accumulator))
									: createNumberValue(accumulator);
						else
							aggResult = NullValue.instanceFrom(measure);
						
						// Depending on mode, store the computed dp for use by other rules
						if (mode == NON_NULL && !originalLeftValue.isNull() && !aggResult.isNull()
								|| mode == NON_ZERO && !(allIsMissing && originalLeftValue.isNull())
								|| mode.isPartial() && !(allIsMissing && originalLeftValue.isNull())
								|| mode.isAlways())
						{
							computedDpGroup.put(code, aggResult);
							
							// Mode codes could now be computed with this new code result
							for (HierarchicalRule dependingRule: ruleset.getDependingRules(code))
								if (!computedDpGroup.containsKey(resolved.mapCode(dependingRule.getLeftCodeItem())) 
										&& canBeComputedNow(resolved, dependingRule, computedCodes, missingCodes, originalDpGroup, computedDpGroup))
									toCompute.add(dependingRule);
						}
						
						allIsNonNull &= !originalLeftValue.isNull();
						allIsMissing &= originalLeftValue.isNull();
						
						// Output the datapoint if the case
						if (mode == NON_NULL && allIsNonNull
							|| mode == NON_ZERO && accumulator != 0.0
							|| mode.isPartial() && !allIsMissing
							|| mode.isAlways())
						{
							ScalarValue<?, ?, ?, ?> imbalance;
							if (originalLeftValue.isNull() || aggResult.isNull())
								imbalance = NullValue.instanceFrom(measure);
							else
								imbalance = integerComputation
										? DIFF.applyAsInteger(originalLeftValue, aggResult)
										: DIFF.applyAsNumber(originalLeftValue, aggResult);
	
							DataPointBuilder builder = new DataPointBuilder(keyValues, DONT_SYNC)
									.add(codeId, code)
									.add(RULEID, StringValue.of(rule.getAlias().getName()))
									.add(IMBALANCE, NUMBERDS.cast(imbalance))
									.add(ERRORCODE, (ScalarValue<?, ?, ?, ?>) rule.getErrorCode())
									.add(ERRORLEVEL, (ScalarValue<?, ?, ?, ?>) rule.getErrorLevel());
							
							if (output == ALL || output == ALL_MEASURES)
							{
								ScalarValue<?, ?, ?, ?> test;
								if (imbalance.isNull())
									test = BooleanValue.NULL;
								else if (integerComputation)
									test = BooleanValue.of(rule.getRuleType().test(imbalance, IntegerValue.of(0L)));
								else
									test = BooleanValue.of(rule.getRuleType().test(imbalance, NumberValueImpl.createNumberValue(0.0)));
	
								builder = builder.add(BOOL_VAR, test);
							}
							if (output == INVALID || output == ALL_MEASURES)
								builder = builder.add(measure, originalLeftValue);
									
							DataPoint dp = builder.build(LineageNode.of(this), newStructure);
							
							LOGGER.trace("Created output datapoint {}", dp);
							results.add(dp);
						}
					}
				return results.stream();
			}).collect(concatenating(ORDERED));
		}, dataset);
		

	}

	private boolean canBeComputedNow(ResolvedHierarchicalRuleset resolved, HierarchicalRule rule, Set<? extends CodeItem<?, ?, ?, ?>> computedCodes, Set<CodeItem<?, ?, ?, ?>> missingCodes, 
			Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>> originalDpGroup, Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>> computedDpGroup)
	{
		CodeItem<?, ?, ?, ?> leftItem = resolved.mapCode(rule.getLeftCodeItem());
		
		// check the left code
		if (mode == NON_NULL && !originalDpGroup.containsKey(leftItem))
			if (input == DATASET_PRIORITY && computedCodes.contains(leftItem))
			{
				if (!computedDpGroup.containsKey(leftItem))
					return false;
			}
			else if (!computedCodes.contains(leftItem))
				return false;
		if (mode.isPartial() && !missingCodes.contains(leftItem))
			if (originalDpGroup.containsKey(leftItem) || input == DATASET_PRIORITY && computedCodes.contains(leftItem))
				return true;
		
		SerPredicate<? super CodeItem<?, ?, ?, ?>> predicate = originalDpGroup::containsKey;
		if (input == DATASET_PRIORITY)
			predicate = predicate.or(computedDpGroup::containsKey);

		return rule.getRightCodeItems().stream().map(resolved::mapCode).allMatch(predicate);
	}		

	@Override
	protected DataSetStructure computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);

		if (metadata.isDataSet())
		{
			DataSetStructure opMeta = (DataSetStructure) metadata;

			if (opMeta.getMeasures().size() != 1)
				throw new VTLSingletonComponentRequiredException(Measure.class, NUMBERDS, opMeta);

			DataSetComponent<Measure, ?, ?> measure = opMeta.getMeasures().iterator().next();
			if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
				throw new VTLIncompatibleTypesException("check_hierarchy", measure, NUMBERDS);

			HierarchicalRuleSet ruleset = scheme.findHierarchicalRuleset(rulesetID);
			if (ruleset != null)
			{
				VTLAlias idCompAlias = ruleset.getType() == VARIABLE ? ruleset.getRuleComponent() : id;
				if (idCompAlias == null)
					throw new VTLException("Rule component is mandatory when using a valuedomain hierarchical ruleset.");
				
				DataSetComponent<?, ?, ?> idComp = opMeta.getComponent(idCompAlias)
						.orElseThrow(() -> new VTLMissingComponentsException(opMeta.getIDs(), ruleset.getRuleComponent()));
				
				ResolvedHierarchicalRuleset resolved = new ResolvedHierarchicalRuleset(scheme.getRepository(), ruleset);
				EnumeratedDomainSubset<?, ?, ?, ?> domain = resolved.getDomain();
				
				if (!domain.isAssignableFrom(idComp.getDomain()))
					throw new VTLIncompatibleTypesException("check_hierarchy", idComp, domain);
			}
			else
				throw new VTLException("Hierarchical ruleset " + rulesetID + " not found.");

			DataSetStructureBuilder builder = new DataSetStructureBuilder(opMeta.getComponents(Identifier.class));
			if (output != ALL)
				builder = builder.addComponents(opMeta.getComponents(Measure.class));
			if (output != INVALID)
				builder = builder.addComponent(BOOL_VAR);

			return builder.addComponents(RULEID, IMBALANCE, ERRORCODE, ERRORLEVEL).build();
		}
		else
			throw new VTLInvalidParameterException(metadata, DataSetStructure.class);
	}
	
	@Override
	public boolean hasAnalytic()
	{
		return false;
	}

	@Override
	public String toString()
	{
		return "check_hierarchy(" + operand + ", " + rulesetID + (conditions.isEmpty() ? "" : " condition " + join(", ", conditions.toString())) + (id == null ? "" : " rule " + id) + " "
				+ mode.toString().toLowerCase() + " " + input.toString().toLowerCase() + " " + output.toString().toLowerCase() + "\")";
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
