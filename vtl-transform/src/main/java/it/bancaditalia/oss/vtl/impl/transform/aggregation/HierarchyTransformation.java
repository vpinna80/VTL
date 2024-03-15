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

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyInput.RULE;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_NULL;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_ZERO;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyOutput.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyOutput.COMPUTED;
import static it.bancaditalia.oss.vtl.impl.types.data.DoubleValue.ZERO;
import static it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl.createNumberValue;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType.EQ;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.Rule;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class HierarchyTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	private final Transformation operand;
	private final String rulesetID;
	private final List<String> conditions;
	private final String id;
	private final HierarchyMode mode;
	private final HierarchyInput input;
	private final HierarchyOutput output;
	
	public enum HierarchyMode
	{
		NON_NULL, NON_ZERO, PARTIAL_NULL, PARTIAL_ZERO, ALWAYS_NULL, ALWAYS_ZERO;
	}
	
	public enum HierarchyInput
	{
		DATASET, RULE, RULE_PRIORITY;
	}
	
	public enum HierarchyOutput
	{
		COMPUTED, ALL;
	}

	public HierarchyTransformation(Transformation operand, String rulesetID, List<String> conditions, String id, HierarchyMode mode, HierarchyInput input, HierarchyOutput output)
	{
		this.operand = operand;
		this.rulesetID = Variable.normalizeAlias(requireNonNull(rulesetID));
		this.conditions = coalesce(conditions, new ArrayList<>()).stream().map(Variable::normalizeAlias).collect(toList());
		
		this.id = id != null ? Variable.normalizeAlias(id) : null;
		this.mode = coalesce(mode, NON_NULL);
		this.input = coalesce(input, RULE);
		this.output = coalesce(output, COMPUTED);
		
		if (!this.conditions.isEmpty())
			throw new UnsupportedOperationException("hierarchy conditioning components not implemented.");
		if (this.mode != NON_NULL && this.mode != NON_ZERO)
			throw new UnsupportedOperationException("hierarchy " + this.mode.toString().toLowerCase() + " option is not implemented.");
		if (this.input != RULE)
			throw new UnsupportedOperationException("hierarchy " + this.input.toString().toLowerCase() + " option is not implemented.");
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);
		
		HierarchicalRuleSet<?, ?, ?, ?, ?> ruleset = scheme.findHierarchicalRuleset(rulesetID);
		
		Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(dataset.getMetadata().getIDs());
		DataStructureComponent<?, ?, ?> idComp = (ruleset.getType() == VALUE_DOMAIN ? dataset.getComponent(id) : dataset.getComponent(ruleset.getRuleId()))
				.orElseThrow(() -> new VTLMissingComponentsException(id, ids));
		ids.remove(idComp);
		
		// Code items that are left-hand in any rule
		List<? extends Rule<?, ?, ?, ?>> rules = ruleset.getRules();
		Set<CodeItem<?, ?, ?, ?>> computed = rules.stream().filter(rule -> rule.getRuleType() == EQ).map(Rule::getLeftCodeItem).collect(toSet());
		DataStructureComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
		DataSetMetadata newStructure = new DataStructureBuilder(ids).addComponent(measure).addComponent(idComp).build();
		
		// key: map with all id vals except idcomp; val: map with key: left-hand code; val: map with vals of each right hand vals for each measure in dataset
		Map<Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>, Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>>> index = new ConcurrentHashMap<>();
		Set<DataPoint> results = ConcurrentHashMap.newKeySet();
		
		try (Stream<DataPoint> stream = dataset.stream())
		{
			stream.forEach(dp -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> key = dp.getValues(ids);
				CodeItem<?, ?, ?, ?> code = (CodeItem<?, ?, ?, ?>) dp.getValue(idComp);
				
				if (input == RULE)
				{
					if (!computed.contains(code))
					{
						if (output == ALL)
							results.add(new DataPointBuilder(dp.getValues(newStructure)).build(dp.getLineage(), newStructure));
						
						for (Rule<?, ?, ?, ?> rule: ruleset.getDependingRules(code))
							processRule(ruleset, rule, idComp, measure, newStructure, index, key, code, results, dp.get(measure));
					}
				}
				else
					throw new UnsupportedOperationException("not implemented.");
			});
		}
		
		if (mode == NON_ZERO)
			// add fictious datapoints with value 0.0 corresponding to each missing leaf datapoint in the ruleset.
			for (Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> key: index.keySet())
				for (CodeItem<?, ?, ?, ?> code: ruleset.getLeaves())
					if (!index.get(key).containsKey(code))
						for (Rule<?, ?, ?, ?> rule: ruleset.getDependingRules(code))
							processRule(ruleset, rule, idComp, measure, newStructure, index, key, code, results, ZERO);
		
		return new StreamWrapperDataSet(newStructure, results::stream); 
	}

	private void processRule(HierarchicalRuleSet<?, ?, ?, ?, ?> ruleset, Rule<?, ?, ?, ?> rule, DataStructureComponent<?, ?, ?> idComp, 
			DataStructureComponent<Measure, ?, ?> measure, DataSetMetadata newStructure, Map<Map<DataStructureComponent<?, ?, ?>, 
			ScalarValue<?, ?, ?, ?>>, Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>>> index, Map<DataStructureComponent<?, ?, ?>, 
			ScalarValue<?, ?, ?, ?>> key, CodeItem<?, ?, ?, ?> code, Set<DataPoint> results, ScalarValue<?, ?, ?, ?> value)
	{
		// add this code value to the map of code-values for key
		Map<CodeItem<?, ?, ?, ?>, ScalarValue<?, ?, ?, ?>> vals = index.computeIfAbsent(key, c -> new ConcurrentHashMap<>());
		vals.put(code, value);
		
		// then, if all the right slots for the rule have been filled in, generate a result datapoint 
		if (vals.keySet().containsAll(rule.getRightCodeItems()))
		{
			double acc = 0;
			
			for (CodeItem<?, ?, ?, ?> rightCode: rule.getRightCodeItems())
			{
				ScalarValue<?, ?, ?, ?> dpValue = requireNonNull(vals.get(rightCode));
				if (!(dpValue instanceof NullValue))
					acc += (rule.isPlusSign(rightCode) ? 1 : -1) * ((Number) dpValue.get()).doubleValue();
				else
					acc = NaN;
			}

			boolean isValidOutput = false;
			ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> result = null;
			if (mode == NON_NULL)
			{
				isValidOutput = Double.isNaN(acc);
				result = isValidOutput ? createNumberValue(acc) : NullValue.instance(NUMBERDS);
			}
			else if (mode == NON_ZERO)
			{
				result = !Double.isNaN(acc) ? createNumberValue(acc) : NullValue.instance(NUMBERDS);
				isValidOutput = !Double.isNaN(acc) && acc != 0.0;
			}
			
			DataPoint resultDataPoint = new DataPointBuilder(key, DONT_SYNC)
					.add(idComp, rule.getLeftCodeItem())
					.add(measure, result)
					.build(LineageNode.of("hierarchy"), newStructure);
			if (isValidOutput)
				results.add(resultDataPoint);

			// recursively invoke processRule in order to reach the subtree leaves of the ruleset
			CodeItem<?, ?, ?, ?> leftCode = rule.getLeftCodeItem();
			for (Rule<?, ?, ?, ?> childRule: ruleset.getDependingRules(leftCode))
				processRule(ruleset, childRule, idComp, measure, newStructure, index, key, leftCode, results, result);
		}
	}

	@Override
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (metadata instanceof DataSetMetadata)
		{
			DataSetMetadata opMeta = (DataSetMetadata) metadata;
			
			HierarchicalRuleSet<?, ?, ?, ?, ?> ruleset = scheme.findHierarchicalRuleset(rulesetID);
			if (ruleset != null)
			{
				if (ruleset.getType() == VALUE_DOMAIN && id == null)
					throw new VTLException("A rule variable is required when using a ruleset defined on a valuedomain.");
				
				DataStructureComponent<?, ?, ?> idComp = (ruleset.getType() == VALUE_DOMAIN ? opMeta.getComponent(id) : opMeta.getComponent(ruleset.getRuleId()))
						.orElseThrow(() -> new VTLMissingComponentsException(id, opMeta.getIDs()));
				
				if (!ruleset.getDomain().isAssignableFrom(idComp.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException("hierarchy", idComp, ruleset.getDomain());
				
				if (opMeta.getMeasures().size() != 1)
					throw new VTLSingletonComponentRequiredException(Measure.class, NUMBERDS, opMeta);
				
				DataStructureComponent<Measure, ?, ?> measure = opMeta.getMeasures().iterator().next();
				if (!NUMBERDS.isAssignableFrom(measure.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException("hierarchy", measure, NUMBERDS);
			}
			else
				throw new VTLException("Hierarchical ruleset " + rulesetID + " not found.");
			
			return new DataStructureBuilder()
					.addComponents(opMeta.getComponents(Identifier.class))
					.addComponents(opMeta.getComponents(Measure.class))
					.build();
		}
		else
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);
	}
	
	@Override
	public String toString()
	{
		return 	"hierarchy(" + operand + ", " + rulesetID + (conditions.isEmpty() ? "" : " condition " + String.join(", ", conditions)) + (id == null ? "" : " rule " + id)
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
