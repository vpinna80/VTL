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

import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.ALL_MEASURES;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.INVALID;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VARIABLE;
import static java.lang.Boolean.FALSE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet.DataPointRule;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CheckDataPointTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	
	private final Transformation operand;
	private final String rulesetID;
	private final List<String> components;
	private final Output output;

	public CheckDataPointTransformation(Transformation operand, String rulesetID, List<String> components, Output output)
	{
		this.operand = operand;
		this.rulesetID = rulesetID;
		this.components = components;
		this.output = output;
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

	@Override
	public DataSet eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);
		DataSetMetadata structure = (DataSetMetadata) getMetadata(scheme);
		final MetadataRepository repo = scheme.getRepository();
		DataPointRuleSet ruleset = repo.getDataPointRuleset(rulesetID);
		RuleSetType type = ruleset.getType();
		List<DataPointRule> rules = ruleset.getRules();
		DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> bool_var = repo.getDefaultVariable(BOOLEANDS).as(Measure.class);
		DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> errorcode = repo.getVariable("errorcode", STRINGDS).as(Measure.class);
		DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> errorlevel = repo.getVariable("errorlevel", INTEGERDS).as(Measure.class);
		DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> ruleid = repo.getVariable("ruleid", STRINGDS).as(Identifier.class);
		
		dataset = dataset.flatmapKeepingKeys(structure, DataPoint::getLineage, dp -> rules.stream()
			.map(r -> {
				if (type != VARIABLE)
					throw new UnsupportedOperationException("check_datapoint on valuedomain ruleset not implemented");

				DatapointScope scope = new DatapointScope(dp, structure, null);
				
				Boolean res = r.eval(dp, scope);
				if (output == INVALID && res != FALSE)
					return null;

				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>();
				map.put(ruleid, StringValue.of(r.getRuleId()));
				map.put(errorcode, res == FALSE ? r.getErrorCode() : NullValue.instance(STRINGDS));
				map.put(errorlevel, res == FALSE ? r.getErrorLevel() : NullValue.instance(STRINGDS));
				
				if (output == INVALID || output == ALL_MEASURES)
					map.putAll(dp.getValues(Measure.class));
				if (output == ALL || output == ALL_MEASURES)
					map.put(bool_var, BooleanValue.of(res));
				
				return map;
			}).filter(Objects::nonNull));
		
		return dataset;
	}

	@Override
	protected DataSetMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme);
		DataPointRuleSet ruleset = scheme.getRepository().getDataPointRuleset(rulesetID);
		MetadataRepository repo = scheme.getRepository();
		
		if (meta instanceof DataSetMetadata)
		{
			DataSetMetadata structure = (DataSetMetadata) meta;
			DataStructureBuilder builder = new DataStructureBuilder(structure.getIDs());
			builder.addComponents(structure.getComponents(Attribute.class));
			
			List<Entry<String, String>> vars = ruleset.getVars();
			if (ruleset.getType() == VARIABLE)
				for (int i = 0; i < vars.size(); i++)
				{
					String var = vars.get(i).getKey();
					if (structure.getComponent(var).filter(c -> c.getVariable().equals(repo.getVariable(var, null))).isEmpty())
						throw new VTLMissingComponentsException(var, structure);
				}
			else
				for (int i = 0; i < vars.size(); i++)
				{
					ValueDomainSubset<?, ?> domain = repo.getDomain(vars.get(i).getKey());
					String compName = components.get(i);
					DataStructureComponent<?, ?, ?> component = structure.getComponent(compName).orElseThrow(() -> new VTLMissingComponentsException(compName, structure));
					
					if (!component.getVariable().getDomain().equals(domain))
						throw new VTLIncompatibleTypesException("check_datapoints", component, domain);
				}
			
			if (output == ALL || output == ALL_MEASURES)
				builder.addComponent(repo.getDefaultVariable(BOOLEANDS).as(Measure.class));
			if (output == INVALID || output == ALL_MEASURES)
				builder.addComponents(structure.getMeasures());
			
			return builder
					.addComponent(repo.getVariable("errorcode", STRINGDS).as(Measure.class))
					.addComponent(repo.getVariable("errorlevel", INTEGERDS).as(Measure.class))
					.addComponent(repo.getVariable("ruleid", STRINGDS).as(Identifier.class))
					.build();
		}
		else
			throw new VTLInvalidParameterException(meta, DataSetMetadata.class);
	}
}
