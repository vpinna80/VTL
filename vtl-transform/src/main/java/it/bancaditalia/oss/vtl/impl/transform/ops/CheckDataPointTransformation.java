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
import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.FALSE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORCODE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORLEVEL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VARIABLE;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

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
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
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
	private final VTLAlias rulesetID;
	private final List<VTLAlias> components;
	private final Output output;

	public CheckDataPointTransformation(Transformation operand, VTLAlias rulesetID, List<VTLAlias> components, Output output)
	{
		this.operand = operand;
		this.rulesetID = rulesetID;
		this.components = components;
		this.output = coalesce(output, INVALID);
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
		MetadataRepository repo = scheme.getRepository();
		DataPointRuleSet ruleset = scheme.findDatapointRuleset(rulesetID);
		RuleSetType type = ruleset.getType();
		List<DataPointRule> rules = ruleset.getRules();
		DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> bool_var = BOOLEANDS.getDefaultVariable().as(Measure.class);
		DataStructureComponent<Identifier, ?, ?> ruleid = repo.getVariable(VTLAliasImpl.of("ruleid")).as(Identifier.class);
		
		return dataset.flatmapKeepingKeys(structure, DataPoint::getLineage, dp -> rules.stream()
			.map(rule -> evalRule(new DatapointScope(repo, dp, dataset.getMetadata(), null), type, bool_var, ruleid, dp, rule))
			.filter(Objects::nonNull));
	}

	private Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> evalRule(TransformationScheme scope, RuleSetType type,
			DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> bool_var, 
			DataStructureComponent<Identifier, ?, ?> ruleid, DataPoint dp, DataPointRule rule)
	{
		if (type != VARIABLE)
			throw new UnsupportedOperationException("check_datapoint on valuedomain ruleset not implemented");
		
		ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> res = rule.eval(dp, scope);
		if (output == INVALID && res != FALSE)
			return null;

		Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>();
		map.put(ruleid, StringValue.of(rule.getRuleId().getName()));
		map.put(ERRORCODE.get(), res == FALSE ? rule.getErrorCode() : NullValue.instance(STRINGDS));
		map.put(ERRORLEVEL.get(), res == FALSE ? rule.getErrorLevel() : NullValue.instance(INTEGERDS));
		
		if (output == INVALID || output == ALL_MEASURES)
			map.putAll(dp.getValues(Measure.class));
		if (output == ALL || output == ALL_MEASURES)
			map.put(bool_var, res);
		
		return map;
	}

	@Override
	protected DataSetMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme);
		DataPointRuleSet ruleset = scheme.findDatapointRuleset(rulesetID);
		MetadataRepository repo = scheme.getRepository();
		
		if (meta instanceof DataSetMetadata)
		{
			DataSetMetadata structure = (DataSetMetadata) meta;
			DataStructureBuilder builder = new DataStructureBuilder()
					.addComponents(structure.getComponents(Identifier.class))
					.addComponents(structure.getComponents(Attribute.class));

			List<Entry<VTLAlias, VTLAlias>> vars = ruleset.getVars();
			if (ruleset.getType() == VARIABLE)
				for (int i = 0; i < vars.size(); i++)
				{
					VTLAlias varName = vars.get(i).getKey();
					if (structure.getComponent(varName).filter(c -> c.getVariable().equals(repo.getVariable(varName))).isEmpty())
						throw new VTLMissingComponentsException(varName, structure);
				}
			else
				for (int i = 0; i < vars.size(); i++)
				{
					ValueDomainSubset<?, ?> domain = repo.getDomain(vars.get(i).getKey());
					VTLAlias compName = components.get(i);
					DataStructureComponent<?, ?, ?> component = structure.getComponent(compName).orElseThrow(() -> new VTLMissingComponentsException(compName, structure));
					
					if (!component.getVariable().getDomain().equals(domain))
						throw new VTLIncompatibleTypesException("check_datapoints", component, domain);
				}
			
			if (output == ALL || output == ALL_MEASURES)
				builder = builder.addComponent(BOOLEANDS.getDefaultVariable().as(Measure.class));
			if (output == INVALID || output == ALL_MEASURES)
				builder = builder.addComponents(structure.getMeasures());
			
			return builder
					.addComponent(ERRORCODE.get())
					.addComponent(ERRORLEVEL.get())
					.addComponent(repo.getVariable(VTLAliasImpl.of("ruleid")).as(Identifier.class))
					.build();
		}
		else
			throw new VTLInvalidParameterException(meta, DataSetMetadata.class);
	}
}
