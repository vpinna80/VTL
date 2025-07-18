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
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORCODE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORLEVEL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VARIABLE;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
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
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output;
import it.bancaditalia.oss.vtl.impl.transform.scope.DatapointScope;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
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
		DataSetStructure structure = (DataSetStructure) getMetadata(scheme);
		MetadataRepository repo = scheme.getRepository();
		DataPointRuleSet ruleset = scheme.findDatapointRuleset(rulesetID);
		RuleSetType type = ruleset.getType();
		List<DataPointRule> rules = ruleset.getRules();
		Variable<?, ?> variable = repo.getVariable(VTLAliasImpl.of("ruleid")).orElseThrow(() -> new VTLUndefinedObjectException("Variable", VTLAliasImpl.of("ruleid")));
		DataSetComponent<Identifier, ?, ?> ruleid = DataSetComponentImpl.of(variable.getAlias(), variable.getDomain(), Identifier.class);
		
		Output output = this.output;
		DataSetStructure metadata = dataset.getMetadata();
		return dataset.flatmapKeepingKeys(structure, identity(), dp -> rules.stream()
			.map(rule -> evalRule(new DatapointScope(repo, dp, metadata, null), type, output, BOOL_VAR, ruleid, dp, rule))
			.filter(Objects::nonNull));
	}

	private static Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> evalRule(TransformationScheme scope,
			RuleSetType type, Output output,
			DataSetComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> bool_var, 
			DataSetComponent<Identifier, ?, ?> ruleid, DataPoint dp, DataPointRule rule)
	{
		if (type != VARIABLE)
			throw new UnsupportedOperationException("check_datapoint on valuedomain ruleset not implemented");
		
		ScalarValue<?, ?, ? extends BooleanDomainSubset<?>, BooleanDomain> res = rule.eval(dp, scope);
		if (output == INVALID && res != FALSE)
			return null;

		Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> map = new HashMap<>();
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
	protected DataSetStructure computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme);
		DataPointRuleSet ruleset = scheme.findDatapointRuleset(rulesetID);
		MetadataRepository repo = scheme.getRepository();
		
		if (meta.isDataSet())
		{
			DataSetStructure structure = (DataSetStructure) meta;
			DataSetStructureBuilder builder = new DataSetStructureBuilder()
					.addComponents(structure.getComponents(Identifier.class))
					.addComponents(structure.getComponents(Attribute.class));

			List<Entry<VTLAlias, VTLAlias>> vars = ruleset.getVars();
			if (ruleset.getType() == VARIABLE)
				for (int i = 0; i < vars.size(); i++)
				{
					VTLAlias varName = vars.get(i).getKey();
					if (structure.getComponent(varName).isEmpty())
						throw new VTLMissingComponentsException(structure, varName);
					if (repo.getVariable(varName).isEmpty())
						throw new VTLUndefinedObjectException("Variable", varName);
				}
			else
				for (int i = 0; i < vars.size(); i++)
				{
					VTLAlias alias = vars.get(i).getKey();
					ValueDomainSubset<?, ?> domain = repo.getDomain(alias).orElseThrow(() -> new VTLUndefinedObjectException("Domain", alias));
					VTLAlias compName = components.get(i);
					DataSetComponent<?, ?, ?> component = structure.getComponent(compName).orElseThrow(() -> new VTLMissingComponentsException(structure, compName));
					
					if (!component.getDomain().equals(domain))
						throw new VTLIncompatibleTypesException("check_datapoints", component, domain);
				}
			
			if (output == ALL || output == ALL_MEASURES)
				builder = builder.addComponent(BOOL_VAR);
			if (output == INVALID || output == ALL_MEASURES)
				builder = builder.addComponents(structure.getMeasures());
			
			Variable<?, ?> variable = repo.getVariable(VTLAliasImpl.of("ruleid")).orElseThrow(() -> new VTLUndefinedObjectException("Variable", VTLAliasImpl.of("ruleid")));
			return builder
					.addComponent(ERRORCODE.get())
					.addComponent(ERRORLEVEL.get())
					.addComponent(DataSetComponentImpl.of(variable.getAlias(), variable.getDomain(), Identifier.class))
					.build();
		}
		else
			throw new VTLInvalidParameterException(meta, DataSetStructure.class);
	}
}
