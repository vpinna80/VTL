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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet.StringRule;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList.StringCodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DefineHierarchyStatement extends AbstractStatement implements RulesetStatement
{
	private static final long serialVersionUID = 1L;

	private final RuleSetType rulesetType;
	private final VTLAlias ruleID;
	private final VTLAlias names[];
	private final String leftOps[];
	private final RuleType[] compOps;
	private final List<Map<String, Boolean>> rightOps;
	private final ScalarValue<?, ?, ?, ?>[] ercodes;
	private final ScalarValue<?, ?, ?, ?>[] erlevels;
	
	public DefineHierarchyStatement(VTLAlias alias, RuleSetType rulesetType, VTLAlias ruleID, List<VTLAlias> names, List<String> leftOps, List<RuleType> compOps, List<Map<String,Boolean>> rightOps, List<String> ercodes, List<Long> erlevels)
	{
		super(alias);
		
		this.rulesetType = rulesetType;
		this.ruleID = ruleID;
		this.names = names.toArray(VTLAlias[]::new);
		this.leftOps = leftOps.toArray(String[]::new);
		this.compOps = compOps.toArray(RuleType[]::new);
		this.rightOps = rightOps;
		this.ercodes = ercodes.stream().map(StringValue::of).toArray(ScalarValue<?, ?, ?, ?>[]::new);
		this.erlevels = erlevels.stream().map(IntegerValue::of).toArray(ScalarValue<?, ?, ?, ?>[]::new);
	}

	@Override
	public RuleSet getRuleSet(TransformationScheme scheme)
	{
		ValueDomainSubset<?, ?> cl;

		if (rulesetType == VALUE_DOMAIN)
			cl = scheme.getRepository().getDomain(ruleID).orElseThrow(() -> new VTLUndefinedObjectException("Domain", ruleID));
		else
		{
			Variable<?, ?> variable = scheme.getRepository().getVariable(ruleID)
					.orElseThrow(() -> new NullPointerException("Variable ruleid is not defined"));
			cl = variable.getDomain();
		}

		if (cl instanceof EnumeratedDomainSubset)
			if (cl instanceof StringCodeList)
			{
				StringCodeList codelist = (StringCodeList) cl;
				
				List<StringRule> rules = IntStream.range(0, names.length)
					.mapToObj(i -> {
						StringCodeItem leftOp = (StringCodeItem) codelist.cast(StringValue.of(leftOps[i]));
						Map<String, Boolean> right = rightOps.get(i);
						Map<StringCodeItem, Boolean> codes = right.keySet().stream()
							.collect(toMap(codelist::getCodeItem, c -> coalesce(right.get(c), true)));
						return new StringRule(names[i], leftOp, compOps[i], codes, ercodes[i], erlevels[i]);
					}).collect(toList());
				
				return new StringHierarchicalRuleSet(getAlias(), ruleID, codelist, rulesetType, rules);
			}
			else
				throw new UnsupportedOperationException("define hierarchical ruleset on codelist of " + cl.getParentDomain() + " not implemented.");
		else
			throw new VTLException(ruleID + " was expected to be a codelist but it is " + cl.getClass().getSimpleName() + " instead.");
	}
}
