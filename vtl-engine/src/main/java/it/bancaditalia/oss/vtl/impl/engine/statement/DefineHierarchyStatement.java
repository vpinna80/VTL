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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toArray;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.stream.Collectors.toMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet.StringRule;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList.StringCodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DefineHierarchyStatement extends AbstractStatement implements RulesetStatement
{
	private static final long serialVersionUID = 1L;

	private final boolean isValueDomain;
	private final String ruleID;
	private final String names[];
	private final String leftOps[];
	private final RuleType[] compOps;
	private final List<Map<String, Boolean>> rightOps;
	private final ScalarValue<?, ?, ?, ?>[] ercodes;
	private final ScalarValue<?, ?, ?, ?>[] erlevels;
	
	public DefineHierarchyStatement(String alias, boolean isValueDomain, String ruleID, List<String> names, List<String> leftOps, List<RuleType> compOps, List<Map<String,Boolean>> rightOps, List<String> ercodes, List<Long> erlevels)
	{
		super(alias);
		
		this.isValueDomain = isValueDomain;
		this.ruleID = ruleID != null ? Variable.normalizeAlias(ruleID) : null;
		this.names = names.toArray(String[]::new);
		this.leftOps = leftOps.stream().map(n -> n != null ? Variable.normalizeAlias(n) : null).collect(toArray(new String[this.names.length]));
		this.compOps = compOps.toArray(RuleType[]::new);
		this.rightOps = rightOps;
		this.ercodes = ercodes.stream().map(StringValue::of).toArray(ScalarValue<?, ?, ?, ?>[]::new);
		this.erlevels = erlevels.stream().map(IntegerValue::of).toArray(ScalarValue<?, ?, ?, ?>[]::new);
	}

	@Override
	public RuleSet getRuleSet(TransformationScheme scheme)
	{
		if (isValueDomain)
		{
			ValueDomainSubset<?, ?> cl = scheme.getRepository().getDomain(ruleID);
			if (cl instanceof EnumeratedDomainSubset)
				if (cl instanceof StringCodeList)
				{
					StringCodeList codelist = (StringCodeList) cl;
					
					List<StringRule> rules = IntStream.range(0, names.length)
						.mapToObj(i -> {
							StringCodeItem leftOp = (StringCodeItem) codelist.cast(StringValue.of(leftOps[i]));
							Map<String, Boolean> right = rightOps.get(i);
							Map<StringCodeItem, Boolean> codes = right.keySet().stream()
								.map(Variable::normalizeAlias)
								.collect(toMap(codelist::getCodeItem, c -> coalesce(right.get(c), true)));
							return new StringRule(names[i], leftOp, compOps[i], codes, ercodes[i], erlevels[i]);
						}).collect(Collectors.toList());
					
					return new StringHierarchicalRuleSet(ruleID, codelist, isValueDomain, rules);
				}
				else
					throw new UnsupportedOperationException("define hierarchical ruleset on valuedomain " + cl.getParentDomain() + " not implemented.");
			else
				throw new VTLException(ruleID + " was expected to be a codelist but it is " + cl + " instead.");
		}
		else
			throw new UnsupportedOperationException("define hierarchical ruleset on variable not implemented.");
	}
}
