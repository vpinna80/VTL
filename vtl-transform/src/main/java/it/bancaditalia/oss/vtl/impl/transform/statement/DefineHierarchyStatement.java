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
package it.bancaditalia.oss.vtl.impl.transform.statement;

import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;

import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.impl.types.statement.AbstractStatement;
import it.bancaditalia.oss.vtl.impl.types.statement.HierarchicalRuleSetImpl;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;
import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType;

public class DefineHierarchyStatement extends AbstractStatement implements RulesetStatement
{
	private static final long serialVersionUID = 1L;

	private final List<Entry<VTLAlias, VTLAlias>> conditions = new ArrayList<>();
	private final HierarchicalRuleSetImpl ruleset;
	
	public DefineHierarchyStatement(VTLAlias rulesetID, RuleSetType rulesetType, VTLAlias ruleComp, List<VTLAlias> condVars, List<VTLAlias> condAliases, List<HierarchicalRule> rules)
	{
		super(rulesetID);
		
		if (condVars != null)
			for (int i = 0; i < condVars.size(); i++)
			{
				VTLAlias condVar = Objects.requireNonNull(condVars.get(i));
				conditions.add(new SimpleEntry<>(condVar, coalesce(condAliases.get(i), condVar)));
			}
		
		ruleset = new HierarchicalRuleSetImpl(rulesetID, rulesetType, ruleComp, conditions, rules);
	}

	@Override
	public RuleSet getRuleSet()
	{
		return ruleset;
	}
//	{
//		ValueDomainSubset<?, ?> cl;
//
//		if (rulesetType == VALUE_DOMAIN)
//			cl = scheme.getRepository().getDomain(ruleComp).orElseThrow(() -> new VTLUndefinedObjectException("Domain", ruleComp));
//		else
//		{
//			Variable<?, ?> variable = scheme.getRepository().getVariable(ruleComp)
//					.orElseThrow(() -> new NullPointerException("Variable ruleid is not defined"));
//			cl = variable.getDomain();
//		}
//
//		if (cl instanceof EnumeratedDomainSubset)
//			if (cl instanceof StringCodeList)
//			{
//				StringCodeList codelist = (StringCodeList) cl;
//				
//				List<HierarchicalRule> rules = IntStream.range(0, names.length)
//					.mapToObj(i -> {
//						StringCodeItem leftOp = (StringCodeItem) codelist.cast(StringValue.of(leftOps[i]));
//						Map<String, Boolean> right = rightOps.get(i);
//						Map<StringCodeItem, Boolean> codes = right.keySet().stream()
//							.collect(toMap(codelist::getCodeItem, c -> coalesce(right.get(c), true)));
//						return new StringRule(names[i], leftOp, compOps[i], whenOps[i], codes, ercodes[i], erlevels[i]);
//					}).collect(toList());
//				
//				return new HierarchicalRuleSetImpl(getAlias(), ruleID, codelist, rulesetType, rules);
//			}
//			else
//				throw new UnsupportedOperationException("define hierarchical ruleset on codelist of " + cl.getParentDomain() + " not implemented.");
//		else
//			throw new VTLException(ruleID + " was expected to be a codelist but it is " + cl.getClass().getSimpleName() + " instead.");
//	}
}
