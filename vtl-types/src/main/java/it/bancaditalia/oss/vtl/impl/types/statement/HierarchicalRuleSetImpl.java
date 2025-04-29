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
package it.bancaditalia.oss.vtl.impl.types.statement;

import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;

public class HierarchicalRuleSetImpl implements HierarchicalRuleSet, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final VTLAlias name;
	private final RuleSetType type;
	private final VTLAlias ruleComp;
	private final List<Entry<VTLAlias, VTLAlias>> conditions;
	private final List<HierarchicalRule> rules;
	private final Map<String, Set<HierarchicalRule>> depends = new HashMap<>();

	public HierarchicalRuleSetImpl(VTLAlias name, RuleSetType type, VTLAlias ruleComp, List<Entry<VTLAlias, VTLAlias>> conditions, List<HierarchicalRule> rules)
	{
		this.name = name; 
		this.ruleComp = ruleComp;
		this.type = type;
		this.conditions = conditions;
		this.rules = rules;

		rules.forEach(rule -> rule.getRightCodeItems().forEach(code -> depends.computeIfAbsent(code, c -> new HashSet<>()).add(rule)));
	}
	
	@Override
	public RuleSetType getType()
	{
		return type;
	}
	
	@Override
	public List<Entry<VTLAlias, VTLAlias>> getConditions()
	{
		return conditions;
	}

	@Override
	public Set<HierarchicalRule> getDependingRules(CodeItem<?, ?, ?, ?> code)
	{
		return coalesce(depends.get(code.get().toString()), emptySet());
	}

	@Override
	public VTLAlias getAlias()
	{
		return name;
	}
	
	@Override
	public VTLAlias getRuleComponent()
	{
		return ruleComp;
	}

	@Override
	public List<HierarchicalRule> getRules()
	{
		return rules;
	}
}
