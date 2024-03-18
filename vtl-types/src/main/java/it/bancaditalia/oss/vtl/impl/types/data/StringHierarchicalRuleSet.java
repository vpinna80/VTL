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
package it.bancaditalia.oss.vtl.impl.types.data;

import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet.StringRule;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList.StringCodeItem;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.Rule;
import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.util.Utils;

public class StringHierarchicalRuleSet implements HierarchicalRuleSet<StringRule, StringCodeList, StringDomain>, Serializable
{
	private static final long serialVersionUID = 1L;
	
	public static class StringRule implements Rule<StringCodeList, StringDomain>
	{
		private final String name;
		private final StringCodeItem leftCodeItem;
		private final RuleSet.RuleType ruleType;
		private final Map<StringCodeItem, Boolean> sign;
		private final ScalarValue<?, ?, ?, ?> errorCode;
		private final ScalarValue<?, ?, ?, ?> errorLevel;
		
		public StringRule(String name, StringCodeItem leftCodeItem, RuleSet.RuleType ruleType, Map<StringCodeItem, Boolean> sign, ScalarValue<?, ?, ?, ?> errorCode, ScalarValue<?, ?, ?, ?> errorLevel)
		{
			this.name = name;
			this.leftCodeItem = leftCodeItem;
			this.ruleType = ruleType;
			this.sign = sign;
			this.errorCode = errorCode;
			this.errorLevel = errorLevel;
		}

		@Override
		public String getName()
		{
			return name;
		}
		
		@Override
		public StringCodeItem getLeftCodeItem()
		{
			return leftCodeItem;
		}

		@Override
		public Collection<StringCodeItem> getRightCodeItems()
		{
			return sign.keySet();
		}

		@Override
		public boolean isPlusSign(CodeItem<?, ?, ?, ?> item)
		{
			return sign.getOrDefault(item, true);
		}

		@Override
		public RuleSet.RuleType getRuleType()
		{
			return ruleType;
		}
		
		@Override
		public String toString()
		{
			return leftCodeItem + " " + ruleType + sign.entrySet().stream().map(e -> (e.getValue() ? " + " : " - ") + e.getKey()).collect(joining());
		}

		public ScalarValue<?, ?, ?, ?> getErrorCode()
		{
			return errorCode;
		}

		public ScalarValue<?, ?, ?, ?> getErrorLevel()
		{
			return errorLevel;
		}
	}
	
	private final String name;
	private final String ruleComp;
	private final Map<StringCodeItem, List<StringRule>> rules;
	private final Map<StringCodeItem, Set<StringRule>> depends;
	private final RuleSetType type;
	private final StringCodeList domain;
	private final Set<CodeItem<?, ?, StringCodeList, StringDomain>> leaves;

	public StringHierarchicalRuleSet(String name, String ruleComp, StringCodeList domain, RuleSetType type, List<StringRule> rules)
	{
		this.name = name; 
		this.ruleComp = requireNonNull(ruleComp);
		this.domain = requireNonNull(domain);
		this.type = type;

		this.rules = new HashMap<>();
		rules.forEach(rule -> this.rules.computeIfAbsent(rule.getLeftCodeItem(), c -> new ArrayList<>()).add(rule));
		this.depends = new HashMap<>();
		this.leaves = new HashSet<>(domain.getCodeItems());
		rules.forEach(rule -> {
				rule.getRightCodeItems().forEach(code -> depends.computeIfAbsent(code, c -> new HashSet<>()).add(rule));
				this.leaves.remove(rule.getLeftCodeItem());
			});
		
		rules.forEach(rule -> rule.getRightCodeItems().forEach(code -> depends.computeIfAbsent(code, c -> new HashSet<>()).add(rule)));
	}
	
	@Override
	public RuleSetType getType()
	{
		return type;
	}

	@Override
	public List<StringRule> getRulesFor(CodeItem<?, ?, ?, ?> code)
	{
		return rules.get(code);
	}

	@Override
	public Set<StringRule> getDependingRules(CodeItem<?, ?, ?, ?> code)
	{
		return coalesce(depends.get(code), emptySet());
	}
	
	@Override
	public Set<CodeItem<?, ?, StringCodeList, StringDomain>> getLeaves()
	{
		return leaves;
	}

	@Override
	public String getName()
	{
		return name;
	}
	
	@Override
	public String getRuleId()
	{
		return ruleComp;
	}

	@Override
	public StringCodeList getDomain()
	{
		return domain;
	}

	@Override
	public List<StringRule> getRules()
	{
		return Utils.getStream(rules.values()).map(List::stream).collect(concatenating(false)).collect(toList());
	}
}
