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

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.AbstractMap;
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

public class StringHierarchicalRuleSet extends AbstractMap<StringCodeItem, List<StringRule>> implements HierarchicalRuleSet<StringCodeItem, StringRule, String, StringCodeList, StringDomain>, Serializable
{
	private static final long serialVersionUID = 1L;
	
	public static class StringRule implements Rule<StringCodeItem, String, StringCodeList, StringDomain>
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
	
	private final Map<StringCodeItem, List<StringRule>> rules;
	private final Map<StringCodeItem, Set<StringRule>> depends;
	private final boolean valueDomainHierarchy;
	private final String name;
	private final StringCodeList domain;
	private final Set<StringCodeItem> leaves;

	public StringHierarchicalRuleSet(String name, StringCodeList domain, boolean valueDomainHierarchy, List<StringRule> rules)
	{
		this.name = requireNonNull(name);
		this.domain = requireNonNull(domain);
		this.valueDomainHierarchy = requireNonNull(valueDomainHierarchy);

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
	public boolean isValueDomainHierarchy()
	{
		return valueDomainHierarchy;
	}

	@Override
	public List<StringRule> getRulesFor(CodeItem<?, ?, ?, ?> code)
	{
		return rules.get(code);
	}

	@Override
	public Set<StringRule> getDependingRules(CodeItem<?, ?, ?, ?> code)
	{
		return Utils.coalesce(depends.get(code), emptySet());
	}
	
	@Override
	public Set<StringCodeItem> getLeaves()
	{
		return leaves;
	}

	@Override
	public String getName()
	{
		return name;
	}

	@Override
	public StringCodeList getDomain()
	{
		return domain;
	}

	@Override
	public Map<StringCodeItem, List<StringRule>> getRules()
	{
		return rules;
	}

	@Override
	public Set<Map.Entry<StringCodeItem, List<StringRule>>> entrySet()
	{
		return rules.entrySet();
	}

	public boolean containsKey(Object key)
	{
		return rules.containsKey(key);
	}

	public List<StringRule> get(Object key)
	{
		return rules.get(key);
	}

	public Set<StringCodeItem> keySet()
	{
		return rules.keySet();
	}
}
