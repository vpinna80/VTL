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
package it.bancaditalia.oss.vtl.impl.transform.util;

import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType.EQ;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLUndefinedObjectException;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class ResolvedHierarchicalRuleset implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final Set<CodeItem<?, ?, ?, ?>> computedCodes;
	private final Set<CodeItem<?, ?, ?, ?>> leafCodes;
	private final Map<String, CodeItem<?, ?, ?, ?>> codesMap;
	private final Map<CodeItem<?, ?, ?, ?>, List<HierarchicalRule>> computingRules;
	private final EnumeratedDomainSubset<?, ?, ?> codelist;

	public ResolvedHierarchicalRuleset(MetadataRepository repo, HierarchicalRuleSet ruleset)
	{
		ValueDomainSubset<?, ?> domain;
		VTLAlias ruleComp = ruleset.getRuleComponent();
		if (ruleset.getType() == VALUE_DOMAIN)
			domain = repo.getDomain(ruleComp).orElseThrow(() -> new VTLUndefinedObjectException("domain", ruleComp));
		else
			domain = repo.getVariable(ruleComp).map(Variable::getDomain).orElseThrow(() -> new VTLUndefinedObjectException("variable", ruleComp));
		
		if (!(domain instanceof EnumeratedDomainSubset))
			throw new VTLException("Expected an enumerated valuedomain for " + ruleComp + " but it was " + domain);
		if (!(domain instanceof StringDomainSubset))
			throw new VTLException("Expected a string codelist for " + ruleComp + " but it was " + domain);
		
		codelist = (EnumeratedDomainSubset<?, ?, ?>) domain;
		
		Map<String, HierarchicalRule> ruleCodes = ruleset.getRules().stream()
				.filter(rule -> rule.getRuleType() == EQ)
				.collect(toMap(HierarchicalRule::getLeftCodeItem, identity()));
		
		codesMap = ruleCodes.entrySet().stream()
				.flatMap(splitting((left, right) -> Stream.concat(Stream.of(left), right.getRightCodeItems().stream())))
				.distinct()
				.collect(toMap(identity(), code -> (CodeItem<?, ?, ?, ?>) codelist.cast(StringValue.of(code))));
		
		computedCodes = ruleCodes.keySet().stream()
				.map(code -> (CodeItem<?, ?, ?, ?>) codelist.cast(StringValue.of(code)))
				.collect(toSet());
		
		computingRules = ruleset.getRules().stream()
				.filter(rule -> rule.getRuleType() == EQ)
				.collect(groupingByConcurrent(rule -> codesMap.get(rule.getLeftCodeItem()), toList()));
		
		leafCodes = ruleCodes.values().stream()
				.map(HierarchicalRule::getRightCodeItems)
				.flatMap(Set::stream)
				.map(code -> (CodeItem<?, ?, ?, ?>) codelist.cast(StringValue.of(code)))
				.filter(not(computedCodes::contains))
				.collect(toSet());
	}
	
	public EnumeratedDomainSubset<?, ?, ?> getDomain()
	{
		return codelist;
	}
	
	public Set<CodeItem<?, ?, ?, ?>> getComputedCodes()
	{
		return computedCodes;
	}
	
	public Set<CodeItem<?, ?, ?, ?>> getLeafCodes()
	{
		return leafCodes;
	}
	
	public CodeItem<?, ?, ?, ?> mapCode(String code)
	{
		return codesMap.get(code);
	}

	public List<HierarchicalRule> getComputingRulesFor(CodeItem<?, ?, ?, ?> code)
	{
		return computingRules.get(code);
	}
}
