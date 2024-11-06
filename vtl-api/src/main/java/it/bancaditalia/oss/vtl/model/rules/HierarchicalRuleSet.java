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
package it.bancaditalia.oss.vtl.model.rules;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.EnumeratedDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.Rule;

/**
 * Representation of a hierarchy ruleset.
 * 
 * @author Valentino Pinna
 */
public interface HierarchicalRuleSet<I extends Rule<S, D>, S extends EnumeratedDomainSubset<S, D>, D extends ValueDomain> extends RuleSet
{
	public interface Rule<S extends EnumeratedDomainSubset<S, D>, D extends ValueDomain>
	{
		public VTLAlias getAlias();
		
		public CodeItem<?, ?, S, D> getLeftCodeItem();

		public Collection<? extends CodeItem<?, ?, S, D>> getRightCodeItems();
		
		public boolean isPlusSign(CodeItem<?, ?, ?, ?> item);
		
		public RuleType getRuleType();
		
		public ScalarValue<?, ?, ?, ?> getErrorCode();
		
		public ScalarValue<?, ?, ?, ?> getErrorLevel();
	}
	
	public RuleSetType getType();
	
	public List<I> getRulesFor(CodeItem<?, ?, ?, ?> code);
	
	public VTLAlias getAlias();

	public VTLAlias getRuleId();

	public List<I> getRules();
	
	public S getDomain();

	public Set<I> getDependingRules(CodeItem<?, ?, ?, ?> code);

	public Set<? extends CodeItem<?, ?, S, D>> getLeaves();
}
