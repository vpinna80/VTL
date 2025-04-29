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

import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;

import java.util.AbstractMap.SimpleEntry;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.IntStream;

import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.AbstractStatement;
import it.bancaditalia.oss.vtl.impl.types.statement.DataPointRuleImpl;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;

public class DefineDataPointStatement extends AbstractStatement implements RulesetStatement, DataPointRuleSet
{
	private static final long serialVersionUID = 1L;
	
	private final RuleSetType type;
	private final List<DataPointRule> rules;
	private final List<VTLAlias> cols;
	private final List<VTLAlias> aliases;

	
	public DefineDataPointStatement(VTLAlias alias, RuleSetType rulesetType, List<VTLAlias> cols, List<VTLAlias> aliases, List<DataPointRule> rules)
	{
		super(alias);
		
		this.type = rulesetType;
		this.cols = cols;
		this.aliases = aliases;
		
		Set<VTLAlias> ruleNames = new HashSet<>();
		DataPointRule[] rulesArray = rules.toArray(DataPointRule[]::new);
		for (int i = 0; i < rulesArray.length; i++)
		{
			if (rulesArray[i].getRuleId() == null)
				rulesArray[i] = ((DataPointRuleImpl) rulesArray[i]).getRenamed(VTLAliasImpl.of(true, String.format("%d", i + 1)));
			
			VTLAlias ruleId = rulesArray[i].getRuleId();
			if (!ruleNames.add(ruleId))
				throw new VTLException("Duplicated rule name in data point ruleset " + getAlias() + ": " + ruleId);
		}
		this.rules = Arrays.asList(rulesArray); 			
	}

	@Override
	public DataPointRuleSet getRuleSet()
	{
		return this;
	}

	@Override
	public RuleSetType getType()
	{
		return type;
	}

	@Override
	public List<Entry<VTLAlias, VTLAlias>> getVars()
	{
		return IntStream.range(0, cols.size()).mapToObj(i -> new SimpleEntry<>(cols.get(i), aliases.get(i))).collect(toList());
	}
	
	@Override
	public List<DataPointRule> getRules()
	{
		return rules;
	}
}
