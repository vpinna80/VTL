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

import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORCODE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORLEVEL;
import static it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.HierarchicalRuleSign.MINUS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.stream.Collectors.joining;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.HierarchicalRuleSign;
import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class HierarchicalRuleImpl implements HierarchicalRule
{
	private final VTLAlias name;
	private final Transformation when;
	private final String leftCodeItem;
	private final RuleType operator;
	private final Map<String, Boolean> rightCodes = new HashMap<>();
	private final Map<String, Transformation> rightConds = new HashMap<>();
	private final ScalarValue<?, ?, ?, ?> errorCode;
	private final ScalarValue<?, ?, ?, ?> errorLevel;

	public HierarchicalRuleImpl(VTLAlias name, Transformation when, String leftCodeItem, RuleType operator, List<String> rightCodes, 
			List<HierarchicalRuleSign> signs, List<Transformation> rightConds, ScalarValue<?, ?, ?, ?> errorCode, ScalarValue<?, ?, ?, ?> errorLevel)
	{
		this.name = name;
		this.when = when;
		this.leftCodeItem = leftCodeItem;
		this.operator = operator;
		this.errorCode = coalesce(errorCode, NullValue.instance(ERRORCODE.get().getVariable().getDomain()));
		this.errorLevel = coalesce(errorLevel, NullValue.instance(ERRORLEVEL.get().getVariable().getDomain()));
		
		for (int i = 0; i < rightCodes.size(); i++)
		{
			this.rightCodes.put(rightCodes.get(i), signs.get(i) != MINUS);
			
			if (rightConds.get(i) != null)
				this.rightConds.put(rightCodes.get(i), rightConds.get(i));
		}
	}

	@Override
	public VTLAlias getAlias()
	{
		return name;
	}
	
	@Override
	public Transformation getCondition()
	{
		return when;
	}
	
	@Override
	public String getLeftCodeItem()
	{
		return leftCodeItem;
	}

	@Override
	public Set<String> getRightCodeItems()
	{
		return rightCodes.keySet();
	}

	@Override
	public boolean isPlusSign(String item)
	{
		return rightCodes.get(item);
	}

	@Override
	public RuleSet.RuleType getRuleType()
	{
		return operator;
	}
	
	@Override
	public String toString()
	{
		return leftCodeItem + " " + operator + rightCodes.entrySet().stream().map(e -> (e.getValue() ? " + " : " - ") + e.getKey()).collect(joining());
	}

	public ScalarValue<?, ?, ?, ?> getErrorCode()
	{
		return errorCode;
	}

	public ScalarValue<?, ?, ?, ?> getErrorLevel()
	{
		return errorLevel;
	}

	@Override
	public Transformation getRightCondition(CodeItem<?, ?, ?, ?> item)
	{
		return rightConds.get(item.get().toString());
	}
}