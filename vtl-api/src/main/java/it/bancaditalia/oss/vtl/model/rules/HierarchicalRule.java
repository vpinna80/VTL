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

import java.util.Set;

import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

/**
 * Class that represent a single rule in a {@link HierarchicalRuleSet}.
 * 
 * @author Valentino Pinna
 */
public interface HierarchicalRule
{
	public VTLAlias getAlias();

	public String getLeftCodeItem();

	public Transformation getCondition();
	
	public Set<String> getRightCodeItems();
	
	public boolean isPlusSign(String item);
	
	public RuleType getRuleType();
	
	public ScalarValue<?, ?, ?, ?> getErrorCode();
	
	public ScalarValue<?, ?, ?, ?> getErrorLevel();

	public Transformation getRightCondition(CodeItem<?, ?, ?, ?> item);
}