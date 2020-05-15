/*******************************************************************************
 * Copyright 2020, Bank Of Italy
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
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static java.util.Collections.emptySet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.types.data.ValueDomainHierarchyImpl;
import it.bancaditalia.oss.vtl.impl.types.data.VariableHierarchyImpl;
import it.bancaditalia.oss.vtl.model.data.Hierarchy;
import it.bancaditalia.oss.vtl.model.data.Hierarchy.RuleItem;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

class DefineHierarchyStatement extends AbstractStatement
{
	private static final long serialVersionUID = 1L;

	private final Hierarchy hierarchy;

	public DefineHierarchyStatement(String id, boolean isValueDomain, Map<String, String> conditions, List<? extends RuleItem> rules)
	{
		super(id);
		this.hierarchy = isValueDomain ? new ValueDomainHierarchyImpl(id, rules, conditions) : new VariableHierarchyImpl(id, rules, conditions);
	}

	@Override
	public Hierarchy eval(TransformationScheme session)
	{
		return hierarchy;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return emptySet();
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		return hierarchy.getMetadata();
	}
	
	@Override
	public String toString()
	{
		return "DEFINE HIERARCHICAL RULESET " + getId() + "(VARIABLE RULE " + hierarchy.getName() + ")";
	}
}
