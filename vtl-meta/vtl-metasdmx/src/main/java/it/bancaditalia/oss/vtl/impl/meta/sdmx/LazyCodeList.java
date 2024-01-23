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
package it.bancaditalia.oss.vtl.impl.meta.sdmx;

import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType.EQ;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sdmx.api.sdmx.model.beans.codelist.CodeBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import io.sdmx.api.sdmx.model.beans.reference.StructureReferenceBean;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet.StringRule;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.util.SerFunction;

public class LazyCodeList extends StringCodeList implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(LazyCodeList.class);
	
	private final StringHierarchicalRuleSet defaultRuleSet;
	
	public LazyCodeList(StringDomainSubset<?> parent, StructureReferenceBean clRef, SDMXRepository fmrRepo)
	{
		super(parent, clRef.getAgencyId() + ":" + clRef.getMaintainableId() + "(" + clRef.getVersion() + ")");
		
		// retrieve codelist 
		String name = clRef.getAgencyId() + ":" + clRef.getMaintainableId() + "(" + clRef.getVersion() + ")";
		LOGGER.info("Fetching maintainable codelist {}", name);
		CodelistBean cl = fmrRepo.getBeanRetrievalManager().getIdentifiableBean(clRef, CodelistBean.class);

		// build hierarchy if present
		Map<StringCodeItem, List<StringCodeItem>> hierarchy = new HashMap<>();
		for (CodeBean codeBean: cl.getRootCodes())
			addChildren(cl, hierarchy, new StringCodeItem(codeBean.getId(), this));

		List<StringRule> rules = new ArrayList<>(); 
		for (StringCodeItem ruleComp: hierarchy.keySet())
			rules.add(new StringRule(ruleComp.get(), ruleComp, EQ, hierarchy.get(ruleComp).stream().collect(toMap(SerFunction.identity(), k -> TRUE)), null, null));
		
		defaultRuleSet = rules.isEmpty() ? null : new StringHierarchicalRuleSet(name, this, true, rules);

		hashCode = 31 + name.hashCode() + this.items.hashCode();
	}

	private void addChildren(CodelistBean cl, Map<StringCodeItem, List<StringCodeItem>> hierarchy, StringCodeItem parent)
	{
		items.add(parent);
		List<StringCodeItem> children = new ArrayList<>();
		for (CodeBean codeBean: cl.getChildren(parent.get()))
		{
			StringCodeItem child = new StringCodeItem(codeBean.getId(), this);
			children.add(child);
			addChildren(cl, hierarchy, child);
		}
		
		if (!children.isEmpty())
			hierarchy.put(parent, children);
	}

	public StringHierarchicalRuleSet getDefaultRuleSet()
	{
		return defaultRuleSet;
	}
}
