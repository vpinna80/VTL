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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType.EQ;
import static it.bancaditalia.oss.vtl.util.SerFunction.identity;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.toMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.sdmx.api.sdmx.model.beans.codelist.CodeBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet;
import it.bancaditalia.oss.vtl.impl.types.data.StringHierarchicalRuleSet.StringRule;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;

public class SdmxCodeList extends StringCodeList implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final StringHierarchicalRuleSet defaultRuleSet;
	private final String agency;
	
	public SdmxCodeList(CodelistBean codelist)
	{
		super(STRINGDS, VTLAliasImpl.of(true, codelist.getAgencyId() + ":" + codelist.getId() + "(" + codelist.getVersion() + ")"));
		
		this.agency = codelist.getAgencyId();
		
		// retrieve codelist 
		VTLAlias clAlias = VTLAliasImpl.of(true, codelist.getAgencyId() + ":" + codelist.getId() + "(" + codelist.getVersion() + ")");

		// build hierarchy if present
		Map<StringCodeItem, List<StringCodeItem>> hierarchy = new HashMap<>();
		for (CodeBean codeBean: codelist.getRootCodes())
			addChildren(codelist, hierarchy, new StringCodeItem(codeBean.getId(), this));

		List<StringRule> rules = new ArrayList<>(); 
		for (StringCodeItem ruleComp: hierarchy.keySet())
			rules.add(new StringRule(VTLAliasImpl.of(ruleComp.get()), ruleComp, EQ, null, hierarchy.get(ruleComp).stream().collect(toMap(identity(), k -> TRUE)), null, null));
		
		defaultRuleSet = rules.isEmpty() ? null : new StringHierarchicalRuleSet(clAlias, clAlias, this, VALUE_DOMAIN, rules);

		hashCode = 31 + clAlias.hashCode() + this.items.hashCode();
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
	
	public String getAgency()
	{
		return agency;
	}
}
