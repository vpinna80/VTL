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
import static it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.HierarchicalRuleSign.PLUS;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType.VALUE_DOMAIN;
import static it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleType.EQ;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static java.util.Collections.nCopies;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sdmx.api.sdmx.model.beans.codelist.CodeBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.HierarchicalRuleImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.HierarchicalRuleSetImpl;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;

public class SdmxCodeList extends StringCodeList implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SdmxCodeList.class);
	
	private final HierarchicalRuleSetImpl defaultRuleSet;
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

		List<HierarchicalRule> rules = new ArrayList<>();
		for (StringCodeItem ruleComp: hierarchy.keySet())
		{
			VTLAlias ruleName = VTLAliasImpl.of(true, ruleComp.get());
			List<String> list = hierarchy.get(ruleComp).stream().map(StringCodeItem::get).collect(toList());
			rules.add(new HierarchicalRuleImpl(ruleName, null, ruleComp.get(), EQ, list, nCopies(list.size(), PLUS), List.of(), null, null));
			LOGGER.trace("Created hierarchy rule {} for codelist {}", ruleComp, getAlias());
		}
		
		defaultRuleSet = rules.isEmpty() ? null : new HierarchicalRuleSetImpl(clAlias, VALUE_DOMAIN, clAlias, List.of(), rules);
		if (defaultRuleSet != null)
			LOGGER.debug("Created default hierarchy ruleset for codelist {}", getAlias());

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

	public HierarchicalRuleSetImpl getDefaultRuleSet()
	{
		return defaultRuleSet;
	}
	
	public String getAgency()
	{
		return agency;
	}
}
