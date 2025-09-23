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
import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.sdmx.api.sdmx.model.beans.codelist.CodeBean;
import io.sdmx.api.sdmx.model.beans.codelist.CodelistBean;
import io.sdmx.api.sdmx.model.beans.reference.MaintainableRefBean;
import it.bancaditalia.oss.vtl.impl.types.domain.StringCodeList;
import it.bancaditalia.oss.vtl.impl.types.names.SDMXAlias;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.HierarchicalRuleImpl;
import it.bancaditalia.oss.vtl.impl.types.statement.HierarchicalRuleSetImpl;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRule;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;

public class SdmxCodeList extends StringCodeList implements Serializable
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(SdmxCodeList.class);
	
	private final AtomicReference<HierarchicalRuleSet> defaultRuleSet = new AtomicReference<>();
	private final String endpoint;
	private final MaintainableRefBean clRef;

	private volatile boolean inited = false;

	public SdmxCodeList(String endpoint, CodelistBean codelist)
	{
		super(STRINGDS, new SDMXAlias(codelist.getAgencyId(), codelist.getId(), codelist.getVersion()));
		
		this.endpoint = null;
		this.clRef = null;
		this.inited = true;
		
		init(codelist);
	}

	public SdmxCodeList(String endpoint, MaintainableRefBean clRef, SDMXAlias alias)
	{
		super(STRINGDS, alias);
		
		requireNonNull(alias.getAgency());
		requireNonNull(alias.getVersion());
		
		this.endpoint = endpoint;
		this.clRef = clRef;
	}

	public HierarchicalRuleSet getDefaultRuleSet()
	{
		// This will trigger the lazy initialization
		getCodeItems();

		return defaultRuleSet.get();
	}
	
	@Override
	public synchronized Set<StringCodeItem> getCodeItems()
	{
		if (inited)
			return items;
		
		synchronized (items)
		{
			if (!inited)
				init(SDMXRepository.fetchCodelist(endpoint, clRef));
			
			return items;
		}
	}

	private void init(CodelistBean codelist)
	{
		// build hierarchy if present
		Map<StringCodeItem, List<StringCodeItem>> hierarchy = new HashMap<>();

		// recursively resolve code hierarchies
		for (CodeBean codeBean: codelist.getRootCodes())
			addChildren(codelist, hierarchy, new StringCodeItem(codeBean.getId(), this));

		// build the ruleset
		List<HierarchicalRule> rules = new ArrayList<>();
		int ruleOrder = 1;
		for (StringCodeItem ruleComp: hierarchy.keySet())
		{
			VTLAlias ruleName = VTLAliasImpl.of(true, ruleComp.get());
			if (ruleName == null)
				ruleName = VTLAliasImpl.of(true, "" + ruleOrder);
			List<String> list = hierarchy.get(ruleComp).stream().map(StringCodeItem::get).collect(toList());
			rules.add(new HierarchicalRuleImpl(ruleName, null, ruleComp.get(), EQ, list, nCopies(list.size(), PLUS), List.of(), null, null));
			LOGGER.trace("Created hierarchy rule {} for codelist {}", ruleComp, getAlias());
			ruleOrder++;
		}
		
		if (!rules.isEmpty())
			defaultRuleSet.set(new HierarchicalRuleSetImpl(alias, VALUE_DOMAIN, alias, List.of(), rules));
		if (defaultRuleSet.get() != null)
			LOGGER.debug("Created default hierarchy ruleset for codelist {}", alias);
		
		inited = true;
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
}
