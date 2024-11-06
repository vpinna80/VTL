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
package it.bancaditalia.oss.vtl.impl.engine.statement;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.data.DataPointRuleSetImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DataPointRuleSetImpl.DataPointRuleImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.names.VTLAliasImpl;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet.DataPointRule;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DefineDataPointStatement extends AbstractStatement implements RulesetStatement
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(DefineDataPointStatement.class);
	
	private final RuleSetType rulesetType;
	private final List<Entry<VTLAlias, VTLAlias>> vars;
	private final List<VTLAlias> ruleIDs;
	private final List<Transformation> conds;
	private final List<Transformation> exprs;
	private final List<String> ercodes;
	private final List<Long> erlevels;

	public DefineDataPointStatement(VTLAlias alias, RuleSetType rulesetType, List<Entry<VTLAlias, VTLAlias>> vars, List<VTLAlias> ruleIDs, 
			List<Transformation> conds, List<Transformation> exprs, List<String> ercodes, List<Long> erlevels)
	{
		super(alias);
		
		this.rulesetType = rulesetType;
		this.vars = vars;
		
		this.ruleIDs = ruleIDs;
		this.conds = conds;
		this.exprs = exprs;
		this.ercodes = ercodes;
		this.erlevels = erlevels;
	}

	@Override
	public DataPointRuleSet getRuleSet(TransformationScheme scheme)
	{
		List<DataPointRule> items = new ArrayList<>();
		
		Set<VTLAlias> ruleNames = new HashSet<>();
		for (int i = 0; i < ruleIDs.size(); i++)
		{
			VTLAlias ruleID = ruleIDs.get(i);
			if (ruleID == null)
				ruleID = VTLAliasImpl.of(String.format("%d", i + 1));
			if (ruleNames.contains(ruleID))
				throw new VTLException("Duplicated rule name in data point ruleset " + getAlias() + ": " + ruleID);
			
			ScalarValue<?, ?, ? extends StringDomainSubset<?>, StringDomain> ercode = ercodes.get(i) == null ? NullValue.instance(STRINGDS) : StringValue.of(ercodes.get(i).substring(1, ercodes.get(i).length() - 1));
			ScalarValue<?, ?, ? extends IntegerDomainSubset<?>, IntegerDomain> erlevel = erlevels.get(i) == null ? NullValue.instance(INTEGERDS) : IntegerValue.of(erlevels.get(i));
			
			DataPointRuleImpl ruledef = new DataPointRuleImpl(ruleID, conds.get(i), exprs.get(i), ercode, erlevel);
			LOGGER.debug("Added datapoint rule {}", ruledef);
			items.add(ruledef);
		}
		
		return new DataPointRuleSetImpl(rulesetType, vars, items);
	}
}
