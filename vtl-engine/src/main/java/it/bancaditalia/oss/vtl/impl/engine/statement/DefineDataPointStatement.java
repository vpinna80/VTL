package it.bancaditalia.oss.vtl.impl.engine.statement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import it.bancaditalia.oss.vtl.engine.RulesetStatement;
import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.types.data.DataPointRuleSetImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DataPointRuleSetImpl.DataPointRuleImpl;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet.DataPointRule;
import it.bancaditalia.oss.vtl.model.rules.RuleSet.RuleSetType;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DefineDataPointStatement extends AbstractStatement implements RulesetStatement
{
	private static final long serialVersionUID = 1L;
	
	private final String alias;
	private final RuleSetType rulesetType;
	private final List<Entry<String, String>> vars;
	private final List<String> ruleIDs;
	private final List<Transformation> conds;
	private final List<Transformation> exprs;
	private final List<String> ercodes;
	private final List<Long> erlevels;

	public DefineDataPointStatement(String alias, RuleSetType rulesetType, List<Entry<String, String>> vars, List<String> ruleIDs, 
			List<Transformation> conds, List<Transformation> exprs, List<String> ercodes, List<Long> erlevels)
	{
		super(alias);
		
		this.alias = alias;
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
		
		Set<String> ruleNames = new HashSet<>();
		for (int i = 0; i < ruleIDs.size(); i++)
		{
			String ruleID = ruleIDs.get(i);
			if (ruleID == null)
				ruleID = String.format("%s_%d", alias, i + 1);
			if (ruleNames.contains(ruleID))
				throw new VTLException("Duplicated rule name in data point ruleset " + alias + ": " + ruleID);
			
			items.add(new DataPointRuleImpl(ruleID, conds.get(i), exprs.get(i), StringValue.of(ercodes.get(i)), IntegerValue.of(erlevels.get(i))));
		}
		
		return new DataPointRuleSetImpl(rulesetType, vars, items);
	}
}
