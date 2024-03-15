package it.bancaditalia.oss.vtl.impl.types.data;

import static java.lang.Boolean.TRUE;

import java.io.Serializable;
import java.util.List;
import java.util.Map.Entry;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.rules.DataPointRuleSet;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class DataPointRuleSetImpl implements DataPointRuleSet, Serializable
{
	private static final long serialVersionUID = 1L;
	
	private final RuleSetType type;
	private final List<DataPointRule> rules;
	private final List<Entry<String, String>> vars;
	
	public static class DataPointRuleImpl implements DataPointRule, Serializable
	{
		private final String ruleId;
		private final Transformation when;
		private final Transformation then;
		private final ScalarValue<?, ?, ?, ?> errorcode;
		private final ScalarValue<?, ?, ?, ?> errorlevel;

		public DataPointRuleImpl(String ruleId, Transformation when, Transformation then, ScalarValue<?, ?, ?, ?> errorcode, ScalarValue<?, ?, ?, ?> errorlevel)
		{
			this.ruleId = ruleId;
			this.when = when;
			this.then = then;
			this.errorcode = errorcode;
			this.errorlevel = errorlevel;
		}

		private static final long serialVersionUID = 1L;

		@Override
		public Boolean eval(DataPoint dp, TransformationScheme scheme)
		{
			Boolean pre = when != null ? (Boolean) ((BooleanValue<?>) when.eval(scheme)).get() : null;
			
			return pre == TRUE ? TRUE : (Boolean) ((BooleanValue<?>) then.eval(scheme)).get();
		}

		@Override
		public String getRuleId()
		{
			return ruleId;
		}

		@Override
		public ScalarValue<?, ?, ?, ?> getErrorCode()
		{
			return errorcode; 
		}

		@Override
		public ScalarValue<?, ?, ?, ?> getErrorLevel()
		{
			return errorlevel;
		}
	}
	
	public DataPointRuleSetImpl(RuleSetType type, List<Entry<String, String>> vars, List<DataPointRule> rules)
	{
		this.type = type;
		this.vars = vars;
		this.rules = rules;
	}

	@Override
	public RuleSetType getType()
	{
		return type;
	}

	@Override
	public List<Entry<String, String>> getVars()
	{
		return vars;
	}
	
	@Override
	public List<DataPointRule> getRules()
	{
		return rules;
	}
}
