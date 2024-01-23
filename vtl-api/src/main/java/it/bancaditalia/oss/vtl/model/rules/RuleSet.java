package it.bancaditalia.oss.vtl.model.rules;

public interface RuleSet
{
	enum RuleType
	{
		EQ("="), LT("<"), LE("<="), GT(">"), GE(">=");
		
		private final String toString;
	
		RuleType(String toString)
		{
			this.toString = toString;
		}
		
		@Override
		public String toString()
		{
			return toString;
		}
	}
}
