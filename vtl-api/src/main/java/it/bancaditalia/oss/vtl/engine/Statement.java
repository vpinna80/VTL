package it.bancaditalia.oss.vtl.engine;

public interface Statement
{
	/**
	 * @return The id of the statement, like the rule name at the left side of an assignment statement. 
	 */
	public String getAlias();

	/**
	 * @return true if it makes sense to cache the result of evaluating this statement.
	 */
	public boolean isCacheable();
}
