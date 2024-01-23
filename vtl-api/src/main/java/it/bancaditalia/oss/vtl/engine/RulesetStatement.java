package it.bancaditalia.oss.vtl.engine;

import it.bancaditalia.oss.vtl.model.rules.RuleSet;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public interface RulesetStatement extends DDLStatement
{
	public RuleSet getRuleSet(TransformationScheme scheme);
}
