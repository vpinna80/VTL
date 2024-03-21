package it.bancaditalia.oss.vtl.impl.types.domain;

import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;

public abstract class NonNullDomainSubset<S extends ValueDomainSubset<S, D>, D extends ValueDomain> implements ValueDomainSubset<NonNullDomainSubset<S, D>, D>  
{
	private static final long serialVersionUID = 1L;
	
	

}
