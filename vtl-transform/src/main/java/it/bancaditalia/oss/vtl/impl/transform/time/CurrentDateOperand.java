package it.bancaditalia.oss.vtl.impl.transform.time;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYS;
import static java.time.LocalDate.now;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CurrentDateOperand extends TransformationImpl implements LeafTransformation
{
	private static final long serialVersionUID = 1L;

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		return new DateValue(now());
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme scheme)
	{
		return DAYS;
	}

	@Override
	public String toString()
	{
		return "current_date()";
	}
	
	@Override
	public String getText()
	{
		return toString();
	}
}
