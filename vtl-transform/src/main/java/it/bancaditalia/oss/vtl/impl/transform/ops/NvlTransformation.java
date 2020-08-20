package it.bancaditalia.oss.vtl.impl.transform.ops;

import it.bancaditalia.oss.vtl.impl.transform.bool.ConditionalTransformation;
import it.bancaditalia.oss.vtl.impl.transform.bool.IsNullTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;

public class NvlTransformation extends ConditionalTransformation
{
	private static final long serialVersionUID = 1L;
	
	public NvlTransformation(Transformation operand, Transformation defaultValue)
	{
		super(new IsNullTransformation(operand), defaultValue, operand);
	}
	
	@Override
	public String toString()
	{
		return "nvl(" + thenExpr + ", " + elseExpr + ")";
	}
}
