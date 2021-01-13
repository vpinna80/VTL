package it.bancaditalia.oss.vtl.eclipse.impl.markers;

import org.eclipse.jface.text.source.Annotation;

public class VTLSyntaxErrorAnnotation extends Annotation
{
	public static final String ANNOT_TYPE = "it.bancaditalia.oss.vtl.eclipse.annotation.syntaxerror";
	public static final String STRAT_TYPE = "it.bancaditalia.oss.vtl.eclipse.strategy.syntaxerror";
	
	private final int start;
	private final int stop;
	
	public VTLSyntaxErrorAnnotation(int start, int stop, String message)
	{
		super(ANNOT_TYPE, false, message);
		this.start = start < stop ? start : stop;
		this.stop = stop;
	}

	public int getStart()
	{
		return start;
	}

	public int getStop()
	{
		return stop;
	}
}