package it.bancaditalia.oss.vtl.eclipse.impl.markers;

import org.eclipse.jface.text.rules.DefaultDamagerRepairer;

import it.bancaditalia.oss.vtl.eclipse.impl.grammar.VTLAntlrTokenizer;

public class VTLDocumentRepairer extends DefaultDamagerRepairer
{
	public VTLDocumentRepairer()
	{
		super(new VTLAntlrTokenizer());
	}
}
