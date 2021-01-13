package it.bancaditalia.oss.vtl.eclipse.impl.grammar;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.eclipse.jface.text.DocumentEvent;
import org.eclipse.jface.text.IDocumentListener;
import org.eclipse.jface.text.Position;
import org.eclipse.jface.text.source.AnnotationModel;

import it.bancaditalia.oss.vtl.grammar.Vtl;
import it.bancaditalia.oss.vtl.grammar.VtlTokens;

public class VTLAntlrRuleScanner implements IDocumentListener
{
	private final AnnotationModel model;
	private final VTLSyntaxErrorListener errorListener;

	public VTLAntlrRuleScanner(AnnotationModel model)
	{
		this.model = model;
		errorListener = new VTLSyntaxErrorListener();
	}

	@Override
	public void documentAboutToBeChanged(DocumentEvent event)
	{

	}

	@Override
	public void documentChanged(DocumentEvent event)
	{
		model.removeAllAnnotations();
		errorListener.reset();
		String source = event.getDocument().get();
		Vtl parser = new Vtl(new CommonTokenStream(new VtlTokens(CharStreams.fromString(source))));
		parser.removeErrorListeners();
		parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
		parser.addErrorListener(errorListener);
		parser.start();

		int offset = 0;
		int length = 0;
		for (var syntaxError : errorListener.getSyntaxErrors())
		{
			if (syntaxError.getStart() < offset + length)
				throw new NullPointerException();
			
			offset = syntaxError.getStart();
			length = syntaxError.getStop() - syntaxError.getStart() + 1;
			
			if (length <= 0)
				throw new NullPointerException();
			
			if (source.substring(offset, offset + length - 1).contains("\n"))
				throw new NullPointerException();
			
			Position position = new Position(offset, length);
			model.addAnnotation(syntaxError, position);
		}
	}
}
