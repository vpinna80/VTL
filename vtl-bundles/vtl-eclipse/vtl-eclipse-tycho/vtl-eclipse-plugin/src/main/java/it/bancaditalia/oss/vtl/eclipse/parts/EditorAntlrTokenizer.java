package it.bancaditalia.oss.vtl.eclipse.parts;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.eclipse.jface.text.BadLocationException;
import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.rules.IToken;
import org.eclipse.jface.text.rules.ITokenScanner;

import it.bancaditalia.oss.vtl.grammar.VtlTokens;

public class EditorAntlrTokenizer implements ITokenScanner
{
	private VtlTokens tokenizer;
	private transient Token currentToken;

	@Override
	public void setRange(IDocument document, int offset, int length)
	{
		try
		{
			tokenizer = new VtlTokens(CharStreams.fromString(document.get(offset, length)));
		} 
		catch (BadLocationException e)
		{
			e.printStackTrace();
		}
	}

	@Override
	public IToken nextToken()
	{
		return new VTLTokenWrapper(currentToken = tokenizer.nextToken());
	}

	@Override
	public int getTokenOffset()
	{
		return currentToken.getStartIndex();
	}

	@Override
	public int getTokenLength()
	{
		return currentToken.getStopIndex() - currentToken.getStartIndex();
	}
}
