package it.bancaditalia.oss.vtl.eclipse.impl.grammar;

import static java.lang.Math.max;

import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.Token;
import org.eclipse.jface.text.IDocument;
import org.eclipse.jface.text.rules.IToken;
import org.eclipse.jface.text.rules.ITokenScanner;

import it.bancaditalia.oss.vtl.grammar.VtlTokens;

public class VTLAntlrTokenizer implements ITokenScanner
{
	private VtlTokens tokenizer;
	private transient Token currentToken;
	private int offset;
	private int length;

	@Override
	public void setRange(IDocument document, int offset, int length)
	{
		this.offset = offset;
		this.length = length;
		tokenizer = new VtlTokens(CharStreams.fromString(document.get()));
	}

	@Override
	public IToken nextToken()
	{
		currentToken = tokenizer.nextToken();
		while (currentToken.getType() != VtlTokens.EOF && isInside())
			currentToken = tokenizer.nextToken();

		return new VTLTokenWrapper(currentToken.getChannel(), currentToken.getType());
	}

	private boolean isInside()
	{
		return currentToken.getStopIndex() < offset || currentToken.getStartIndex() > offset + length;
	}

	@Override
	public int getTokenOffset()
	{
		// Token may start before the offset of scanned range
		return max(currentToken.getStartIndex(), offset);
	}

	@Override
	public int getTokenLength()
	{
		if (currentToken.getType() == VtlTokens.EOF)
			return 0;
		
		// skip characters in token before the scanned range
		int skipped = offset - currentToken.getStartIndex();
		if (skipped < 0)
			skipped = 0;
		int tokenLength = currentToken.getStopIndex() - currentToken.getStartIndex() + 1 - skipped;
		return max(tokenLength, 0);
	}
}
