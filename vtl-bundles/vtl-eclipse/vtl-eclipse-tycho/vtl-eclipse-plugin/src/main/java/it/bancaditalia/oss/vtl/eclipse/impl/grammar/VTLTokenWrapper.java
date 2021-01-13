package it.bancaditalia.oss.vtl.eclipse.impl.grammar;

import org.eclipse.jface.text.TextAttribute;
import org.eclipse.jface.text.rules.IToken;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.Display;

import it.bancaditalia.oss.vtl.grammar.VtlTokens;

public class VTLTokenWrapper implements IToken
{
	private static final TextAttribute BLUE = new TextAttribute(Display.getCurrent().getSystemColor(SWT.COLOR_BLUE));
	private static final TextAttribute RED = new TextAttribute(Display.getCurrent().getSystemColor(SWT.COLOR_RED), null, SWT.BOLD);
	private static final TextAttribute GREEN = new TextAttribute(Display.getCurrent().getSystemColor(SWT.COLOR_GREEN));
	private final int channel;
	private final int type;

	public VTLTokenWrapper(int channel, int type)
	{
		this.channel = channel;
		this.type = type;
	}

	@Override
	public boolean isUndefined()
	{
		return false;
	}

	@Override
	public boolean isWhitespace()
	{
		return channel == VtlTokens.HIDDEN;
	}

	@Override
	public boolean isEOF()
	{
		return type == VtlTokens.EOF;
	}

	@Override
	public boolean isOther()
	{
		return channel != VtlTokens.HIDDEN && type != VtlTokens.EOF;
	}

	@Override
	public Object getData()
	{
		if (isOther())
			switch (type)
			{
				case VtlTokens.ASSIGN:
				case VtlTokens.PLUS:
				case VtlTokens.MINUS:
				case VtlTokens.MUL:
				case VtlTokens.DIV:
					return RED;
				case VtlTokens.IDENTIFIER:
					return BLUE;
				case VtlTokens.ML_COMMENT:
				case VtlTokens.SL_COMMENT:
					return GREEN;
				default:
					return null;
			}
		else
			return null;
	}
}
