package it.bancaditalia.oss.vtl.eclipse.impl.grammar;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.stream.IntStream;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

import it.bancaditalia.oss.vtl.eclipse.impl.markers.VTLSyntaxErrorAnnotation;
import it.bancaditalia.oss.vtl.grammar.VtlTokens;

public class VTLSyntaxErrorListener extends BaseErrorListener
{
	private final List<VTLSyntaxErrorAnnotation> syntaxErrors = new ArrayList<>();
	
	public void reset()
	{
		syntaxErrors.clear();
	}
	
	@Override
	public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e)
	{
		if (e instanceof NoViableAltException)
		{
			Token token = ((NoViableAltException) e).getStartToken();
			syntaxErrors.add(new VTLSyntaxErrorAnnotation(token.getStartIndex(), token.getStopIndex(), msg));
		}
		else if (e instanceof InputMismatchException)
		{
			Token token = (Token) offendingSymbol;
			syntaxErrors.add(new VTLSyntaxErrorAnnotation(token.getStartIndex(), token.getStopIndex(), msg));
		}
		else if (e != null)
			System.err.println(e.getClass().getSimpleName() + ": " + msg);
		else if (recognizer instanceof Parser && !((Parser) recognizer).getExpectedTokens().isNil())
		{
			String tokens = ((Parser) recognizer).getExpectedTokens().getIntervals().stream()
				.flatMapToInt(interval -> IntStream.rangeClosed(interval.a, interval.b))
				.mapToObj(VtlTokens.VOCABULARY::getSymbolicName)
				.collect(joining());
			System.out.println(tokens);
		}
		else
			System.err.println(msg);
	}
	
	@Override
	public void reportAmbiguity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, boolean exact, BitSet ambigAlts, ATNConfigSet configs)
	{
		super.reportAmbiguity(recognizer, dfa, startIndex, stopIndex, exact, ambigAlts, configs);
	}
	
	@Override
	public void reportAttemptingFullContext(Parser recognizer, DFA dfa, int startIndex, int stopIndex, BitSet conflictingAlts, ATNConfigSet configs)
	{
		super.reportAttemptingFullContext(recognizer, dfa, startIndex, stopIndex, conflictingAlts, configs);
	}
	
	@Override
	public void reportContextSensitivity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, int prediction, ATNConfigSet configs)
	{
		super.reportContextSensitivity(recognizer, dfa, startIndex, stopIndex, prediction, configs);
	}
	
	public List<VTLSyntaxErrorAnnotation> getSyntaxErrors()
	{
		return syntaxErrors;
	}
}
