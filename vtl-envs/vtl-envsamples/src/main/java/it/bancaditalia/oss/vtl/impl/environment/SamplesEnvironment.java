package it.bancaditalia.oss.vtl.impl.environment;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.environment.Environment;
import it.bancaditalia.oss.vtl.impl.data.samples.SampleDataSets;
import it.bancaditalia.oss.vtl.model.data.VTLValue;

public class SamplesEnvironment implements Environment
{
	private static final Pattern SAMPLE_PATTERN = Pattern.compile("sample(1[0-7]|[1-9])");
	@Override
	public boolean contains(String alias)
	{
		return getValue(alias).isPresent();
	}

	@Override
	public Optional<VTLValue> getValue(String alias)
	{
		Matcher matcher = SAMPLE_PATTERN.matcher(alias);
		return matcher.matches() ? Optional.of(SampleDataSets.valueOf("SAMPLE" + matcher.group(1))) : Optional.empty();
	}
}
