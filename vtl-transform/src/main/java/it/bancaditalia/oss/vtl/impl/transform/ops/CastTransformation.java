/*******************************************************************************
 * Copyright 2020, Bank Of Italy
 *
 * Licensed under the EUPL, Version 1.2 (the "License");
 * You may not use this work except in compliance with the
 * License.
 * You may obtain a copy of the License at:
 *
 * https://joinup.ec.europa.eu/sites/default/files/custom-page/attachment/2020-03/EUPL-1.2%20EN.txt
 *
 * Unless required by applicable law or agreed to in
 * writing, software distributed under the License is
 * distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied.
 *
 * See the License for the specific language governing
 * permissions and limitations under the License.
 *******************************************************************************/
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static java.time.format.SignStyle.NOT_NEGATIVE;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.DAY_OF_WEEK;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.Collections.singletonMap;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CastTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final static Map<Pattern, UnaryOperator<DateTimeFormatterBuilder>> PATTERNS = new LinkedHashMap<>();
	
	static {
		PATTERNS.put(Pattern.compile("^(YYYY)(.*)"), b -> b.appendValue(YEAR, 4));
		PATTERNS.put(Pattern.compile("^(YYY)(.*)$"), b -> b.appendValue(YEAR, 3));
		PATTERNS.put(Pattern.compile("^(YY)(.*)$"), b -> b.appendValue(YEAR, 2));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]3)(.*)$"), b -> b.appendValue(MONTH_OF_YEAR, 3));
		PATTERNS.put(Pattern.compile("^(M[Oo][Nn][Tt][Hh]1)(.*)$"), b -> b.appendValue(MONTH_OF_YEAR, 5));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]3)(.*)$"), b -> b.appendValue(DAY_OF_WEEK, 3));
		PATTERNS.put(Pattern.compile("^(D[Aa][Yy]1)(.*)$"), b -> b.appendValue(DAY_OF_WEEK, 5));
		PATTERNS.put(Pattern.compile("^(MM)(.*)$"), b -> b.appendValue(MONTH_OF_YEAR, 2));
		PATTERNS.put(Pattern.compile("^(M)(.*)$"), b -> b.appendValue(MONTH_OF_YEAR, 1, 2, NOT_NEGATIVE));
		PATTERNS.put(Pattern.compile("^(DD)(.*)$"), b -> b.appendValue(DAY_OF_MONTH, 2));
		PATTERNS.put(Pattern.compile("^(D)(.*)$"), b -> b.appendValue(DAY_OF_MONTH, 1, 2, NOT_NEGATIVE));
		PATTERNS.put(Pattern.compile("^(-)(.*)$"), b -> b.appendLiteral("-"));
		PATTERNS.put(Pattern.compile("^(/)(.*)$"), b -> b.appendLiteral("/"));
		PATTERNS.put(Pattern.compile("^( )(.*)$"), b -> b.appendLiteral(" "));
	}
	
	private final ValueDomainSubset<?> target;
	private final String mask;
	private transient DateTimeFormatter dateTimeFormatter;
	private transient DecimalFormat numberFormatter;

	public CastTransformation(Transformation operand, Domains target, String mask)
	{
		super(operand);
		this.target = target.getDomain();
		this.mask = mask != null ? mask.substring(1, mask.length() - 1) : "";
	}

	public CastTransformation(Transformation operand, String targetDomainName, String mask)
	{
		super(null);
		throw new UnsupportedOperationException("cast with non-basic domain names not implemented");
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return castScalar(scalar);
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		DataStructureComponent<Measure, ?, ?> oldMeasure = dataset.getComponents(Measure.class).iterator().next();
		DataStructureComponent<Measure, ?, ?> measure = new DataStructureComponentImpl<>(target.getVarName(), Measure.class, target);
		VTLDataSetMetadata structure = new DataStructureImpl.Builder(dataset.getComponents(Identifier.class))
				.addComponent(measure)
				.build();
		return dataset.mapKeepingKeys(structure, dp -> singletonMap(measure, castScalar(dp.get(oldMeasure))));
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);
		ValueDomainSubset<?> domain;
		
		if (meta instanceof VTLScalarValueMetadata)
			domain = ((VTLScalarValueMetadata<?>) meta).getDomain();
		else
		{
			VTLDataSetMetadata dataset = (VTLDataSetMetadata) meta;
			
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, measures);
			
			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
			
			// keep the ordering! DateDomain subclass of TimeDomain, IntegerDomain subclass of NumberDomain
			domain = measure.getDomain();
		}

		if (domain instanceof StringDomainSubset && target instanceof DateDomainSubset)
			return DATE;
//			else if (scalarmeta.getDomain() instanceof StringDomainSubset && target instanceof TimeDomainSubset)
//				return TIME;
		else if (domain instanceof StringDomainSubset && target instanceof IntegerDomainSubset)
			return INTEGER;
		else if (domain instanceof StringDomainSubset && target instanceof NumberDomainSubset)
			return NUMBER;
		else if (domain instanceof TimeDomainSubset && target instanceof StringDomainSubset)
			return STRING;
		else if (domain instanceof TimeDomainSubset && target instanceof TimeDomainSubset)
			return (VTLScalarValueMetadata<?>) () -> target;
//			else if (scalarmeta.getDomain() instanceof TimeDomainSubset && target instanceof StringDomainSubset)
//				return STRING;
		else
			throw new UnsupportedOperationException();
	}

	private synchronized DecimalFormat getNumberFormatter()
	{
		if (numberFormatter != null)
			return numberFormatter;
		
		synchronized (this)
		{
			if (numberFormatter != null)
				return numberFormatter;
			
			return numberFormatter = new DecimalFormat(mask);
		}
	}

	private synchronized DateTimeFormatter getDateFormatter()
	{
		if (dateTimeFormatter != null)
			return dateTimeFormatter;
		
		synchronized (this)
		{
			if (dateTimeFormatter != null)
				return dateTimeFormatter;
			
			DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();
			String maskRemaining = mask;
			while (!maskRemaining.isEmpty())
			{
				boolean found = false;
				for (Pattern pattern: PATTERNS.keySet())
					if (!found)
					{
						Matcher matcher = pattern.matcher(maskRemaining);
						if (matcher.find())
						{
							builder = PATTERNS.get(pattern).apply(builder);
							maskRemaining = matcher.group(2);
							found = true;
						}
					}
				
				if (!found)
					throw new IllegalStateException("Unrecognized mask characters in cast operator: " + maskRemaining);
			}
			
			return dateTimeFormatter = builder.toFormatter();
		}
	}

	private ScalarValue<?, ?, ?> castScalar(ScalarValue<?, ?, ?> scalar)
	{
		try
		{
			if (scalar instanceof StringValue && target instanceof DateDomainSubset)
				return new DateValue(LocalDateTime.parse((String) scalar.get(), getDateFormatter()));
//				else if (scalar instanceof StringValue && target instanceof TimeDomainSubset)
//					return new TimeValue(LocalTime.parse((String) scalar.get(), mask));
			else if (scalar instanceof StringValue && target instanceof IntegerDomainSubset)
				return new IntegerValue(Long.parseLong((String) scalar.get()));
			else if (scalar instanceof StringValue && target instanceof NumberDomainSubset)
				return new DoubleValue(getNumberFormatter().parse((String) scalar.get()).doubleValue());
			else if (scalar instanceof TimeValue && target instanceof TimeDomainSubset)
				return target.cast(scalar);
			else if (scalar instanceof TimeValue && target instanceof StringDomainSubset)
				return new StringValue(getDateFormatter().format(((TimeValue<?, ?, ?>) scalar).get()));
//				else if (scalar instanceof TimeValueImpl && target instanceof StringDomainSubset)
//					return new StringValue(dateFormatter.format((LocalTime) scalar.get()));
			else
				throw new UnsupportedOperationException(scalar.getClass() + " " + target.getClass() + " " + scalar);
		}
		catch (DateTimeParseException e)
		{
			throw new VTLNestedException("Date/time '" + scalar.get() + "' unparseable with mask '" + getDateFormatter() + "'", e);
		}
		catch (ParseException e)
		{
			throw new VTLNestedException("Number '" + scalar.get() + "' unparseable with mask '" + getNumberFormatter() + "'", e);
		}
	}
	
	@Override
	public String toString()
	{
		return "cast(" + operand + ", " + target + ", \"" + mask + "\")";
	}
}
