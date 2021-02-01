/*
 * Copyright © 2020 Banca D'Italia
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
 */
package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.types.data.date.TimePatterns.parseString;
import static it.bancaditalia.oss.vtl.impl.types.data.date.TimePatterns.parseTemporal;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DAYS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static java.util.Collections.singletonMap;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.DateHolder;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.DateDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimePeriodDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CastTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	
	private final ValueDomainSubset<?> target;
	private final String mask;
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
		DataSetMetadata structure = new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponent(measure)
				.build();
		return dataset.mapKeepingKeys(structure, dp -> singletonMap(measure, castScalar(dp.get(oldMeasure))));
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session);
		ValueDomainSubset<?> domain;
		
		if (meta instanceof ScalarValueMetadata)
			domain = ((ScalarValueMetadata<?>) meta).getDomain();
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;
			
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, measures);
			
			DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
			
			// keep the ordering! DateDomain subclass of TimeDomain, IntegerDomain subclass of NumberDomain
			domain = measure.getDomain();
		}

		if (domain instanceof StringDomainSubset && target instanceof DateDomainSubset)
			return DATE;
		else if (domain instanceof StringDomainSubset && target instanceof TimePeriodDomainSubset)
			return DAYS;
		else if (domain instanceof StringDomainSubset && target instanceof IntegerDomainSubset)
			return INTEGER;
		else if (domain instanceof StringDomainSubset && target instanceof NumberDomainSubset)
			return NUMBER;
		else if (domain instanceof TimeDomainSubset && target instanceof StringDomainSubset)
			return STRING;
		else if (domain instanceof TimeDomainSubset && target instanceof TimeDomainSubset)
			return (ScalarValueMetadata<?>) () -> target;
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

	private ScalarValue<?, ?, ?> castScalar(ScalarValue<?, ?, ?> scalar)
	{
		try
		{
			if (scalar instanceof StringValue && target instanceof DateDomainSubset)
				return new DateValue(parseString(scalar.get().toString(), mask));
			else if (scalar instanceof StringValue && target instanceof TimePeriodDomainSubset)
				return new TimePeriodValue(parseString(scalar.get().toString(), mask));
			else if (scalar instanceof DateValue && target instanceof StringDomainSubset)
				return new StringValue(parseTemporal((DateHolder<?>) scalar.get(), mask));
			else if (scalar instanceof TimePeriodValue && target instanceof StringDomainSubset)
				return new StringValue(parseTemporal((PeriodHolder<?>) scalar.get(), mask));
			else if (scalar instanceof StringValue && target instanceof IntegerDomainSubset)
				return new IntegerValue(Long.parseLong((String) scalar.get()));
			else if (scalar instanceof StringValue && target instanceof NumberDomainSubset)
				return new DoubleValue(getNumberFormatter().parse((String) scalar.get()).doubleValue());
			else
				throw new UnsupportedOperationException(scalar.getClass() + " " + target.getClass() + " " + scalar);
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
