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

import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns.parseString;
import static it.bancaditalia.oss.vtl.impl.types.data.date.VTLTimePatterns.parseTemporal;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.DATE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIME_PERIOD;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static java.util.Collections.singletonMap;
import static java.util.Locale.ENGLISH;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.time.LocalDate;
import java.util.Arrays;

import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.DateValue;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NumberValueImpl;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimePeriodValue;
import it.bancaditalia.oss.vtl.impl.types.data.date.PeriodHolder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.TimeDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class CastTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	
	private final Domains target;
	private final String mask;
	private final ThreadLocal<DecimalFormat> numberFormatter; 

	public CastTransformation(Transformation operand, Domains target, String mask)
	{
		super(operand);
		this.target = target;
		this.mask = mask != null ? mask.substring(1, mask.length() - 1) : "";
		numberFormatter = ThreadLocal.withInitial(() -> new DecimalFormat(this.mask, DecimalFormatSymbols.getInstance(ENGLISH)));
	}

	public CastTransformation(Transformation operand, String targetDomainName, String mask)
	{
		this(operand, Arrays.stream(Domains.values())
				.filter(domain -> domain.name().equalsIgnoreCase(targetDomainName))
				.findAny()
				.orElseThrow(() -> new UnsupportedOperationException("Cast with non-basic domain name '" + targetDomainName + "' not implemented")),
			mask);
	}

	@Override
	protected VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		return castScalar(scalar);
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata, TransformationScheme scheme)
	{
		DataStructureComponent<Measure, ?, ?> oldMeasure = dataset.getMetadata().getMeasures().iterator().next();
		if (target.getDomain() == oldMeasure.getVariable().getDomain())
			return dataset;
		
		DataStructureComponent<Measure, ?, ?> measure = target.getDomain().getDefaultVariable().as(Measure.class);
		DataSetMetadata structure = new DataStructureBuilder(dataset.getMetadata().getIDs())
				.addComponent(measure)
				.build();
		
		return dataset.mapKeepingKeys(structure, lineageEnricher(this), dp -> singletonMap(measure, castScalar(dp.get(oldMeasure))));
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme);
		ValueDomainSubset<?, ?> domain;
		
		if (!meta.isDataSet())
			domain = ((ScalarValueMetadata<?, ?>) meta).getDomain();
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;
			DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getSingleton(Measure.class);
			domain = measure.getVariable().getDomain();
		}

		if (domain == target.getDomain())
			return target;
		else if (domain instanceof StringDomainSubset && target == TIME_PERIOD)
			return target;
		else if (domain instanceof StringDomainSubset && target == INTEGER)
			return INTEGER;
		else if (domain instanceof StringDomainSubset && target == NUMBER)
			return NUMBER;
		else if (domain instanceof StringDomainSubset && target == DATE)
			return DATE;
		else if (domain instanceof TimeDomainSubset && target == STRING)
			return STRING;
		else if (domain instanceof NumberDomainSubset && target == INTEGER)
			return INTEGER;
		else if (domain instanceof NumberDomainSubset && target == STRING)
			return STRING;
		else
			throw new UnsupportedOperationException("cast " + domain + " => " + target.getDomain() + " not implemented ");
	}

	private DecimalFormat getNumberFormatter()
	{
		return numberFormatter.get();
	}

	private ScalarValue<?, ?, ?, ?> castScalar(ScalarValue<?, ?, ?, ?> scalar)
	{
		if (scalar.getDomain() == target.getDomain())
			return scalar;
		
		DecimalFormat formatter = getNumberFormatter();
		
		if (scalar.isNull())
			return target.getDomain().cast(scalar);
		else if (scalar instanceof StringValue && target == DATE)
			return DateValue.of(parseString(scalar.get().toString(), mask));
		else if (scalar instanceof StringValue && target == TIME_PERIOD)
			return TimePeriodValue.of(scalar.get().toString(), mask);
		else if (scalar instanceof DateValue && target == STRING)
			return StringValue.of(parseTemporal((LocalDate) scalar.get(), mask));
		else if (scalar instanceof TimePeriodValue && target == STRING)
			return StringValue.of(parseTemporal((PeriodHolder<?>) scalar.get(), mask));
		else if (scalar instanceof StringValue && target == INTEGER)
			return IntegerValue.of(Long.parseLong((String) scalar.get()));
		else if (scalar instanceof StringValue && target == NUMBER)
			return NumberValueImpl.createNumberValue((String) scalar.get());
		else if (scalar instanceof NumberValue && target == INTEGER)
			return IntegerValue.of(((Number) scalar.get()).longValue());
		else if (scalar instanceof IntegerValue && target == STRING)
			return StringValue.of(formatter.format(((Number) scalar.get()).longValue()));
		else if (scalar instanceof NumberValue && target == STRING)
			return StringValue.of(formatter.format(((Number) scalar.get()).doubleValue()));
		else
			throw new UnsupportedOperationException("cast " + scalar.getDomain() + " => " + target.getDomain() + " not implemented ");
	}
	
	@Override
	public String toString()
	{
		return "cast(" + operand + ", " + target + ", \"" + mask + "\")";
	}
}
