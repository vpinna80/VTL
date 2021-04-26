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
package it.bancaditalia.oss.vtl.impl.transform.aggregation;

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.summingDouble;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class RatioToReportTransformation extends UnaryTransformation implements AnalyticTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(RatioToReportTransformation.class);

	private final List<String> partitionBy;

	private transient DataSetMetadata metadata;
	
	public RatioToReportTransformation(Transformation operand, List<String> partitionBy)
	{
		super(operand);

		this.partitionBy = coalesce(partitionBy, emptyList());
	}

	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar)
	{
		throw new UnsupportedOperationException();
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> partitionIDs;
		partitionIDs = partitionBy.stream()
			.map(dataset::getComponent)
			.map(Optional::get)
			.map(c -> c.as(Identifier.class))
			.collect(toSet());
		
		// The measures to aggregate
		dataset.getComponents(Measure.class)
			.stream()
			.filter(c -> !NUMBER.isAssignableFrom(c.getDomain()))
			.findAny()
			.ifPresent(c -> { throw new VTLIncompatibleTypesException("ratio_to_report", c, NUMBERDS); });
		
		return new LightFDataSet<>((DataSetMetadata) metadata, ds -> ds.streamByKeys(
				partitionIDs, collectingAndThen(toSet(), this::ratioToReportByPartition)
			).reduce(Stream::concat)
			.orElse(Stream.empty()), dataset);
	}

	private Stream<DataPoint> ratioToReportByPartition(Set<DataPoint> partition)
	{
		Map<DataStructureComponent<Measure, ?, ?>, Double> measureSums = Utils.getStream(metadata.getComponents(Measure.class))
		.map(m -> Utils.getStream(partition)
			.map(dp -> dp.get(m))
			.filter(NumberValue.class::isInstance)
			.map(ScalarValue::get)
			.map(Number.class::cast)
			.map(Number::doubleValue)
			.map(Utils.toEntryWithKey(v -> m))
		).reduce(Stream::concat)
		.orElse(Stream.empty())
		.collect(groupingByConcurrent(Entry::getKey, summingDouble(e -> e.getValue())));
	
		return Utils.getStream(partition)
			.map(dp -> Utils.getStream(measureSums.entrySet())
				.map(keepingKey((m, v) -> dp.get(m) instanceof NullValue ? null : ((Number) dp.get(m).get()).doubleValue() / v))
				.map(keepingKey((m, v) -> (ScalarValue<?, ?, ?, ?>)(v == null ? NullValue.instanceFrom(m) : DoubleValue.of(v))))
				.collect(toDataPoint(metadata, dp.getValues(Identifier.class)))
			);
	}

	@Override
	public DataSetMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session);
		
		if (opmeta instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class);
		
		DataSetMetadata dataset = (DataSetMetadata) opmeta;
		
		partitionBy.stream()
			.map(toEntryWithValue(dataset::getComponent))
			.map(e -> e.getValue().orElseThrow(() -> new VTLMissingComponentsException(e.getKey(), dataset)))
			.peek(c -> { if (!c.is(Identifier.class)) throw new VTLIncompatibleRolesException("partition by", c, Identifier.class); })
			.map(c -> c.as(Identifier.class))
			.collect(toSet());
		
		return metadata = new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponents(dataset.getComponents(Measure.class))
				.build();
	}
	
	@Override
	public String toString()
	{
		return "ratio_to_report(" + operand + " over (" 
				+ (partitionBy != null ? partitionBy.stream().collect(joining(", ", " partition by ", " ")) : "")
				+ ")";
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((partitionBy == null) ? 0 : partitionBy.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof RatioToReportTransformation)) return false;
		RatioToReportTransformation other = (RatioToReportTransformation) obj;
		if (partitionBy == null)
		{
			if (other.partitionBy != null) return false;
		}
		else if (!partitionBy.equals(other.partitionBy)) return false;
		return true;
	}
}
