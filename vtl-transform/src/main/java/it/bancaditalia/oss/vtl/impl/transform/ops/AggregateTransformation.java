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

import static it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope.THIS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructure;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class AggregateTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponentImpl<Measure, IntegerDomainSubset, IntegerDomain> COUNT_MEASURE = new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS);

	private final AggregateOperator	aggregation;
	private final List<String> groupBy;
	private final Transformation having;

	private VTLValueMetadata metadata;

	public AggregateTransformation(AggregateOperator aggregation, Transformation operand, List<String> groupBy, Transformation having)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.groupBy = groupBy == null || groupBy.isEmpty() ? null : groupBy;
		this.having = having;

		if (this.having != null)
			throw new UnsupportedOperationException(aggregation + "(... having ...) not implemented");
	}

	public AggregateTransformation(List<String> groupBy, Transformation having)
	{
		super(null);

		this.aggregation = COUNT;
		this.groupBy = groupBy == null || groupBy.isEmpty() ? null : groupBy;
		this.having = having;

		if (this.having != null)
			throw new UnsupportedOperationException(aggregation + "(... having ...) not implemented");
	}
	
	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return Stream.of(scalar).collect(aggregation.getReducer());
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		Collector<? super ScalarValue<?, ?, ?>, ?, ? extends ScalarValue<?, ?, ?>> reducer = aggregation.getReducer();

		if (groupBy == null) // aggregation group is defined by the caller expression (typically AGGR clause)
			if (aggregation == COUNT)
				return dataset.stream().map(dp -> NullValue.instance(INTEGERDS)).collect(reducer);
			else
			{
				DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();
				return dataset.stream().map(d -> d.get(measure)).collect(reducer);
			}
		else
			if (aggregation == COUNT)
			{
				Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = groupBy.stream()
						.map(dataset::getComponent)
						.map(Optional::get)
						.map(c -> c.as(Identifier.class))
						.collect(toSet());
				
				// dataset-level aggregation
				return new LightFDataSet<>((VTLDataSetMetadata) metadata, ds -> {
					return ds.streamByKeys(groupIDs, (key, group) -> {
						return new DataPointBuilder(key)
								.add(COUNT_MEASURE, group.map(d -> NullValue.instance(INTEGERDS)).collect(reducer))
								.build((VTLDataSetMetadata) metadata);
					});
				}, dataset);
			}
			else
			{
				DataStructureComponent<? extends Measure, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();
				Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = groupBy.stream()
						.map(dataset::getComponent)
						.map(Optional::get)
						.map(c -> c.as(Identifier.class))
						.collect(toSet());
				
				// dataset-level aggregation
				return new LightFDataSet<>((VTLDataSetMetadata) metadata, ds -> ds.streamByKeys(groupIDs, (key, group) -> new DataPointBuilder(key)
						.add(measure, group.map(d -> d.get(measure)).collect(reducer))
						.build((VTLDataSetMetadata) metadata)), dataset);
			}
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		metadata = NUMBER;
		
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session) ;
		
		if (opmeta instanceof VTLScalarValueMetadata && ((VTLScalarValueMetadata<?>) opmeta).getDomain() instanceof NumberDomainSubset)
			return metadata;
		else// if (meta instanceof VTLDataSetMetadata)
		{
			VTLDataSetMetadata dataset = (VTLDataSetMetadata) opmeta;
			final Set<DataStructureComponent<Measure, ?, ?>> measures = dataset.getComponents(Measure.class);

			if (groupBy == null)
			{
				if (aggregation == COUNT)
					return metadata;
					
				if (measures.size() != 1)
					throw new VTLExpectedComponentException(Measure.class, measures);
				
				DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
				if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
					throw new VTLIncompatibleTypesException("Aggregation", NUMBERDS, measure.getDomain());
				
				return metadata;
			}
			else
			{
				Set<DataStructureComponent<?, ?, ?>> groupComps = groupBy.stream()
						.map(dataset::getComponent)
						.map(o -> o.orElseThrow(() -> new VTLMissingComponentsException((DataStructure) operand, groupBy.toArray(new String[0]))))
						.collect(toSet());
				
				Optional<DataStructureComponent<?, ?, ?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				Set<DataStructureComponent<Identifier, ?, ?>> keys = groupComps.stream().map(c -> c.as(Identifier.class)).collect(toSet());
				if (aggregation == COUNT && measures.isEmpty())
					measures.add(COUNT_MEASURE);
				
				metadata = new DataStructureBuilder().addComponents(keys).addComponents(measures).build();
				return metadata;
			}
		}
	}
	
	@Override
	public String toString()
	{
		return aggregation + "(" + operand + ")" + (groupBy != null ? groupBy.stream().collect(Collectors.joining(", ", " GROUP BY ", "")) : "");
	}
}
