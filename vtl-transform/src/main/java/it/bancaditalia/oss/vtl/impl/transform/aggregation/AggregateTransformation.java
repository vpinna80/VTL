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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT;
import static it.bancaditalia.oss.vtl.util.Utils.afterMapping;
import static java.util.stream.Collectors.toSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class AggregateTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponentImpl<Measure, IntegerDomainSubset, IntegerDomain> COUNT_MEASURE = new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS);

	private final AggregateOperator	aggregation;
	private final List<String> groupBy;
	private final Transformation having;
	private final String name;
	private final Class<? extends ComponentRole> role;

	private transient VTLValueMetadata metadata;
	
	public AggregateTransformation(AggregateOperator aggregation, Transformation operand, List<String> groupBy, Transformation having)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.groupBy = groupBy == null || groupBy.isEmpty() ? null : groupBy;
		this.having = having;
		this.name = null;
		this.role = null;

		if (this.having != null)
			throw new UnsupportedOperationException(aggregation + "(... having ...) not implemented");
	}

	// constructor for COUNT operator
	public AggregateTransformation(List<String> groupBy, Transformation having)
	{
		this(COUNT, null, groupBy, having);
	}
	
	// constructor for AGGR clause
	public AggregateTransformation(AggregateTransformation other, List<String> groupBy, String name, Class<? extends ComponentRole> role)
	{
		super(other.operand);
		
		this.aggregation = other.aggregation;
		this.groupBy = groupBy == null || groupBy.isEmpty() ? null : groupBy;
		this.having = null;
		this.name = name;
		this.role = role;
	}
	
	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?> scalar)
	{
		return Stream.of(scalar).collect(aggregation.getReducer());
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset)
	{
		DataStructureComponent<? extends Measure, ?, ?> sourceMeasure = aggregation == COUNT ? COUNT_MEASURE : dataset.getComponents(Measure.class).iterator().next();
		Collector<DataPoint, ?, ScalarValue<?, ?, ?>> reducer = aggregation.getReducer(sourceMeasure);
		DataStructureComponent<?, ?, ?> resultComponent = name != null ? role != null
				? new DataStructureComponentImpl<>(name, role, sourceMeasure.getDomain())
				: new DataStructureComponentImpl<>(name, sourceMeasure.getRole(), sourceMeasure.getDomain())
				: sourceMeasure;

		if (groupBy == null)
			try (Stream<DataPoint> stream = dataset.stream())
			{
				return stream.collect(reducer);
			}
		else
		{
			Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = getGroupByComponents(dataset.getMetadata());
			
			// dataset-level aggregation
			return new LightFDataSet<>((DataSetMetadata) metadata, ds -> ds.streamByKeys(groupIDs, reducer, (v, keyValues) -> 
					new DataPointBuilder(keyValues)
						.add(resultComponent, v)
						.build((DataSetMetadata) metadata)), dataset);
		}
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session) ;
		
		if (opmeta instanceof ScalarValueMetadata && NUMBER.isAssignableFrom(((ScalarValueMetadata<?>) opmeta).getDomain()))
			return metadata = NUMBER;
		else if (opmeta instanceof DataSetMetadata)
		{
			DataSetMetadata dataset = (DataSetMetadata) opmeta;
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
				Set<DataStructureComponent<Identifier,?,?>> groupComps = getGroupByComponents(dataset);
				
				Optional<DataStructureComponent<Identifier,?,?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				Set<DataStructureComponent<Identifier, ?, ?>> keys = groupComps.stream().map(c -> c.as(Identifier.class)).collect(toSet());
				if (aggregation == COUNT && measures.isEmpty())
					measures.add(COUNT_MEASURE);

				DataStructureComponent<? extends Measure, ?, ?> sourceMeasure = aggregation == COUNT ? COUNT_MEASURE : dataset.getComponents(Measure.class).iterator().next();
				DataStructureComponent<?, ?, ?> resultComponent = name != null ? role != null
						? new DataStructureComponentImpl<>(name, role, sourceMeasure.getDomain())
						: new DataStructureComponentImpl<>(name, sourceMeasure.getRole(), sourceMeasure.getDomain())
						: sourceMeasure;
				
				return metadata = new DataStructureBuilder().addComponents(keys).addComponents(resultComponent).build();
			}
		}
		else
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class, ScalarValueMetadata.class);
	}

	private Set<DataStructureComponent<Identifier, ?, ?>> getGroupByComponents(DataSetMetadata dataset)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> groupComps = groupBy.stream()
				.map(name -> name.matches("'.*'")
						? dataset.getComponent(name.replaceAll("'(.*)'", "$1"))
						: dataset.stream().filter(afterMapping(DataStructureComponent::getName, name::equalsIgnoreCase)).findAny()
				).map(o -> o.orElseThrow(() -> new VTLMissingComponentsException(dataset, groupBy.toArray(new String[0]))))
				.peek(component -> {
					if (!component.is(Identifier.class))
						throw new VTLIncompatibleRolesException("aggregation group by", component, Identifier.class);
				}).map(component -> component.as(Identifier.class))
				.collect(toSet());
		return groupComps;
	}
	
	@Override
	public String toString()
	{
		return aggregation + "(" + operand + ")" + (groupBy != null ? groupBy.stream().collect(Collectors.joining(", ", " GROUP BY ", "")) : "");
	}
}
