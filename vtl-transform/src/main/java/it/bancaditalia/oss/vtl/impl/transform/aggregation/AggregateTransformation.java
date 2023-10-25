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
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.AVG;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT_MEASURE;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.STDDEV_POP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.STDDEV_SAMP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.VAR_POP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.VAR_SAMP;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static java.util.stream.Collectors.toSet;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.GroupingClause;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
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
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class AggregateTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final AggregateOperator	aggregation;
	private final GroupingClause groupingClause;
	private final Transformation having;
	private final Class<? extends ComponentRole> role; 
	private final String name; 
	
	public AggregateTransformation(AggregateOperator aggregation, Transformation operand, GroupingClause groupingClause, Transformation having)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.groupingClause = groupingClause;
		this.having = having;
		this.role = null;
		this.name = null;

		if (this.having != null)
			throw new UnsupportedOperationException(aggregation + "(... having ...) not implemented");
	}

	// constructor for COUNT operator
	public AggregateTransformation(GroupingClause groupingClause, Transformation having)
	{
		this(COUNT, null, groupingClause, having);
	}
	
	// constructor for AGGR clause
	public AggregateTransformation(AggregateTransformation other, GroupingClause groupingClause, Class<? extends ComponentRole> role, String name)
	{
		super(other.operand);
		
		this.aggregation = other.aggregation;
		this.groupingClause = groupingClause;
		this.having = other.having;
		this.role = coalesce(role, Measure.class);
		this.name = name;

		if (this.having != null)
			throw new UnsupportedOperationException(aggregation + "(... having ...) not implemented");
	}
	
	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return Stream.of(scalar).collect(aggregation.getReducer());
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		SerCollector<DataPoint, ?, DataPoint> reducer = aggregation.getReducer(dataset.getMetadata().getMeasures());
		Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = groupingClause == null ? emptySet() : groupingClause.getGroupingComponents(dataset.getMetadata());
		DataStructureComponent<?, ?, ?> outputComponent;
		if (name != null)
			outputComponent = ((DataSetMetadata) metadata).getComponent(name).orElseThrow(() -> new VTLMissingComponentsException(name, dataset.getMetadata()));
		else
			outputComponent = dataset.getMetadata().getMeasures().iterator().next();
		
		// dataset-level aggregation
		DataSet aggr = dataset.aggr((DataSetMetadata) metadata, groupIDs, reducer, (dp, keyValues) -> {
			DataPointBuilder builder = new DataPointBuilder(keyValues);
			if (name == null)
				builder = builder.addAll(dp);
			else if (dp.size() == 1)
				builder = builder.add(outputComponent, dp.values().iterator().next());
			else
				throw new IllegalStateException();
			return builder.build(dp.getLineage(), (DataSetMetadata) metadata);
		});
		
		return aggr;
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session) ;
		
		if (opmeta instanceof ScalarValueMetadata && NUMBER.isAssignableFrom(((ScalarValueMetadata<?, ?>) opmeta).getDomain()))
			return NUMBER;
		else if (opmeta instanceof DataSetMetadata)
		{
			DataSetMetadata dataset = (DataSetMetadata) opmeta;
			Set<DataStructureComponent<Measure, ?, ?>> measures = dataset.getMeasures();

			if (groupingClause == null)
			{
				if (measures.isEmpty())
					throw new VTLExpectedComponentException(Measure.class, dataset);

				Set<DataStructureComponent<Measure, ?, ?>> newMeasures = measures;
				if (aggregation == COUNT)
					newMeasures = COUNT_MEASURE;
				else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(aggregation))
					newMeasures = newMeasures.stream()
						.map(c -> INTEGERDS.isAssignableFrom(c.getDomain()) ? DataStructureComponentImpl.of(c.getName(), Measure.class, NUMBERDS) : c)
						.collect(toSet());

				if (operand != null)
					return new DataStructureBuilder(newMeasures).build();

				if (measures.size() == 1)
					return measures.iterator().next().getMetadata();
				else
					return new DataStructureBuilder(newMeasures).build();
			}
			else
			{
				Set<DataStructureComponent<Identifier, ?, ?>> groupComps = groupingClause.getGroupingComponents(dataset);
				
				Optional<DataStructureComponent<Identifier, ?, ?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				DataStructureBuilder builder = new DataStructureBuilder(groupComps.stream().map(c -> c.asRole(Identifier.class)).collect(toSet()));
				
				Set<? extends DataStructureComponent<?, ?, ?>> newComps = dataset.getMeasures();
				if (aggregation == COUNT)
					newComps = COUNT_MEASURE;
				else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(aggregation))
					newComps = newComps.stream()
						.map(c -> INTEGERDS.isAssignableFrom(c.getDomain()) ? DataStructureComponentImpl.of(c.getName(), Measure.class, NUMBERDS) : c)
						.collect(toSet());
				
				if (name != null)
					if (measures.size() > 1)
						throw new VTLSingletonComponentRequiredException(Measure.class, newComps);
					else
						newComps = singleton(DataStructureComponentImpl.of(name, role, measures.iterator().next().getDomain()));

				builder = builder.addComponents(aggregation == COUNT ? AggregateOperator.COUNT_MEASURE : newComps);
				
				return builder.build();
			}
		}
		else
			throw new VTLInvalidParameterException(opmeta, DataSetMetadata.class, ScalarValueMetadata.class);
	}


	@Override
	public String toString()
	{
		return aggregation + "(" + operand + (groupingClause != null ? " " + groupingClause.toString() : " ") + ")";
	}
}
