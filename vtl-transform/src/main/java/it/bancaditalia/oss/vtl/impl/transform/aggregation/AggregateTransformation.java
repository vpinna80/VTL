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
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.GroupingClause;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
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
	private final String name;
	private final Class<? extends ComponentRole> role;
	
	public AggregateTransformation(AggregateOperator aggregation, Transformation operand, GroupingClause groupingClause, Transformation having)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.groupingClause = groupingClause;
		this.having = having;
		this.name = null;
		this.role = null;

		if (this.having != null)
			throw new UnsupportedOperationException(aggregation + "(... having ...) not implemented");
	}

	// constructor for COUNT operator
	public AggregateTransformation(GroupingClause groupingClause, Transformation having)
	{
		this(COUNT, null, groupingClause, having);
	}
	
	// constructor for AGGR clause
	public AggregateTransformation(AggregateTransformation other, GroupingClause groupingClause, String name, Class<? extends ComponentRole> role)
	{
		this(other.aggregation, other.operand, groupingClause, null);
	}
	
	@Override
	protected VTLValue evalOnScalar(ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return Stream.of(scalar).collect(aggregation.getReducer());
	}

	@Override
	protected VTLValue evalOnDataset(DataSet dataset, VTLValueMetadata metadata)
	{
		SerCollector<DataPoint, ?, DataPoint> reducer = aggregation.getReducer(dataset.getMetadata().getComponents(Measure.class));

		Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = groupingClause == null ? emptySet() : groupingClause.getGroupingComponents(dataset.getMetadata());
		
		// dataset-level aggregation
		return dataset.aggr((DataSetMetadata) metadata, groupIDs, reducer, (dp, keyValues) -> {
				return new DataPointBuilder(keyValues)
					.addAll(dp)
					.build(dp.getLineage(), (DataSetMetadata) metadata);
			});
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
			Set<DataStructureComponent<Measure, ?, ?>> measures = dataset.getComponents(Measure.class);

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
				
				Set<DataStructureComponent<Measure, ?, ?>> newMeasures = dataset.getComponents(Measure.class);
				if (aggregation == COUNT)
					newMeasures = COUNT_MEASURE;
				else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(aggregation))
					newMeasures = newMeasures.stream()
						.map(c -> INTEGERDS.isAssignableFrom(c.getDomain()) ? DataStructureComponentImpl.of(c.getName(), Measure.class, NUMBERDS) : c)
						.collect(toSet());

				builder = builder.addComponents(aggregation == COUNT ? AggregateOperator.COUNT_MEASURE : newMeasures);
				
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
