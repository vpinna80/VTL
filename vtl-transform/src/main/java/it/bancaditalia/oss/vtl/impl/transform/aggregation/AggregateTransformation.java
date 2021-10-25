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
import static java.util.stream.Collectors.toSet;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.impl.transform.GroupingClause;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator;
import it.bancaditalia.oss.vtl.model.data.ComponentRole;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class AggregateTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final Set<DataStructureComponentImpl<Measure, EntireIntegerDomainSubset, IntegerDomain>> COUNT_MEASURE = singleton(new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS));

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
		SerCollector<DataPoint, ?, Entry<Lineage, Map<DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>>>> reducer = aggregation.getReducer(((DataSetMetadata) metadata).getComponents(Measure.class));

		if (groupingClause == null)
			try (Stream<DataPoint> stream = dataset.stream())
			{
				Entry<Lineage, Map<DataStructureComponent<? extends Measure, ?, ?>, ScalarValue<?, ?, ?, ?>>> result = stream.collect(reducer);
				if (operand == null && result.getValue().size() == 1)
					return result.getValue().values().iterator().next();

				return new LightDataSet((DataSetMetadata) metadata, () -> Stream.of(new DataPointBuilder()
						.addAll(result.getValue())
						.build(LineageNode.of(this, result.getKey()), (DataSetMetadata) metadata)));
			}
		else
		{
			Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = groupingClause.getGroupingComponents(dataset.getMetadata());
			
			// dataset-level aggregation
			return dataset.aggr((DataSetMetadata) metadata, groupIDs, reducer, (result, keyValues) -> 
					new DataPointBuilder(keyValues)
						.addAll(result.getValue())
						.build(LineageNode.of(this, result.getKey()), (DataSetMetadata) metadata));
		}
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
				
				for (DataStructureComponent<? extends Measure, ?, ?> measure: measures)
					if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
						throw new VTLIncompatibleTypesException("Aggregation", NUMBERDS, measure.getDomain());
				
				if (operand != null)
					return new DataStructureBuilder(measures).build();
				
				if (measures.size() == 1)
					return NUMBER;
				else
					return new DataStructureBuilder(measures).build();
			}
			else
			{
				Set<DataStructureComponent<Identifier,?,?>> groupComps = groupingClause.getGroupingComponents(dataset);
				
				Optional<DataStructureComponent<Identifier, ?, ?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				DataStructureBuilder builder = new DataStructureBuilder(groupComps.stream().map(c -> c.as(Identifier.class)).collect(toSet()));
				builder = builder.addComponents(aggregation == COUNT ? COUNT_MEASURE : dataset.getComponents(Measure.class));
				
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

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((aggregation == null) ? 0 : aggregation.hashCode());
		result = prime * result + ((groupingClause == null) ? 0 : groupingClause.hashCode());
		result = prime * result + ((having == null) ? 0 : having.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((role == null) ? 0 : role.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		AggregateTransformation other = (AggregateTransformation) obj;
		if (aggregation != other.aggregation)
			return false;
		if (groupingClause == null)
		{
			if (other.groupingClause != null)
				return false;
		}
		else if (!groupingClause.equals(other.groupingClause))
			return false;
		if (having == null)
		{
			if (other.having != null)
				return false;
		}
		else if (!having.equals(other.having))
			return false;
		if (name == null)
		{
			if (other.name != null)
				return false;
		}
		else if (!name.equals(other.name))
			return false;
		if (role == null)
		{
			if (other.role != null)
				return false;
		}
		else if (!role.equals(other.role))
			return false;
		return true;
	}
}
