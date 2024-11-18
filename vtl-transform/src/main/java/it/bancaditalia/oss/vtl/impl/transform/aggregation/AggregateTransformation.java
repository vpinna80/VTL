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
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEAN;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.TIMEDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.AVG;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.COUNT;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.STDDEV_POP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.STDDEV_SAMP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.VAR_POP;
import static it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator.VAR_SAMP;
import static it.bancaditalia.oss.vtl.util.SerCollectors.collectingAndThen;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.filtering;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.teeing;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerPredicate.not;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

import java.util.AbstractMap.SimpleEntry;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLExpectedRoleException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleParametersException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleRolesException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.transform.GroupingClause;
import it.bancaditalia.oss.vtl.impl.transform.UnaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.TimeValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.AggregateOperator;
import it.bancaditalia.oss.vtl.model.data.Component;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;
import it.bancaditalia.oss.vtl.util.SerCollector;

public class AggregateTransformation extends UnaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> COUNT_MEASURE = INTEGERDS.getDefaultVariable().as(Measure.class);

	private final AggregateOperator	aggregation;
	private final GroupingClause groupingClause;
	private final Transformation having;
	private final VTLAlias targetName; // target component name, used by aggr clause 
	private final Class<? extends Component> targetRole; // target component role, used by aggr clause
	
	public AggregateTransformation(AggregateOperator aggregation, Transformation operand, GroupingClause groupingClause, Transformation having)
	{
		super(operand);
		
		this.aggregation = aggregation;
		this.groupingClause = groupingClause;
		this.having = having;
		this.targetName = null;
		this.targetRole = null;
	}

	// constructor for COUNT operator
	public AggregateTransformation(GroupingClause groupingClause, Transformation having)
	{
		this(COUNT, null, groupingClause, having);
	}
	
	// constructor for AGGR clause
	public AggregateTransformation(AggregateTransformation other, GroupingClause groupingClause, Transformation having, Class<? extends Component> targetRole, VTLAlias targetName)
	{
		super(other.operand);
		
		this.aggregation = other.aggregation;
		this.groupingClause = groupingClause;
		this.having = having;
		this.targetName = targetName;
		this.targetRole = coalesce(targetRole, Measure.class);
	}
	
	@Override
	protected VTLValue evalOnScalar(MetadataRepository repo, ScalarValue<?, ?, ?, ?> scalar, VTLValueMetadata metadata)
	{
		return Stream.of(scalar).collect(aggregation.getReducer());
	}

	@Override
	protected VTLValue evalOnDataset(MetadataRepository repo, DataSet dataset, VTLValueMetadata metadata)
	{
		SerCollector<DataPoint, ?, Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>>> combined = null;
		DataSetMetadata origStructure = dataset.getMetadata();

		if (aggregation == COUNT)
			combined = collectingAndThen(counting(), v -> Map.of(COUNT_MEASURE, IntegerValue.of(v)));
		else
			for (DataStructureComponent<Measure, ?, ?> measure: dataset.getMetadata().getMeasures())
				if (combined == null)
					combined = mapping(dp -> dp.get(measure), filtering(not(NullValue.class::isInstance), collectingAndThen(aggregation.getReducer(), v -> new HashMap<>(Map.of(measure, v)))));
				else
					combined = teeing(mapping(dp -> dp.get(measure), filtering(not(NullValue.class::isInstance), collectingAndThen(aggregation.getReducer(), v -> new SimpleEntry<>(measure, v)))), combined, (e, m) -> { m.put(e.getKey(), e.getValue()); return m; });
		
		Set<DataStructureComponent<Identifier, ?, ?>> groupIDs = groupingClause == null ? emptySet() : groupingClause.getGroupingComponents(dataset.getMetadata());

		if (groupingClause != null && groupingClause.getFrequency() != null)
		{
			// aggregate the time-id
			DataStructureComponent<Identifier, ?, ?> timeID = groupIDs.stream()
					.filter(id -> TIMEDS.isAssignableFrom(id.getVariable().getDomain()))
					.findAny()
					.orElseThrow(() -> new VTLMissingComponentsException("A time identifier", groupIDs));
			
			dataset = new FunctionDataSet<>(origStructure, ds -> ds.stream().map(dp -> {
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> c = new HashMap<>(dp);
				c.compute(timeID, (k, v) -> groupingClause.getFrequency().wrap((TimeValue<?, ?, ?, ?>) v));
				return new DataPointBuilder(c).build(dp.getLineage(), origStructure);
			}), dataset);
		}

		// dataset-level aggregation
		return dataset.aggregate((DataSetMetadata) metadata, groupIDs, combined, (map, keyValues) -> {
			DataPointBuilder builder = new DataPointBuilder(keyValues.getValue());
			if (targetName == null)
				for (DataStructureComponent<?, ?, ?> measure: map.keySet())
					builder = builder.add(getCompFor(measure, repo, (DataSetMetadata) metadata), map.get(measure));
			else if (map.size() == 1)
			{
				DataStructureComponent<Measure, ?, ?> srcComp = origStructure.getMeasures().iterator().next();
				builder = builder.add(getCompFor(srcComp, repo, (DataSetMetadata) metadata), map.values().iterator().next());
			}
			else
				throw new IllegalStateException();
			
			return builder.build(LineageNode.of(THIS.toString(), keyValues.getKey()), (DataSetMetadata) metadata);
		});
	}

	private DataStructureComponent<?, ?, ?> getCompFor(DataStructureComponent<?, ?, ?> src, MetadataRepository repo, DataSetMetadata metadata)
	{
		DataStructureComponent<?, ?, ?> dest;
		if (aggregation == COUNT && targetName != null)
			dest = repo.createTempVariable(targetName, INTEGERDS).as(Measure.class);
		else if (aggregation == COUNT)
			dest = INTEGERDS.getDefaultVariable().as(Measure.class);
		else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(aggregation))
			dest = INTEGERDS.isAssignableFrom(src.getVariable().getDomain()) ? NUMBERDS.getDefaultVariable().as(Measure.class) : src;
		else if (targetName != null)
			dest = metadata.getComponent(targetName).orElseThrow(() -> new VTLMissingComponentsException(targetName, metadata));
		else
			dest = src;
		return dest;
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		VTLValueMetadata opmeta = operand == null ? session.getMetadata(THIS) : operand.getMetadata(session);
		MetadataRepository repo = session.getRepository();
		
		if (opmeta instanceof ScalarValueMetadata)
			if (NUMBER.isAssignableFrom(((ScalarValueMetadata<?, ?>) opmeta).getDomain()))
				return NUMBER;
			else
				throw new VTLIncompatibleTypesException(aggregation.toString().toLowerCase(), ((ScalarValueMetadata<?, ?>) opmeta).getDomain(), NUMBERDS);
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) opmeta;
			Set<DataStructureComponent<Measure, ?, ?>> measures = dataset.getMeasures();

			if (groupingClause == null)
			{
				if (measures.isEmpty())
					throw new VTLExpectedRoleException(Measure.class, dataset);

				Set<DataStructureComponent<Measure, ?, ?>> newMeasures = measures;
				if (aggregation == COUNT)
					newMeasures = singleton(COUNT_MEASURE);
				else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(aggregation))
					if (newMeasures.size() == 1)
					{
						DataStructureComponent<Measure, ?, ?> c = newMeasures.iterator().next();
						newMeasures = Set.of(INTEGERDS.isAssignableFrom(c.getVariable().getDomain()) ? NUMBERDS.getDefaultVariable().as(Measure.class) : c);
					}
					else if (newMeasures.stream().map(DataStructureComponent::getVariable).map(Variable::getDomain).anyMatch(INTEGERDS::isAssignableFrom))
						throw new UnsupportedOperationException("Only number measures are allowed for " + this);

				if (operand != null)
					return new DataStructureBuilder(newMeasures).build();
				
				if (measures.size() == 1)
					return measures.iterator().next().getVariable();
				else
					return new DataStructureBuilder(newMeasures).build();
			}
			else
			{
				Set<DataStructureComponent<Identifier, ?, ?>> groupComps = groupingClause.getGroupingComponents(dataset);
				
				Optional<DataStructureComponent<Identifier, ?, ?>> nonID = groupComps.stream().filter(c -> c.is(NonIdentifier.class)).findAny();
				if (nonID.isPresent())
					throw new VTLIncompatibleRolesException("aggr with group by", nonID.get(), Identifier.class);
				
				if (groupingClause.getFrequency() != null)
				{
					Set<DataStructureComponent<Identifier, ?, ?>> timeIDs = groupComps.stream()
						.filter(id -> TIMEDS.isAssignableFrom(id.getVariable().getDomain()))
						.collect(toSet());
					
					if (timeIDs.size() != 1)
						throw new VTLSingletonComponentRequiredException(Identifier.class, timeIDs);
				}
				
				Set<? extends DataStructureComponent<?, ?, ?>> newComps = dataset.getMeasures();
				if (aggregation == COUNT && targetName != null)
					newComps = singleton(repo.createTempVariable(targetName, INTEGERDS).as(Measure.class));
				else if (aggregation == COUNT)
					newComps = singleton(COUNT_MEASURE);
				else if (EnumSet.of(AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP).contains(aggregation))
					newComps = newComps.stream()
						.map(c -> INTEGERDS.isAssignableFrom(c.getVariable().getDomain()) ? NUMBERDS.getDefaultVariable().as(Measure.class) : c)
						.collect(toSet());
				
				if (targetName != null)
					if (measures.size() > 1)
						throw new VTLSingletonComponentRequiredException(Measure.class, newComps);
					else
						newComps = singleton(repo.createTempVariable(targetName, measures.iterator().next().getVariable().getDomain()).as(targetRole));

				DataSetMetadata structure = new DataStructureBuilder(groupComps).addComponents(newComps).build();
				
				if (having != null)
				{
					DataSetMetadata beforeHaving = new DataStructureBuilder(dataset).removeComponents(groupComps).build();
					VTLValueMetadata havingMeta = having.getMetadata(new ThisScope(repo, beforeHaving));
					ValueDomainSubset<?, ?> domain = null;
					if (havingMeta instanceof ScalarValueMetadata)
						domain = ((ScalarValueMetadata<?, ?>) havingMeta).getDomain();
					else
					{
						DataSetMetadata havingStructure = (DataSetMetadata) havingMeta;
						if (havingStructure.size() == 1)
						{
							DataStructureComponent<?, ?, ?> singleton = havingStructure.iterator().next();
							if (singleton.is(Measure.class))
								domain = singleton.getVariable().getDomain();
						}
					}
					
					if (!Domains.BOOLEANDS.isAssignableFrom(domain))
						throw new VTLIncompatibleParametersException(targetName.toString(), BOOLEAN, havingMeta);
				}
				
				return structure;
			}
		}
	}

	public AggregateOperator getAggregation()
	{
		return aggregation;
	}

	@Override
	public String toString()
	{
		return aggregation + "(" + coalesce(operand, "") + (groupingClause != null ? " " + groupingClause.toString() : " ") + ")";
	}
}
