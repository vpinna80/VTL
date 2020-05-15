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

import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.INNER_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.LEFT_JOIN;
import static it.bancaditalia.oss.vtl.util.Utils.byKey;
import static it.bancaditalia.oss.vtl.util.Utils.byValue;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.dataset.LightFDataSet;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLAmbiguousComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.transform.scope.JoinApplyScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointImpl.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl.Builder;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.NonIdentifier;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class JoinTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(JoinTransformation.class);

	public enum JoinOperator
	{
		 LEFT_JOIN,	INNER_JOIN, FULL_JOIN, CROSS_JOIN;
	}
	
	public static class JoinOperand implements Serializable
	{
		private static final long serialVersionUID = 1L;
		
		private final Transformation operand;
		private final String id;
		
		public JoinOperand(Transformation operand, String id)
		{
			this.operand = operand;
			this.id = id;
		}

		public Transformation getOperand()
		{
			return operand;
		}

		public String getId()
		{
			return id != null ? id : operand instanceof VarIDOperand ? ((VarIDOperand) operand).getText() : null;
		}
		
		@Override
		public String toString()
		{
			return operand + (id != null ? " AS " + id : "");
		}
	}

	private final JoinOperator operator;
	private final List<String> usingNames;
	private final List<JoinOperand> operands;
	private final Transformation apply;
	private final Transformation keepOrDrop;
	private final Transformation rename;
	private final Transformation filter;
	private final Transformation calc;
	private final Transformation aggr;

	private VTLDataSetMetadata metadata;
	private JoinOperand referenceDataSet;

	public JoinTransformation(JoinOperator operator, List<JoinOperand> operands, List<String> using, Transformation filter, Transformation apply, Transformation calc, Transformation aggr, Transformation keepOrDrop, Transformation rename)
	{
		this.operator = operator;
		this.filter = filter;
		this.calc = calc;
		this.aggr = aggr;
		this.apply = apply;
		this.rename = rename;
		this.operands = unmodifiableList(operands);
		this.usingNames = using == null ? emptyList() : using;
		this.keepOrDrop = keepOrDrop;
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operands.stream()
				.map(JoinOperand::getOperand)
				.map(Transformation::getTerminals)
				.flatMap(Set::stream)
				.collect(toSet());
	}

	@Override
	public DataSet eval(TransformationScheme session)
	{
		LOGGER.debug("Preparing renamed datasets for join");
		
		Map<JoinOperand, DataSet> values = new HashMap<>();
		for (JoinOperand operand: operands)
			values.put(operand, (DataSet) operand.getOperand().eval(session));
		
		Map<JoinOperand, DataSet> datasets = Utils.getStream(values.entrySet())
				.collect(toConcurrentMap(Entry::getKey, Utils.splitting(this::generateRenamedDataset)));
		
		DataSet result;
		
		if (usingNames.isEmpty())
		{
			// Case A: join all to reference ds
			LOGGER.debug("Collecting all identifiers");
			Map<DataSet, Set<DataStructureComponent<Identifier, ?, ?>>> ids = Utils.getStream(datasets)
					.filter(byKey(op -> op != referenceDataSet))
					.map(Entry::getValue)
					.collect(toConcurrentMap(ds -> ds, ds -> ds.getComponents(Identifier.class)));

			// TODO: Memory hungry!!! Find some way to stream instead of building this big index collection 
			LOGGER.debug("Indexing all datapoints");
			Map<DataSet, ? extends Map<Map<DataStructureComponent<Identifier,?,?>, ScalarValue<?,?,?>>, DataPoint>> indexes = Utils.getStream(datasets)
					.filter(Utils.byKey(op -> op != referenceDataSet))
					.map(Entry::getValue)
					.collect(toConcurrentMap(ds -> ds, ds -> {
						try (Stream<DataPoint> stream = ds.stream())
						{
							return stream
									// toMap instead of groupingBy because there's never more than one datapoint in each group
									.peek(dp -> LOGGER.trace("Indexing {}", dp))
									.collect(toConcurrentMap(dp -> dp.getValues(Identifier.class), dp -> dp));
						}
					}));
			
			LOGGER.debug("Joining all datapoints");
			result = new LightFDataSet<>(metadata, dataset -> dataset.stream()
				.peek(refDP -> LOGGER.trace("Joining {}", refDP))
				.map(refDP -> {
					// Get all datapoints from other datasets (there is no more than 1 for each dataset)
					List<DataPoint> otherDPs = Utils.getStream(datasets)
							.filter(byKey(op -> op != referenceDataSet))
							.map(Entry::getValue)
							.map(ds -> indexes.get(ds).get(refDP.getValues(ids.get(ds), Identifier.class)))
							.filter(Objects::nonNull)
							.collect(toList());
						
					switch (operator)
					{
						case INNER_JOIN:
						case LEFT_JOIN:
							if (otherDPs.size() != indexes.size())
								return operator == INNER_JOIN ? null : new DataPointBuilder(refDP)
										.addAll(metadata.stream().collect(toMap(c -> c, c -> NullValue.instanceFrom(c))))
										.build(metadata);
							else
							{
								// Join all datapoints
								DataPoint accDP = refDP;
								for (DataPoint otherDP : otherDPs)
									accDP = accDP.merge(otherDP);
								
								LOGGER.trace("Joined {}", accDP);
								return accDP;
							}
						default: throw new UnsupportedOperationException(operator + " not implemented"); 
					}
				}).filter(Objects::nonNull), datasets.get(referenceDataSet));
		}
		else
		{
//			Set<DataStructureComponent<?, ?, ?>> using = usingNames.stream()
//					.map(metadata::getComponent)
//					.collect(Collectors.toSet());
//			
//			boolean isCaseB1 = datasets.values().stream()
//					.map(DataSet::getDataStructure)
//					.allMatch(str -> str.containsAll(using));
//			
//			if (isCaseB1)
//			{
//				// case B1: join all to reference dataset with given keys
//				DataSet accumulator = datasets.get(referenceDataSet);
//				for (JoinOperand item: datasets.keySet())
//					if (item != referenceDataSet)
//						accumulator = performBinaryJoin(accumulator, datasets.get(item));
//				
//				return accumulator;
//			}
//			else
				throw new UnsupportedOperationException("inner_join case B1-B2 not implemented");
		}
		
		if (filter != null)
			result = (DataSet) filter.eval(new ThisScope(result, session));
		if (apply != null)
			result = applyClause(session, result);
		if (calc != null)
			result = (DataSet) calc.eval(new ThisScope(result, session));
		if (aggr != null)
			result = (DataSet) aggr.eval(new ThisScope(result, session));
		if (keepOrDrop != null)
			result = (DataSet) keepOrDrop.eval(new ThisScope(result, session));
		if (rename != null)
			result = (DataSet) rename.eval(new ThisScope(result, session));
		
		return result;
	}

	private DataSet applyClause(TransformationScheme session, DataSet dataset)
	{
		if (apply == null)
			return dataset;
		
		Set<DataStructureComponent<Measure, ?, ?>> applyComponents = metadata.getComponents(Measure.class).stream()
				.map(c -> c.getName( ).replaceAll("^.*#", ""))
				.distinct()
				.map(name -> {
					ValueDomainSubset<?> domain = ((VTLScalarValueMetadata<?>) apply.getMetadata(new JoinApplyScope(session, name, metadata))).getDomain();
					return new DataStructureComponentImpl<>(name, Measure.class, domain);
				}).collect(toSet());
		
		VTLDataSetMetadata applyMetadata = metadata.stream()
				.filter(c -> !c.is(Measure.class) || !c.getName().contains("#"))
				.reduce(new Builder(), Builder::addComponent, Builder::merge)
				.addComponents(applyComponents)
				.build();
		
		return dataset.mapKeepingKeys(applyMetadata, dp -> applyComponents.stream()
				.collect(toConcurrentMap(c -> c, c -> (ScalarValue<?, ?, ?>) apply.eval(new JoinApplyScope(session, c.getName(), dp)))));
	}

	private DataSet generateRenamedDataset(JoinOperand op, DataSet ds)
	{
		Set<DataStructureComponent<?, ?, ?>> nonMatching = new HashSet<>(ds.getComponents());
		nonMatching.removeAll(metadata);
		// Shortcut if no component is to be renamed
		if (nonMatching.isEmpty())
			return new NamedDataSet(op.getId(), ds);
		else
		{
			LOGGER.trace("In dataset {} renaming components {}", op.getId(), ds.getComponents());
			Set<DataStructureComponent<?, ?, ?>> matching = new HashSet<>(ds.getComponents());
			matching.retainAll(metadata);
			Map<DataStructureComponent<?, ?, ?>, DataStructureComponent<?, ?, ?>> renamed = nonMatching.stream()
					.collect(toMap(c -> c, c -> c.rename(op.getId() + "#" + c.getName())));
			VTLDataSetMetadata operandStructure = new DataStructureImpl.Builder(matching)
					.addComponents(renamed.values())
					.build();
			LOGGER.trace("Structure of dataset {} will be changed to {}", op.getId(), operandStructure);
			// must go through all datapoints to rename each component in the datapoint
			return new NamedDataSet(op.getId(), new LightFDataSet<>(operandStructure, dataset -> dataset.stream()
				.map(dp -> new DataPointBuilder(Utils.getStream(nonMatching)
								.collect(toMap(renamed::get, dp::get)))
							.addAll(dp.getValues(matching))
							.build(operandStructure)), ds));
		}
	}

	@Override
	public VTLDataSetMetadata getMetadata(TransformationScheme session)
	{
		if (metadata != null)
			return metadata;
		
		if (operator != INNER_JOIN && operator != LEFT_JOIN)
			throw new UnsupportedOperationException("Not implemented: " + operator.toString());
		
		Set<Transformation> unaliased = operands.stream()
				.filter(o -> o.getId() == null)
				.map(JoinOperand::getOperand)
				.collect(toSet());
		
		if (unaliased.size() > 0)
			throw new VTLSyntaxException("Join expressions must be aliased: " + unaliased + ".", null);
		
		Set<String> counts = operands.stream()
				.map(JoinOperand::getId)
				.collect(groupingBy(identity(), counting()))
				.entrySet().stream()
				.filter(e -> e.getValue() > 1)
				.map(Entry::getKey)
				.collect(toSet());
		if (counts.size() > 0)
			throw new VTLSyntaxException("Duplicate aliases in join: " + counts);

		Map<JoinOperand, VTLDataSetMetadata> datasetsMeta = operands.stream()
				.collect(toMap(op -> op, op -> (VTLDataSetMetadata) op.getOperand().getMetadata(session)));
		
		Optional<JoinOperand> caseAorB1 = isCaseAorB1(datasetsMeta);
		Optional<JoinOperand> caseB2 = isCaseB2(caseAorB1, datasetsMeta);
		
		referenceDataSet = caseAorB1.orElseGet(caseB2::get);
		
		LOGGER.info("Joining {} to ({}: {})", operands.stream().filter(op -> op != referenceDataSet).map(JoinOperand::getId).collect(toList()), 
				referenceDataSet.getId(), datasetsMeta.get(referenceDataSet));
			
		metadata = joinStructures(datasetsMeta, caseAorB1, caseB2);
		
		VTLDataSetMetadata result = metadata;
		
		if (filter != null)
			result = (VTLDataSetMetadata) aggr.getMetadata(new ThisScope(result, session));
		if (apply != null)
		{
			Set<DataStructureComponent<Measure, ?, ?>> applyComponents = metadata.getComponents(Measure.class).stream()
					.map(c -> c.getName( ).replaceAll("^.*#", ""))
					.distinct()
					.map(name -> {
						ValueDomainSubset<?> domain = ((VTLScalarValueMetadata<?>) apply.getMetadata(new JoinApplyScope(session, name, metadata))).getDomain();
						return new DataStructureComponentImpl<>(name, Measure.class, domain);
					}).collect(toSet());
			
			result = metadata.stream()
					.filter(c -> !c.is(Measure.class) || !c.getName().contains("#"))
					.reduce(new Builder(), Builder::addComponent, Builder::merge)
					.addComponents(applyComponents)
					.build();
		}
		if (calc != null)
			result = (VTLDataSetMetadata) calc.getMetadata(new ThisScope(result, session));
		if (aggr != null)
			result = (VTLDataSetMetadata) aggr.getMetadata(new ThisScope(result, session));
		if (keepOrDrop != null)
			result = (VTLDataSetMetadata) keepOrDrop.getMetadata(new ThisScope(result, session));
		if (rename != null)
			result = (VTLDataSetMetadata) rename.getMetadata(new ThisScope(result, session));

		Optional<String> ambiguousComponent = result.stream()
			.map(DataStructureComponent::getName)
			.filter(n -> n.contains("#"))
			.findAny()
			.map(n -> n.replaceAll("^.*#", ""));
		
		if (ambiguousComponent.isPresent())
			throw new VTLAmbiguousComponentException(ambiguousComponent.get(), result.stream()
					.filter(c -> c.getName().endsWith("#" + ambiguousComponent.get()))
					.collect(toSet()));
		
		return result;
	}
	
	private Optional<JoinOperand> isCaseAorB1(Map<JoinOperand, VTLDataSetMetadata> datasetsMeta)
	{
		// Case A: One dataset must contain the identifiers of all the others
		Set<DataStructureComponent<Identifier, ?, ?>> allIDs = datasetsMeta.values().stream()
				.flatMap(ds -> ds.getComponents(Identifier.class).stream())
				.collect(Collectors.toSet());
		
		Optional<JoinOperand> max = datasetsMeta.entrySet().stream()
				.filter(Utils.byValue(ds -> ds.containsAll(allIDs)))
				.map(Entry::getKey)
				.findFirst();
		
		if (!max.isPresent())
		{
			if (usingNames.isEmpty())
			{
				// In Case A but conditions not fulfilled
				UnsupportedOperationException e = new UnsupportedOperationException("In inner join without using clause, one dataset identifier set must contain identifiers of all other datasets.");
				LOGGER.error("Error in " + this, e);
				for (Entry<JoinOperand, VTLDataSetMetadata> operand: datasetsMeta.entrySet())
					LOGGER.debug("Operand {} is {}", operand.getKey().getId(), operand.getValue().getComponents(Identifier.class).toString());
				throw e;
			}
			else
				// Not in case A or B1
				return Optional.empty();
		}
		else if (usingNames.isEmpty())
		{
			// In case B1 
			Long howMany = (long) datasetsMeta.size();
			Set<DataStructureComponent<Identifier, ?, ?>> commonIDs = datasetsMeta.values().stream()
					.flatMap(ds -> ds.getComponents(Identifier.class).stream())
					.collect(groupingByConcurrent(c -> c, counting()))
					.entrySet().stream()
					.filter(byValue(howMany::equals))
					.map(Entry::getKey)
					.collect(toSet());

			Set<DataStructureComponent<?,?,?>> using = datasetsMeta.values().stream()
					.flatMap(ds -> ds.getComponents(usingNames).stream())
					.collect(toSet());
			
			if (!commonIDs.containsAll(using))
			{
				// Case B1 conditions not fulfilled
				UnsupportedOperationException e = new UnsupportedOperationException("In inner join with using clause, named ids must be common to all datasets.");
				LOGGER.error("Error in " + this, e);
				LOGGER.debug("Using " + using);
				for (VTLDataSetMetadata dataset: datasetsMeta.values())
					LOGGER.debug(dataset.getComponents(Identifier.class).toString());
				throw e;
			}
			else
				// B1 fulfilled
				return max;
		}
		else
			// case A fulfilled
			return max;
	}

	private Optional<JoinOperand> isCaseB2(Optional<JoinOperand> caseAorB1, Map<JoinOperand, VTLDataSetMetadata> datasetsMeta)
	{
		if (caseAorB1.isPresent())
			return caseAorB1;
		
		throw new UnsupportedOperationException("Case B2 not implemented");
//		
//		if (datasetsMeta.size() < 2)
//			throw new VTLException("Expected at least 2 datasets in join.");
//		
//		Iterator<VTLDataSetMetadata> iterator = datasetsMeta.values().iterator();
//		Set<DataStructureComponent<Identifier, ?, ?>> first = iterator.next().getComponents(Identifier.class);
//		Set<DataStructureComponent<Identifier, ?, ?>> second = iterator.next().getComponents(Identifier.class);
//		VTLDataSetMetadata max = null;
//		for (VTLDataSetMetadata dataset: datasetsMeta.values())
//		{
//			Set<DataStructureComponent<Identifier, ?, ?>> ids = dataset.getComponents(Identifier.class);
//
//			if (!ids.equals(first) && !ids.equals(second))
//			{
//				// Case B2 conditions not fulfilled
//				UnsupportedOperationException e = new UnsupportedOperationException("In inner join with using clause, all datasets but one must have the same identifiers.");
//				LOGGER.error("Error in " + this, e);
//				for (VTLDataSetMetadata dataset2: datasetsMeta.values())
//					LOGGER.debug(dataset2.getComponents(Identifier.class).toString());
//				throw e;
//			}
//			else
//				max = max != null ? max : ids.equals(first) ? second : first;
//		}
//		
//		// Verify that reference dataset contains all the using components
//		if (max.getComponents(usingNames).size() != usingNames.size())
//		{
//			// Case B2 conditions not fulfilled
//			UnsupportedOperationException e = new UnsupportedOperationException("In inner join with using clause, the reference dataset must contain all used components.");
//			LOGGER.error("Error in {}" + this, e);
//			LOGGER.debug("Using {}", usingNames);
//			LOGGER.debug("But reference dataset structure is {}", max);
//			throw e;
//		}
//		
//		for (Entry<JoinOperand, VTLDataSetMetadata> dataset: datasetsMeta.entrySet())
//			if (max != dataset.getValue() && dataset.getValue().getComponents(usingNames, Identifier.class).size() != usingNames.size())
//			{
//				// Case B2 conditions not fulfilled
//				UnsupportedOperationException e = new UnsupportedOperationException("In inner join with using clause, dataset " + dataset.getKey().getId() + " must contain all used components as identifiers.");
//				LOGGER.error("Error in {}" + this, e);
//				LOGGER.debug("Using {}", usingNames);
//				LOGGER.debug("But dataset {} is {}", dataset.getKey().getId(), dataset.getValue());
//				throw e;
//			}
//		
//		return datasetsMeta.entrySet().stream()
//				.filter(byValue(max::equals))
//				.map(Entry::getKey)
//				.findFirst();
	}
	
	private VTLDataSetMetadata joinStructures(Map<JoinOperand,VTLDataSetMetadata> datasetsMeta, Optional<?> caseAorB1, Optional<?> caseB2)
	{
//		List<Entry<String, VTLDataSetMetadata>> dataSets = operands.stream()
//				.map(op -> new SimpleEntry<>(op.getId(), datasetsMeta.get(op)))
//				.collect(toList());
	
		if (caseAorB1.isPresent())
		{
			Set<DataStructureComponent<?,?,?>> using = datasetsMeta.values().stream()
					.flatMap(ds -> ds.getComponents(usingNames).stream())
					.collect(toSet());
			
			// Case A: rename all measures and attributes with the same name 
			// Case B1: rename all components with the same name except those in the using clause  
			ConcurrentMap<DataStructureComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
			Set<DataStructureComponent<?, ?, ?>> toBeRenamed = datasetsMeta.entrySet().stream()
					.map(Entry::getValue)
					.flatMap(d -> d.stream())
					.filter(c -> unique.putIfAbsent(c, TRUE) != null)
					.filter(c -> using.isEmpty() && c.is(NonIdentifier.class) || !using.isEmpty() && !using.contains(c))
					.collect(toSet());
			
			LOGGER.debug("Inner join renames: {}", toBeRenamed);
			
			Builder builder = new Builder();
			for (Entry<JoinOperand, VTLDataSetMetadata> e: datasetsMeta.entrySet())
				for (DataStructureComponent<?, ?, ?> c: e.getValue())
					if (toBeRenamed.contains(c))
						builder.addComponent(c.rename(e.getKey().getId() + "#" + c.getName()));
					else
						builder.addComponent(c);

			return builder.build();
		}
		else
			throw new UnsupportedOperationException();
	}
	
	@Override
	public String toString()
	{
		return "inner_join(" + operands.stream().map(Object::toString).collect(joining(", "))
				+ (usingNames.isEmpty() ? "" : " using " + usingNames.stream().collect(joining(", ")))
				+ ")";
	}
}
