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

import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.INNER_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.LEFT_JOIN;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.normalizeAlias;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.onlyIf;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLAmbiguousComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.transform.scope.JoinApplyScope;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.FunctionDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.NamedDataSet;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
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
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

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
			this.id = id == null ? null : normalizeAlias(id);
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

	private transient JoinOperand referenceDataSet;

	@SuppressWarnings("java:S107")
	public JoinTransformation(JoinOperator operator, List<JoinOperand> operands, List<String> using, Transformation filter, Transformation apply, Transformation calc, Transformation aggr, Transformation keepOrDrop, Transformation rename)
	{
		this.operator = operator;
		this.filter = filter;
		this.calc = calc;
		this.aggr = aggr;
		this.apply = apply;
		this.rename = rename;
		this.operands = unmodifiableList(operands);
		this.usingNames = coalesce(using, emptyList()).stream().map(DataStructureComponent::normalizeAlias).collect(toList());
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
	@SuppressWarnings("java:S3864")
	public DataSet eval(TransformationScheme scheme)
	{
		LOGGER.debug("Preparing renamed datasets for join");
		
		Map<JoinOperand, DataSet> values = operands.stream()
			.collect(toMapWithValues(operand -> (DataSet) operand.getOperand().eval(scheme)));
		
		DataSet result;
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		
		if (usingNames.isEmpty())
		{
			// Find out which component must be renamed
			Map<JoinOperand, DataSet> datasets = renameCaseAB1(values); 

			// Case A: join all to reference ds
			LOGGER.debug("Collecting all identifiers");
			Map<DataSet, Set<DataStructureComponent<Identifier, ?, ?>>> ids = datasets.entrySet().stream()
					.filter(entryByKey(op -> op != referenceDataSet))
					.map(Entry::getValue)
					.collect(toConcurrentMap(ds -> ds, ds -> ds.getComponents(Identifier.class)));

			// TODO: Memory hungry!!! Find some way to stream instead of building this big index collection 
			LOGGER.debug("Indexing all datapoints");
			Map<DataSet, ? extends Map<Map<DataStructureComponent<Identifier,?,?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> indexes = datasets.entrySet().stream()
					.filter(entryByKey(op -> op != referenceDataSet))
					.map(Entry::getValue)
					.collect(toMapWithValues(ds -> {
						// needed to close the stream after usage
						try (Stream<DataPoint> stream = ds.stream())
						{
							return stream
									.peek(dp -> LOGGER.trace("Indexing {}", dp))
									// toMap instead of groupingBy because there's never more than one datapoint in each group
									.collect(toConcurrentMap(dp -> dp.getValues(Identifier.class), dp -> dp));
						}
					}));

			// Structure before applying any clause
			DataSetMetadata totalStructure = datasets.values().stream()
				.map(DataSet::getMetadata)
				.flatMap(Set::stream)
				.collect(toDataStructure());
			
			LOGGER.debug("Joining all datapoints");
			
			result = new FunctionDataSet<>(totalStructure, dataset -> dataset.stream()
				.peek(refDP -> LOGGER.trace("Joining {}", refDP))
				.map(refDP -> {
					// Get all datapoints from other datasets (there is no more than 1 for each dataset)
					List<DataPoint> otherDPs = datasets.entrySet().stream()
							.filter(entryByKey(op -> op != referenceDataSet))
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
										.addAll(totalStructure.stream().collect(toMapWithValues(NullValue::instanceFrom)))
										.build(refDP.getLineage(), totalStructure);
							else
							{
								// Join all datapoints
								DataPoint accDP = refDP;
								for (DataPoint otherDP : otherDPs)
									accDP = accDP.combine(otherDP, (dp1, dp2) -> lineageCombiner(dp1, dp2));
								
								LOGGER.trace("Joined {}", accDP);
								return accDP;
							}
						default: throw new UnsupportedOperationException(operator + " not implemented"); 
					}
				}).filter(Objects::nonNull), datasets.get(referenceDataSet));
		}
		else
			throw new UnsupportedOperationException("inner_join case B1-B2 not implemented");
		
		if (filter != null)
			result = (DataSet) filter.eval(new ThisScope(result));
		
		/* apply calc and aggr are mutually exclusive */
		if (apply != null)
			result = applyClause(metadata, scheme, result);
		else if (calc != null)
			result = (DataSet) calc.eval(new ThisScope(result));
		else if (aggr != null)
			result = (DataSet) aggr.eval(new ThisScope(result));
		
		if (keepOrDrop != null)
			result = (DataSet) keepOrDrop.eval(new ThisScope(result));
		
		if (rename != null)
		{
			result = (DataSet) rename.eval(new ThisScope(result));

			// unalias all remaining components that have not been already unaliased 
			Set<DataStructureComponent<?, ?, ?>> remaining = new HashSet<>(result.getMetadata());
			remaining.removeAll(metadata);
			
			DataSet finalResult = result;
			result = new StreamWrapperDataSet(metadata, () -> finalResult.stream()
				.map(dp -> dp.entrySet().stream()
						.map(keepingValue(onlyIf(comp -> comp.getName().contains("#"), 
								comp -> comp.rename(comp.getName().split("#", 2)[1])))
						).collect(toDataPoint(LineageNode.of(rename, dp.getLineage()), metadata))));
		}
		
		return result;
	}

	private Lineage lineageCombiner(DataPoint dp1, DataPoint dp2)
	{
		Lineage l1 = dp1.getLineage();
		Lineage l2 = dp2.getLineage();

		String joinString = operands.stream()
				.map(o -> o.getId() != null ? o.getId() : o.getOperand().toString())
				.collect(joining(", ", operator.toString().toLowerCase() + "(", ")"));

		Collection<Lineage> s1, s2;
		if (l1 instanceof LineageNode && joinString == ((LineageNode) l1).getTransformation())
			s1 = ((LineageNode) l1).getSourceSet().getSources();
		else
			s1 = singleton(l1);
		if (l2 instanceof LineageNode && joinString == ((LineageNode) l2).getTransformation())
			s2 = ((LineageNode) l2).getSourceSet().getSources();
		else
			s2 = singleton(l2);
		
		List<Lineage> sources = new ArrayList<>(s1);
		sources.addAll(s2);

		return LineageNode.of(joinString, sources.toArray(new Lineage[sources.size()]));
	}

	private Map<JoinOperand, DataSet> renameCaseAB1(Map<JoinOperand, DataSet> datasets)
	{
		ConcurrentMap<DataStructureComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
		Set<DataStructureComponent<?, ?, ?>> toBeRenamed = datasets.values().stream()
				.map(DataSet::getMetadata)
				.flatMap(d -> d.stream())
				.filter(c -> unique.putIfAbsent(c, TRUE) != null)
				.filter(c -> usingNames.isEmpty() ? c.is(NonIdentifier.class) : !usingNames.contains(c.getName()))
				.collect(toSet());

		return datasets.entrySet().stream()
			.map(keepingKey((op, ds) -> {
				String qualifier = op.getId() + "#";
				DataSetMetadata oldStructure = ds.getMetadata();
				
				// find components that must be renamed and add 'alias#' in front of their name
				DataSetMetadata newStructure = oldStructure.stream()
						.map(c -> toBeRenamed.contains(c) ? c.rename(qualifier + c.getName()) : c)
						.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
						.build();
				
				if (newStructure.equals(oldStructure))
				{
					// no need to change the operand, the structure is the same
					LOGGER.trace("Structure of dataset {} will be kept", oldStructure);
					return ds;
				}

				LOGGER.trace("Structure of dataset {} will be changed to {}", oldStructure, newStructure);
				
				// Create the dataset operand renaming components in all its datapoints
				return new NamedDataSet(op.getId(), new FunctionDataSet<>(newStructure, dataset -> dataset.stream()
						.map(dp -> dp.entrySet().stream()
								.map(keepingValue(c -> toBeRenamed.contains(c) ? c.rename(qualifier + c.getName()) : c))
								.collect(toDataPoint(dp.getLineage(), newStructure))
						), ds));
			})).collect(entriesToMap());
	}

	private DataSet applyClause(DataSetMetadata metadata, TransformationScheme session, DataSet dataset)
	{
		if (apply == null)
			return dataset;
		
		Set<DataStructureComponent<Measure, ?, ?>> applyComponents = dataset.getComponents(Measure.class).stream()
				.map(c -> c.getName( ).replaceAll("^.*#", ""))
				.distinct()
				.map(name -> {
					ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) apply.getMetadata(new JoinApplyScope(session, name, dataset.getMetadata()))).getDomain();
					return DataStructureComponentImpl.of(name, Measure.class, domain).asRole(Measure.class);
				}).collect(toSet());
		
		DataSetMetadata applyMetadata = dataset.getMetadata().stream()
				.filter(c -> !c.is(Measure.class) || !c.getName().contains("#"))
				.collect(toDataStructure(applyComponents));
		
		return dataset.mapKeepingKeys(applyMetadata, dp -> LineageNode.of(this, dp.getLineage()), 
				dp -> applyComponents.stream()
						.collect(toMapWithValues(c -> {
							return (ScalarValue<?, ?, ?, ?>) apply.eval(new JoinApplyScope(session, c.getName(), dp));
						})));
	}
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		if (operator != INNER_JOIN && operator != LEFT_JOIN)
			throw new UnsupportedOperationException("Not implemented: " + operator.toString());
		
		// check if expressions have aliases
		operands.stream()
				.filter(o -> o.getId() == null)
				.map(JoinOperand::getOperand)
				.findAny()
				.ifPresent(unaliased -> {
					throw new VTLSyntaxException("Join expressions must be aliased: " + unaliased + ".", null);
				});
		
		// check for duplicate aliases
		operands.stream()
				.map(JoinOperand::getId)
				.collect(groupingBy(identity(), counting()))
				.entrySet().stream()
				.filter(e -> e.getValue() > 1)
				.map(Entry::getKey)
				.findAny()
				.ifPresent(alias -> {
					throw new VTLSyntaxException("Join aliases must be unique: " + alias);
				});

		Map<JoinOperand, DataSetMetadata> datasetsMeta = new HashMap<>();
		for (JoinOperand op: operands)
			datasetsMeta.put(op, (DataSetMetadata) op.getOperand().getMetadata(scheme));
		
		Optional<JoinOperand> caseAorB1 = isCaseAorB1(datasetsMeta);
		Optional<JoinOperand> caseB2 = isCaseB2(caseAorB1, datasetsMeta);
		
		referenceDataSet = caseAorB1.orElseGet(caseB2::get);
		
		LOGGER.info("Joining {} to ({}: {})", 
				operands.stream().filter(op -> op != referenceDataSet).collect(toMap(JoinOperand::getId, datasetsMeta::get)), 
				referenceDataSet.getId(), datasetsMeta.get(referenceDataSet));
			
		DataSetMetadata result = joinStructures(datasetsMeta, caseAorB1, caseB2);
		
		// modify the result structure as needed
		if (filter != null)
			result = (DataSetMetadata) filter.getMetadata(new ThisScope(result));
		if (apply != null)
		{
			DataSetMetadata applyResult = result; 
			Set<DataStructureComponent<Measure, ?, ?>> applyComponents = applyResult.getComponents(Measure.class).stream()
					.map(c -> c.getName( ).replaceAll("^.*#", ""))
					.distinct()
					.map(name -> {
						ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) apply.getMetadata(new JoinApplyScope(scheme, name, applyResult))).getDomain();
						return DataStructureComponentImpl.of(name, Measure.class, domain).asRole(Measure.class);
					}).collect(toSet());
			
			result = applyResult.stream()
					.filter(c -> !c.is(Measure.class) || !c.getName().contains("#"))
					.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
					.addComponents(applyComponents)
					.build();
		}
		else if (calc != null)
			result = (DataSetMetadata) calc.getMetadata(new ThisScope(result));
		else if (aggr != null)
			result = (DataSetMetadata) aggr.getMetadata(new ThisScope(result));
		if (keepOrDrop != null)
			result = (DataSetMetadata) keepOrDrop.getMetadata(new ThisScope(result));
		if (rename != null)
		{
			result = (DataSetMetadata) rename.getMetadata(new ThisScope(result));
			// check if rename has made some components unambiguous
			Map<String, List<String>> sameUnaliasedName = result.stream()
				.map(DataStructureComponent::getName)
				.filter(name -> name.contains("#"))
				.map(name -> name.split("#", 2))
				.collect(groupingByConcurrent(name -> name[1], mapping(name -> name[0], toList())));
			// unalias the unambiguous components and add them to the renaming list
			result = result.stream()
				.map(comp -> comp.getName().contains("#") && sameUnaliasedName.get(comp.getName().split("#", 2)[1]).size() <= 1 
					? comp.rename(comp.getName().split("#", 2)[1])
					: comp
				).collect(toDataStructure());
		}

		// find components with the same name from different aliases
		DataSetMetadata finalResult = result;
		result.stream()
			.map(DataStructureComponent::getName)
			.filter(name -> name.contains("#"))
			.findAny()
			.map(name -> name.replaceAll("^.*#", ""))
			.ifPresent(ambiguousComponent -> {
				final Set<DataStructureComponent<?, ?, ?>> sameUnaliasedName = finalResult.stream()
						.filter(c -> c.getName().endsWith("#" + ambiguousComponent))
						.collect(toSet());
				throw new VTLAmbiguousComponentException(ambiguousComponent, sameUnaliasedName);
			});
		
		return result;
	}
	
	private Optional<JoinOperand> isCaseAorB1(Map<JoinOperand, DataSetMetadata> datasetsMeta)
	{
		// Case A: One dataset must contain the identifiers of all the others
		Set<DataStructureComponent<Identifier, ?, ?>> allIDs = datasetsMeta.values().stream()
				.flatMap(ds -> ds.getComponents(Identifier.class).stream())
				.collect(toSet());
		
		Optional<JoinOperand> max = datasetsMeta.entrySet().stream()
				.filter(entryByValue(ds -> ds.containsAll(allIDs)))
				.map(Entry::getKey)
				.findFirst();
		
		if (!max.isPresent())
		{
			if (usingNames.isEmpty())
			{
				// In Case A but conditions not fulfilled
				UnsupportedOperationException e = new UnsupportedOperationException("In inner join without using clause, one dataset identifier set must contain identifiers of all other datasets.");
				LOGGER.error("Error in " + this, e);
				for (Entry<JoinOperand, DataSetMetadata> operand: datasetsMeta.entrySet())
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
					.filter(entryByValue(howMany::equals))
					.map(Entry::getKey)
					.collect(toSet());

			Set<DataStructureComponent<?,?,?>> using = datasetsMeta.values().stream()
					.flatMap(ds -> usingNames.stream()
							.map(ds::getComponent)
							.map(Optional::get)
					).collect(toSet());
			
			if (!commonIDs.containsAll(using))
			{
				// Case B1 conditions not fulfilled
				UnsupportedOperationException e = new UnsupportedOperationException("In inner join with using clause, named ids must be common to all datasets.");
				LOGGER.error("Error in " + this, e);
				LOGGER.debug("Using " + using);
				for (DataSetMetadata dataset: datasetsMeta.values())
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

	private Optional<JoinOperand> isCaseB2(Optional<JoinOperand> caseAorB1, Map<JoinOperand, DataSetMetadata> datasetsMeta)
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
	
	private DataSetMetadata joinStructures(Map<JoinOperand,DataSetMetadata> datasetsMeta, Optional<?> caseAorB1, Optional<?> caseB2)
	{
//		List<Entry<String, VTLDataSetMetadata>> dataSets = operands.stream()
//				.map(op -> new SimpleEntry<>(op.getId(), datasetsMeta.get(op)))
//				.collect(toList());
	
		if (caseAorB1.isPresent())
		{
			Set<DataStructureComponent<?,?,?>> using = datasetsMeta.values().stream()
					.flatMap(ds -> usingNames.stream()
							.map(ds::getComponent)
							.map(Optional::get)
					).collect(toSet());
			
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
			
			DataStructureBuilder builder = new DataStructureBuilder();
			for (Entry<JoinOperand, DataSetMetadata> e: datasetsMeta.entrySet())
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
		return operator.toString().toLowerCase() + "(" + operands.stream().map(Object::toString).collect(joining(", "))
				+ (usingNames.isEmpty() ? "" : " using " + usingNames.stream().collect(joining(", ")))
				+ (filter != null ? " " + filter : "")
				+ (apply != null ? " apply " + apply : "")
				+ (calc != null ? " " + calc : "")
				+ (aggr != null ? " " + aggr : "")
				+ (keepOrDrop != null ? " " + keepOrDrop : "")
				+ (rename != null ? " " + rename : "")
				+ ")";
	}
}