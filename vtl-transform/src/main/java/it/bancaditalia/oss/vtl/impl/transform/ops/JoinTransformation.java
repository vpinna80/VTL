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

import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.CROSS_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.FULL_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.INNER_JOIN;
import static it.bancaditalia.oss.vtl.impl.transform.ops.JoinTransformation.JoinOperator.LEFT_JOIN;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.toDataPoint;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder.toDataStructure;
import static it.bancaditalia.oss.vtl.util.SerCollectors.counting;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.groupingByConcurrent;
import static it.bancaditalia.oss.vtl.util.SerCollectors.mapping;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithValues;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.SerUnaryOperator.identity;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static it.bancaditalia.oss.vtl.util.Utils.entryByKey;
import static it.bancaditalia.oss.vtl.util.Utils.entryByValue;
import static it.bancaditalia.oss.vtl.util.Utils.keepingKey;
import static it.bancaditalia.oss.vtl.util.Utils.keepingValue;
import static it.bancaditalia.oss.vtl.util.Utils.onlyIf;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static it.bancaditalia.oss.vtl.util.Utils.tryWith;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;
import static java.util.Collections.unmodifiableList;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.joining;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
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
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLAmbiguousComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLIncompatibleStructuresException;
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
import it.bancaditalia.oss.vtl.model.data.Variable;
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
		LEFT_JOIN, INNER_JOIN, FULL_JOIN, CROSS_JOIN
	}

	public static class JoinOperand implements Serializable
	{
		private static final long serialVersionUID = 1L;

		private final Transformation operand;
		private final String id;

		public JoinOperand(Transformation operand, String id)
		{
			this.operand = operand;
			this.id = id == null ? null : Variable.normalizeAlias(id);
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

	private transient JoinOperand refOperand;

	@SuppressWarnings("java:S107")
	public JoinTransformation(JoinOperator operator, List<JoinOperand> operands, List<String> using, Transformation filter, Transformation apply, Transformation calc, Transformation aggr,
			Transformation keepOrDrop, Transformation rename)
	{
		this.operator = operator;
		this.filter = filter;
		this.calc = calc;
		this.aggr = aggr;
		this.apply = apply;
		this.rename = rename;
		this.operands = unmodifiableList(operands);
		this.usingNames = coalesce(using, emptyList()).stream().map(Variable::normalizeAlias).collect(toList());
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
		return operands.stream().map(JoinOperand::getOperand).map(Transformation::getTerminals).flatMap(Set::stream).collect(toSet());
	}

	@Override
	@SuppressWarnings("java:S3864")
	public DataSet eval(TransformationScheme scheme)
	{
		LOGGER.debug("Preparing renamed datasets for join");

		Map<JoinOperand, DataSet> values = operands.stream().collect(toMapWithValues(operand -> (DataSet) operand.getOperand().eval(scheme)));

		DataSet result;
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);

		if (usingNames.isEmpty())
			result = joinCaseA(values);
		else
		{
			List<DataSetMetadata> structures = values.values().stream().map(DataSet::getMetadata).collect(toList());

			Set<DataStructureComponent<Identifier, ?, ?>> commonIDs = getCommonIDs(values.size(), structures);
			Set<DataStructureComponent<?, ?, ?>> usingComponents = getUsingComponents(values.get(refOperand).getMetadata(), structures);

			if (commonIDs.equals(usingComponents))
				result = joinCaseA(values);
			else if (commonIDs.containsAll(usingComponents))
				// case B1
				result = joinCaseB1(values, usingComponents);
			else
				result = joinCaseB2(values);
		}

		if (filter != null)
			result = (DataSet) filter.eval(new ThisScope(result));
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
			result = new StreamWrapperDataSet(metadata,
					() -> finalResult.stream().map(dp -> dp.entrySet().stream().map(keepingValue(onlyIf(comp -> comp.getName().contains("#"), comp -> comp.rename(comp.getName().split("#", 2)[1]))))
							.collect(toDataPoint(LineageNode.of(rename, dp.getLineage()), metadata))));
		}

		return result;
	}

	private DataSet joinForB2(DataSet lDs, DataSet rDs, DataSetMetadata resultMetadata)
	{
		Map<Map<String, ScalarValue<?, ?, ?, ?>>, DataPoint> index = rDs.stream().collect(toMapWithKeys(dp -> dp.getValuesByNames(usingNames)));
		Set<String> lDsComponentNames = lDs.getMetadata().stream().map(DataStructureComponent::getName).collect(toSet());
		DataSetMetadata stepMetadata = rDs.getMetadata().stream().filter(component -> !lDsComponentNames.contains(component.getName())).collect(toDataStructure());
		DataSet stepResult = lDs.mapKeepingKeys(stepMetadata, DataPoint::getLineage, dp -> {
			var key = dp.getValuesByNames(usingNames);
			if (index.containsKey(key))
				return dp.combine(index.get(key), this::lineageCombiner);
			else
				return new DataPointBuilder(dp).addAll(resultMetadata.stream().filter(c -> !lDsComponentNames.contains(c.getName())).collect(toMapWithValues(NullValue::instanceFrom)))
						.build(dp.getLineage(), resultMetadata);
		});
		return operator == LEFT_JOIN ? stepResult : stepResult.filter(dp -> index.containsKey(dp.getValuesByNames(usingNames)), identity());
	}

	private DataSet joinCaseB2(final Map<JoinOperand, DataSet> inputDatasets)
	{
		Map<JoinOperand, DataSet> datasets = renameBefore(inputDatasets);
		DataSet refDataset = datasets.get(refOperand);
		Map<JoinOperand, DataSet> datasetsWoutRef = datasets.entrySet().stream().filter(e -> !e.getKey().equals(refOperand)).collect(entriesToMap());
		Map<JoinOperand, DataSetMetadata> datasetsMeta = datasets.entrySet().stream().map(e -> new SimpleEntry<>(e.getKey(), e.getValue().getMetadata())).collect(entriesToMap());
		DataSetMetadata resultMetadata = virtualStructure(datasetsMeta, refDataset.getMetadata(), false);
		if (operator == LEFT_JOIN || operator == INNER_JOIN)
		{
			return new FunctionDataSet<>(resultMetadata, DataSet::stream, datasetsWoutRef.values().stream().reduce(refDataset, (ds1, ds2) -> joinForB2(ds1, ds2, resultMetadata)));
		}
		else
		{
			throw new UnsupportedOperationException(operator.toString().toLowerCase());
		}
	}

	private DataSet joinCaseA(Map<JoinOperand, DataSet> values)
	{
		DataSet result;
		// Find out which component must be renamed inside each dataset
		Map<JoinOperand, DataSet> datasets = renameBefore(values);

		// Case A: join all to reference ds
		LOGGER.debug("Collecting all identifiers");
		Map<DataSet, Set<DataStructureComponent<Identifier, ?, ?>>> ids = datasets.entrySet().stream().filter(entryByKey(op -> op != refOperand)).map(Entry::getValue)
				.map(toEntryWithValue(DataSet::getMetadata)).map(keepingKey(DataSetMetadata::getIDs)).collect(entriesToMap());

		// TODO: Memory hungry!!! Find some way to stream instead of building this big
		// index collection
		LOGGER.debug("Indexing all datapoints");
		Map<DataSet, ? extends Map<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>, DataPoint>> indexes = datasets.entrySet().stream().filter(entryByKey(op -> op != refOperand))
				.map(Entry::getValue).collect(toMapWithValues(ds -> tryWith(ds::stream, stream -> {
					// toMap instead of groupingBy because there's never more than one datapoint in
					// each group
					return (LOGGER.isTraceEnabled() ? stream.peek(dp -> LOGGER.trace("Indexing {}", dp)) : stream).collect(toConcurrentMap(dp -> dp.getValues(Identifier.class), identity()));
				})));

		// Structure before applying any clause
		DataSetMetadata structureBefore = datasets.values().stream().map(DataSet::getMetadata).flatMap(Set::stream).collect(toDataStructure());

		LOGGER.debug("Joining all datapoints");

		result = new FunctionDataSet<>(structureBefore, dataset -> dataset.stream().peek(refDP -> LOGGER.trace("Joining {}", refDP)).map(refDP -> {
			// Get all datapoints from other datasets (there is no more than 1 for each
			// dataset)
			List<DataPoint> otherDPs = datasets.entrySet().stream().filter(entryByKey(op -> op != refOperand)).map(Entry::getValue)
					.map(ds -> indexes.get(ds).get(refDP.getValues(ids.get(ds), Identifier.class))).filter(Objects::nonNull).collect(toList());

			switch (operator)
			{
				case INNER_JOIN:
				case LEFT_JOIN:
					if (otherDPs.size() != indexes.size())
						if (operator == INNER_JOIN)
							return null;
						else
						{
							var dp = otherDPs.stream().reduce(refDP, (dp1, dp2) -> dp1.combine(dp2, this::lineageCombiner));
							return new DataPointBuilder(dp).addAll(structureBefore.stream().filter(Predicate.not(dp::containsKey)).collect(toMapWithValues(NullValue::instanceFrom)))
									.build(refDP.getLineage(), structureBefore);
						}
					else
					{
						// Join all datapoints
						DataPoint accDP = refDP;
						for (DataPoint otherDP : otherDPs)
							accDP = accDP.combine(otherDP, (dp1, dp2) -> lineageCombiner(dp1, dp2));

						LOGGER.trace("Joined {}", accDP);
						return accDP;
					}
				default:
					throw new UnsupportedOperationException(operator + " not implemented");
			}
		}).filter(Objects::nonNull), datasets.get(refOperand));
		return result;
	}

	private DataSet joinCaseB1(Map<JoinOperand, DataSet> datasets, Set<DataStructureComponent<?, ?, ?>> usingComponents)
	{
		// Find out which component must be renamed
		datasets = renameBefore(datasets);
		Map<JoinOperand, DataSetMetadata> structures = datasets.entrySet().stream().map(keepingKey(DataSet::getMetadata)).collect(entriesToMap());

		DataSetMetadata virtualStructure = virtualStructure(structures, datasets.get(refOperand).getMetadata(), true);
		if (!virtualStructure.containsAll(usingComponents))
		{
			Set<DataStructureComponent<?, ?, ?>> missing = new HashSet<>(virtualStructure);
			missing.removeAll(usingComponents);
			throw new VTLMissingComponentsException(missing, virtualStructure);
		}

		// Case B1: join all to reference ds
		LOGGER.debug("Joining using {}", usingComponents);

		throw new UnsupportedOperationException();
	}

	private Lineage lineageCombiner(DataPoint dp1, DataPoint dp2)
	{
		Lineage l1 = dp1.getLineage();
		Lineage l2 = dp2.getLineage();

		String joinString = operands.stream().map(o -> o.getId() != null ? o.getId() : o.getOperand().toString()).collect(joining(", ", operator.toString().toLowerCase() + "(", ")"));

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

	private Map<JoinOperand, DataSet> renameBefore(Map<JoinOperand, DataSet> datasets)
	{
		ConcurrentMap<DataStructureComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
		Set<DataStructureComponent<?, ?, ?>> toBeRenamed = datasets.values().stream().map(DataSet::getMetadata).flatMap(d -> d.stream()).filter(c -> unique.putIfAbsent(c, TRUE) != null)
				.filter(c -> usingNames.isEmpty() ? c.is(NonIdentifier.class) : !usingNames.contains(c.getName())).collect(toSet());

		return datasets.entrySet().stream().map(keepingKey((op, ds) -> {
			String qualifier = op.getId() + "#";
			DataSetMetadata oldStructure = ds.getMetadata();

			// find components that must be renamed and add 'alias#' in front of their name
			DataSetMetadata newStructure = oldStructure.stream().map(c -> toBeRenamed.contains(c) ? c.rename(qualifier + c.getName()) : c)
					.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge).build();

			if (newStructure.equals(oldStructure))
			{
				// no need to change the operand, the structure is the same
				LOGGER.trace("Structure of dataset {} will be kept", oldStructure);
				return ds;
			}

			LOGGER.trace("Structure of dataset {} will be changed to {}", oldStructure, newStructure);

			// Create the dataset operand renaming components in all its datapoints
			return new NamedDataSet(op.getId(),
					new FunctionDataSet<>(newStructure, dataset -> dataset.stream().map(
							dp -> dp.entrySet().stream().map(keepingValue(c -> toBeRenamed.contains(c) ? c.rename(qualifier + c.getName()) : c)).collect(toDataPoint(dp.getLineage(), newStructure))),
							ds));
		})).collect(entriesToMap());
	}

	private DataSet applyClause(DataSetMetadata metadata, TransformationScheme session, DataSet dataset)
	{
		if (apply == null)
			return dataset;

		Set<DataStructureComponent<Measure, ?, ?>> applyComponents = dataset.getMetadata().getMeasures().stream()
				.map(c -> c.getName().replaceAll("^.*#", ""))
				.distinct()
				.map(name -> {
					ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) apply.getMetadata(new JoinApplyScope(session, name, dataset.getMetadata()))).getDomain();
					DataStructureComponent<Measure, ?, ?> component = DataStructureComponentImpl.of(name, Measure.class, domain);
					return component;
				}).collect(toSet());

		DataSetMetadata applyMetadata = dataset.getMetadata().stream().filter(c -> !c.is(Measure.class) || !c.getName().contains("#")).collect(toDataStructure(applyComponents));

		return dataset.mapKeepingKeys(applyMetadata, dp -> LineageNode.of(this, dp.getLineage()),
				dp -> applyComponents.stream().collect(toMapWithValues(c -> (ScalarValue<?, ?, ?, ?>) apply.eval(new JoinApplyScope(session, c.getName(), dp)))));
	}

	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		// check if expressions have aliases
		operands.stream().filter(o -> o.getId() == null).map(JoinOperand::getOperand).findAny().ifPresent(unaliased -> {
			throw new VTLSyntaxException("Join expressions must be aliased: " + unaliased + ".", null);
		});

		// check for duplicate aliases
		operands.stream().map(JoinOperand::getId).collect(groupingByConcurrent(identity(), counting())).entrySet().stream()
				.filter(e -> e.getValue() > 1)
				.map(Entry::getKey)
				.findAny()
				.ifPresent(alias -> {
					throw new VTLSyntaxException("Join aliases must be unique: " + alias);
				});

		Map<JoinOperand, DataSetMetadata> datasetsMeta = new HashMap<>();
		for (JoinOperand op : operands)
			datasetsMeta.put(op, (DataSetMetadata) op.getOperand().getMetadata(scheme));

		Entry<JoinOperand, Boolean> caseAorB1 = isCaseAorB1(datasetsMeta);
		refOperand = caseAorB1.getKey();
		DataSetMetadata result = virtualStructure(datasetsMeta, datasetsMeta.get(refOperand), caseAorB1.getValue());

		LOGGER.info("Joining {} to ({}: {})", operands.stream().filter(op -> op != refOperand).collect(toConcurrentMap(JoinOperand::getId, datasetsMeta::get)), refOperand.getId(),
				datasetsMeta.get(refOperand));

		// modify the result structure as needed
		if (filter != null)
			result = (DataSetMetadata) filter.getMetadata(new ThisScope(result));
		if (apply != null)
		{
			DataSetMetadata applyResult = result;
			Set<DataStructureComponent<Measure, ?, ?>> applyComponents = applyResult.getMeasures().stream()
					.map(c -> c.getName().replaceAll("^.*#", ""))
					.distinct()
					.map(name -> {
						ValueDomainSubset<?, ?> domain = ((ScalarValueMetadata<?, ?>) apply.getMetadata(new JoinApplyScope(scheme, name, applyResult))).getDomain();
						return DataStructureComponentImpl.of(name, Measure.class, domain).asRole(Measure.class);
					}).collect(toSet());

			result = applyResult.stream().filter(c -> !c.is(Measure.class) || !c.getName().contains("#"))
					.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge).addComponents(applyComponents).build();
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
			Map<String, List<String>> sameUnaliasedName = result.stream().map(DataStructureComponent::getName).filter(name -> name.contains("#")).map(name -> name.split("#", 2))
					.collect(groupingByConcurrent(name -> name[1], mapping(name -> name[0], toList())));
			// unalias the unambiguous components and add them to the renaming list
			result = result.stream()
					.map(comp -> comp.getName().contains("#") && sameUnaliasedName.get(comp.getName().split("#", 2)[1]).size() <= 1 ? comp.rename(comp.getName().split("#", 2)[1]) : comp)
					.collect(toDataStructure());
		}

		// find components with the same name from different aliases
		DataSetMetadata finalResult = result;
		result.stream().map(DataStructureComponent::getName).filter(name -> name.contains("#")).findAny().map(name -> name.replaceAll("^.*#", "")).ifPresent(ambiguousComponent -> {
			final Set<DataStructureComponent<?, ?, ?>> sameUnaliasedName = finalResult.stream().filter(c -> c.getName().endsWith("#" + ambiguousComponent)).collect(toSet());
			throw new VTLAmbiguousComponentException(ambiguousComponent, sameUnaliasedName);
		});

		return result;
	}

	private Entry<JoinOperand, Boolean> isCaseAorB1(Map<JoinOperand, DataSetMetadata> datasetsMeta)
	{
		// Determine the superset of all identifiers
		Set<DataStructureComponent<Identifier, ?, ?>> allIDs = datasetsMeta.values().stream().flatMap(ds -> ds.getIDs().stream()).collect(toSet());

		// Get the common ids
		Set<DataStructureComponent<Identifier, ?, ?>> commonIDs = getCommonIDs(datasetsMeta.size(), datasetsMeta.values());

		// Find the reference dataset if there is one. It must contain the superset of
		// ids
		Optional<JoinOperand> refDataSet = datasetsMeta.entrySet().stream().filter(entryByValue(ds -> ds.containsAll(allIDs))).map(Entry::getKey).findFirst();

		// All datasets have the same ids?
		boolean sameIDs = refDataSet.isPresent();
		DataSetMetadata last = null;
		for (DataSetMetadata ds : datasetsMeta.values())
		{
			if (last != null)
				sameIDs &= ds.getIDs().equals(last.getIDs());
			last = ds;
		}

		if (usingNames.isEmpty()) // Case A
			if (operator == INNER_JOIN && refDataSet.isEmpty())
				throw new VTLException(operator.toString().toLowerCase() + " requires one dataset to contain all the identifiers from all other datasets.");
			else if ((operator == LEFT_JOIN || operator == FULL_JOIN) && !sameIDs)
				throw new VTLIncompatibleStructuresException(operator.toString().toLowerCase() + " requires all the input datasets to have the same identifiers",
						datasetsMeta.values().stream().map(DataSetMetadata::getIDs).collect(toList()));
			else if (operator == CROSS_JOIN)
				throw new UnsupportedOperationException(operator.toString().toLowerCase() + " not implemented");
			else
				return new SimpleEntry<>(operator == INNER_JOIN ? refDataSet.get() : operands.get(0), TRUE);
		else if (operator == INNER_JOIN) // case B1-B2
			if (refDataSet.isEmpty())
				throw new VTLException(operator.toString().toLowerCase() + " requires one dataset to contain all the identifiers from all other datasets.");
			else if (commonIDs.stream().map(DataStructureComponent::getName).collect(toSet()).containsAll(usingNames)) // case B1
				return new SimpleEntry<>(refDataSet.get(), TRUE);
			else // case B2
				throw new UnsupportedOperationException("inner_join case B2 not implemented");
		else if (operator == LEFT_JOIN) // case B1-B2
			if (sameIDs && commonIDs.stream().map(DataStructureComponent::getName).collect(toSet()).containsAll(usingNames)) // Case B1
				return new SimpleEntry<>(refDataSet.get(), TRUE);
			else // case B2
			{
				DataSetMetadata leftmost = datasetsMeta.get(operands.get(0));
				List<DataSetMetadata> nonRefStructures = datasetsMeta.values().stream().filter(not(leftmost::equals)).collect(toList());

				Set<DataStructureComponent<?, ?, ?>> usingComponents = getUsingComponents(leftmost, datasetsMeta.values());

				boolean sameIDsWithoutRefDataset = true;
				last = null;
				for (DataSetMetadata ds : nonRefStructures)
				{
					if (last != null)
						sameIDsWithoutRefDataset &= ds.getIDs().equals(last.getIDs());
					last = ds;
				}

				// All the input Data Sets, except the reference Data Set, have the same
				// Identifiers [Id1, … , Idn];
				if (!sameIDsWithoutRefDataset)
					throw new VTLIncompatibleStructuresException("left_join requires all the input datasets, except the leftmost one, to have the same identifiers",
							nonRefStructures.stream().map(DataSetMetadata::getIDs).collect(toList()));

				// The using clause specifies all and only the common Identifiers of the
				// non-reference Data Sets [Id1, … , Idn].
				Set<DataStructureComponent<Identifier, ?, ?>> nonRefCommonIDs = getCommonIDs(nonRefStructures.size(), nonRefStructures);
				if (!nonRefCommonIDs.stream().map(DataStructureComponent::getName).collect(toSet()).equals(new HashSet<>(usingNames)))
					throw new VTLIncompatibleStructuresException("Error in the using clause", nonRefCommonIDs, usingComponents);

				return new SimpleEntry<>(operands.get(0), FALSE);
			}
		else
			throw new VTLException(operator.toString().toLowerCase() + " cannot have a using clause.");
	}

	private Set<DataStructureComponent<?, ?, ?>> getUsingComponents(DataSetMetadata refDataSet, Collection<DataSetMetadata> datasets)
	{
		ConcurrentMap<String, Boolean> unique = new ConcurrentHashMap<>();
		final Set<DataStructureComponent<?, ?, ?>> usingComps = datasets.stream().flatMap(Collection::stream).filter(c -> usingNames.contains(c.getName()))
				.filter(c -> unique.putIfAbsent(c.getName(), TRUE) == null).map(c -> refDataSet.getComponent(c.getName()).orElse(c)).collect(toSet());

		Set<String> missing = new HashSet<>(usingNames);
		for (DataStructureComponent<?, ?, ?> c : usingComps)
			missing.remove(c.getName());

		if (missing.size() > 0)
		{
			unique.clear();
			Set<DataStructureComponent<?, ?, ?>> available = datasets.stream().flatMap(Collection::stream).filter(c -> unique.putIfAbsent(c.getName(), TRUE) == null).collect(toSet());
			throw new VTLMissingComponentsException(missing, available);
		}

		return usingComps;
	}

	private Set<DataStructureComponent<Identifier, ?, ?>> getCommonIDs(int howMany, Collection<DataSetMetadata> structures)
	{
		Set<DataStructureComponent<Identifier, ?, ?>> commonIDs = structures.stream().flatMap(ds -> ds.getIDs().stream()).collect(groupingByConcurrent(c -> c, counting())).entrySet().stream()
				.filter(entryByValue(c -> c.intValue() == howMany)).map(Entry::getKey).collect(toSet());

		return commonIDs;
	}

	private DataSetMetadata virtualStructure(Map<JoinOperand, DataSetMetadata> datasetsMeta, DataSetMetadata refDataSet, boolean isCaseAorB1)
	{
		Set<DataStructureComponent<?, ?, ?>> usingComponents = getUsingComponents(refDataSet, datasetsMeta.values());

		if (isCaseAorB1)
		{
			// Case A: rename all measures and attributes with the same name
			// Case B1: rename all components with the same name except those in the using
			// clause
			ConcurrentMap<DataStructureComponent<?, ?, ?>, Boolean> unique = new ConcurrentHashMap<>();
			Set<DataStructureComponent<?, ?, ?>> toBeRenamed = datasetsMeta.values().stream().flatMap(Collection::stream).filter(c -> unique.putIfAbsent(c, TRUE) != null)
					.filter(c -> /* A */ usingComponents.isEmpty() && c.is(NonIdentifier.class) || /* B1 */ !usingComponents.isEmpty() && !usingComponents.contains(c)).collect(toSet());

			LOGGER.debug("Inner join renames: {}", toBeRenamed);

			// Do the renaming
			DataStructureBuilder builder = new DataStructureBuilder();
			for (Entry<JoinOperand, DataSetMetadata> e : datasetsMeta.entrySet())
				for (DataStructureComponent<?, ?, ?> c : e.getValue())
					if (toBeRenamed.contains(c))
						builder.addComponent(c.rename(e.getKey().getId() + "#" + c.getName()));
					else
						builder.addComponent(c);

			return builder.build();
		}
		else
		{
			// case B2
			if (operator == INNER_JOIN)
				throw new UnsupportedOperationException("Inner join with using not implemented.");
			else if (operator == LEFT_JOIN)
			{
				// Components from using clause
				DataStructureBuilder builder = new DataStructureBuilder(usingComponents);
				// Other IDs of the ref data set
				builder.addComponents(refDataSet.getIDs());

				// Find unique components from all datasets
				ConcurrentMap<String, Boolean> unique = new ConcurrentHashMap<>();
				Set<String> toBeRenamed = datasetsMeta.values().stream().flatMap(Collection::stream).filter(c -> unique.putIfAbsent(c.getName(), TRUE) != null)
						.filter(c -> !usingNames.contains(c.getName()) && !(c.is(Identifier.class) && refDataSet.contains(c))).map(DataStructureComponent::getName).collect(toSet());

				// add remaining components
				for (Entry<JoinOperand, DataSetMetadata> e : datasetsMeta.entrySet())
					for (DataStructureComponent<?, ?, ?> c : e.getValue())
						// rename non-unique components
						if (toBeRenamed.contains(c.getName()))
							builder.addComponent(c.rename(e.getKey().getId() + "#" + c.getName()));
						// add component from reference dataset if it has one, otherwise add it
						else if (!usingNames.contains(c.getName()))
							builder.addComponent(refDataSet.getComponent(c.getName()).orElse(c));

				return builder.build();
			}
			else
				throw new UnsupportedOperationException(operator.toString().toLowerCase());
		}

	}

	@Override
	public String toString()
	{
		return operator.toString().toLowerCase() + "(" + operands.stream().map(Object::toString).collect(joining(", "))
				+ (usingNames.isEmpty() ? "" : " using " + usingNames.stream().collect(joining(", "))) + (filter != null ? " " + filter : "") + (apply != null ? " apply " + apply : "")
				+ (calc != null ? " " + calc : "") + (aggr != null ? " " + aggr : "") + (keepOrDrop != null ? " " + keepOrDrop : "") + (rename != null ? " " + rename : "") + ")";
	}
}