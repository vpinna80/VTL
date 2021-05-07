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
package it.bancaditalia.oss.vtl.impl.transform.number;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.reverseIf;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.singleton;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toConcurrentMap;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.types.data.IntegerValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator;
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
import it.bancaditalia.oss.vtl.model.data.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

public class ArithmeticTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;
	@SuppressWarnings("unused")
	private final static Logger LOGGER = LoggerFactory.getLogger(ArithmeticTransformation.class);
	private final ArithmeticOperator operator;
	
	public ArithmeticTransformation(ArithmeticOperator operator, Transformation left, Transformation right)
	{
		super(left, right);

		this.operator = operator;
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			if (INTEGERDS.isAssignableFrom(left.getDomain()) && INTEGERDS.isAssignableFrom(left.getDomain()))
				return NullValue.instance(INTEGERDS);
			else
				return NullValue.instance(NUMBERDS);
		else if (left instanceof IntegerValue && right instanceof IntegerValue)
			return getOperator().applyAsInt((NumberValue<?, ?, ?, ?>) left, (NumberValue<?, ?, ?, ?>) right);
		else
			return getOperator().applyAsDouble((NumberValue<?, ?, ?, ?>) left, (NumberValue<?, ?, ?, ?>) right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		Set<String> measureNames = dataset.getComponents(Measure.class, NUMBERDS).stream().map(DataStructureComponent::getName).collect(toSet());
		ScalarValue<?, ?, EntireNumberDomainSubset, NumberDomain> castedScalar = NUMBERDS.cast(scalar);
		
		Predicate<String> bothIntegers = name -> ((DataSetMetadata) metadata).getComponent(name)
					.map(DataStructureComponent::getDomain)
					.map(c -> INTEGERDS.isAssignableFrom(c))
					.orElseThrow(() -> new VTLMissingComponentsException(name, (DataSetMetadata) metadata)) 
				&& INTEGERDS.isAssignableFrom(scalar.getDomain());
		
		// must remember which is the left operand because some operators are not commutative
		BiFunction<? super DataPoint, ? super String, ScalarValue<?, ?, ?, ?>> finisher = (dp, name) -> 
			reverseIf(bothIntegers.test(name) ? getOperator()::applyAsInt : getOperator()::applyAsDouble, !datasetIsLeftOp)
				.apply(NUMBERDS.cast(dp.get(dataset.getComponent(name).get())), castedScalar);
		
		return dataset.mapKeepingKeys((DataSetMetadata) metadata, dp -> LineageNode.of(this, dp.getLineage(), getLeftOperand().getLineage()), dp -> Utils.getStream(measureNames)
							.collect(toConcurrentMap(name -> ((DataSetMetadata) metadata)
									.getComponent(name)
									.map(c -> c.as(Measure.class))
									.orElseThrow(() -> new VTLMissingComponentsException(name, dp.keySet())
								), name -> finisher.apply(dp, name))));
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		// index (as right operand) the one with less keys and stream the other (as left operand)
		boolean swap = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));
		DataSet streamed = swap ? right : left;
		DataSet indexed = swap ? left : right;

		if (metadata == null)
		{
			DataStructureComponent<Measure, ? extends NumberDomainSubset<?, ?>, NumberDomain> leftMeasure = streamed.getComponents(Measure.class, NUMBERDS).iterator().next();
			DataStructureComponent<Measure, ? extends NumberDomainSubset<?, ?>, NumberDomain> rightMeasure = indexed.getComponents(Measure.class, NUMBERDS).iterator().next();
			DataStructureComponentImpl<Measure, ? extends NumberDomainSubset<?, ?>, ?> resultComp;
			if (INTEGERDS.isAssignableFrom(leftMeasure.getDomain()) && INTEGERDS.isAssignableFrom(rightMeasure.getDomain()))
				resultComp = new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS);
			else
				resultComp = new DataStructureComponentImpl<>(NUMBERDS.getVarName(), Measure.class, NUMBERDS);
			
			DataSetMetadata newStructure = new DataStructureBuilder(streamed.getComponents(Identifier.class))
					.addComponent(resultComp)
					.build();
			
			boolean intResult = INTEGERDS.isAssignableFrom(resultComp.getDomain());
			return streamed.mappedJoin(newStructure, indexed,  
					(dpl, dpr) -> new DataPointBuilder()
						.add(resultComp, compute(swap, intResult, dpl.get(leftMeasure), dpr.get(rightMeasure)))
						.addAll(dpl.getValues(Identifier.class))
						.addAll(dpr.getValues(Identifier.class))
						.build(LineageNode.of(this, dpl.getLineage(), dpr.getLineage()), newStructure));
		}
		else
		{
			Set<DataStructureComponent<Measure, ?, ?>> resultMeasures = ((DataSetMetadata) metadata).getComponents(Measure.class);
			
			if (resultMeasures.size() == 1)
			{
				DataStructureComponent<Measure, ?, ?> resultMeasure = resultMeasures.iterator().next(); 
				DataStructureComponent<Measure, ?, ?> streamedMeasure = streamed.getComponents(Measure.class).iterator().next(); 
				DataStructureComponent<Measure, ?, ?> indexedMeasure = indexed.getComponents(Measure.class).iterator().next(); 
				
				// at component level, source measures can have different names but there is only 1 for each operand
				return streamed.mappedJoin((DataSetMetadata) metadata, indexed, 
						(dpl, dpr) -> new DataPointBuilder()
							.add(resultMeasure, compute(swap, INTEGERDS.isAssignableFrom(resultMeasure.getDomain()), 
									dpl.get(streamedMeasure), 
									dpr.get(indexedMeasure))
							).addAll(dpl.getValues(Identifier.class))
							.addAll(dpr.getValues(Identifier.class))
							.build(LineageNode.of(this, dpl.getLineage(), dpr.getLineage()), (DataSetMetadata) metadata));
			}
			else
				// Scan the dataset with less identifiers and find the matches
				return streamed.mappedJoin((DataSetMetadata) metadata, indexed, 
					(dpl, dpr) -> new DataPointBuilder(resultMeasures.stream()
							.map(toEntryWithValue(compToCalc -> compute(swap, INTEGERDS.isAssignableFrom(compToCalc.getDomain()), 
									dpl.get(streamed.getComponent(compToCalc.getName()).get()), 
									dpr.get(indexed.getComponent(compToCalc.getName()).get()))
							)).collect(entriesToMap()))		
						.addAll(dpl.getValues(Identifier.class))
						.addAll(dpr.getValues(Identifier.class))
						.build(LineageNode.of(this, dpl.getLineage(), dpr.getLineage()), (DataSetMetadata) metadata));
		}
	}

	// take account of the order of parameters because some operators are not commutative 
	private ScalarValue<?, ?, ?, ?> compute(boolean swap, boolean intResult, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return intResult ? NullValue.instance(INTEGERDS) : NullValue.instance(NUMBERDS);
		
		return reverseIf(intResult ? getOperator()::applyAsInt : getOperator()::applyAsDouble, swap)
			.apply((NumberValue<?, ?, ?, ?>) left, (NumberValue<?, ?, ?, ?>) right);
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		ValueDomain domainLeft = left.getDomain();
		ValueDomain domainRight = right.getDomain();
		
		if (INTEGERDS.isAssignableFrom(domainLeft) && INTEGERDS.isAssignableFrom(domainRight))
			return INTEGER;
		else if (NUMBERDS.isAssignableFrom(domainLeft) && NUMBERDS.isAssignableFrom(domainRight))
			return NUMBER;
		else if (!NUMBERDS.isAssignableFrom(domainLeft))
			throw new VTLIncompatibleTypesException(getOperator().toString(), NUMBERDS, domainLeft);
		else 
			throw new VTLIncompatibleTypesException(getOperator().toString(), NUMBERDS, domainRight);
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (dataset.getComponents(Measure.class).size() == 0)
			throw new UnsupportedOperationException("Expected at least 1 measure but found none.");
		if (dataset.getComponents(Measure.class).stream().anyMatch(c -> !NUMBERDS.isAssignableFrom(c.getDomain())))
			throw new UnsupportedOperationException("Expected only numeric measures but found: " + dataset.getComponents(Measure.class));
		if (INTEGERDS.isAssignableFrom(scalar.getDomain()))
			return dataset;
		
		// Sum to float, convert integer measures to floating point
		return dataset.stream()
				.map(c -> c.is(Measure.class) && INTEGERDS.isAssignableFrom(c.getDomain()) 
						? new DataStructureComponentImpl<>(c.getName(), Measure.class, NUMBERDS) : c)
				.reduce(new DataStructureBuilder(), DataStructureBuilder::addComponent, DataStructureBuilder::merge)
				.build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasures = left.getComponents(Measure.class);
		final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> rightMeasures = right.getComponents(Measure.class);
		
		if (leftMeasures.size() == 0)
			throw new VTLExpectedComponentException(Measure.class, NUMBERDS, leftMeasures);
		if (rightMeasures.size() == 0)
			throw new VTLExpectedComponentException(Measure.class, NUMBERDS, rightMeasures);

//		DataStructureComponent<? extends Measure, ?, ?> firstLeft = leftMeasures.iterator().next();
//		DataStructureComponent<? extends Measure, ?, ?> firstRight = rightMeasures.iterator().next();
//		
//		ValueDomainSubset<?, ?> firstLeftDomain = firstLeft.getDomain();
//		ValueDomainSubset<?, ?> firstRightDomain = firstRight.getDomain();
//		
//		boolean areFirstCompatible = !firstLeft.getName().equals(firstRight.getName()) && 
//				(firstLeftDomain.isAssignableFrom(firstRightDomain) || firstRight.getDomain().isAssignableFrom(firstLeft.getDomain()));
//		
//		if (areFirstCompatible && leftMeasures.size() == 1 && rightMeasures.size() == 1)
//			return NUMBER;

		if (!left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class))
				&& !right.getComponents(Identifier.class).containsAll(left.getComponents(Identifier.class)))
			throw new UnsupportedOperationException("One dataset must have all the identifiers of the other.");

		// check if measures are the same, unless we are at component level
		Set<DataStructureComponent<? extends Measure, ?, ?>> resultMeasures;
		if (leftMeasures.size() == 1 && rightMeasures.size() == 1 && !leftMeasures.equals(rightMeasures))
			resultMeasures = singleton(new DataStructureComponentImpl<>(NUMBERDS.getVarName(), Measure.class, NUMBERDS));
		else
		{
			Map<String, ? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasuresMap = Utils.getStream(leftMeasures).collect(toMap(DataStructureComponent::getName, identity()));
			Map<String, ? extends DataStructureComponent<? extends Measure, ?, ?>> rightMeasuresMap = Utils.getStream(rightMeasures).collect(toMap(DataStructureComponent::getName, identity()));
			
			resultMeasures = Stream.concat(leftMeasuresMap.keySet().stream(), rightMeasuresMap.keySet().stream())
				.map(name -> new SimpleEntry<>(leftMeasuresMap.get(name), rightMeasuresMap.get(name)))
				.peek(splittingConsumer((lm, rm) -> 
					{
						if (lm == null)
							throw new VTLMissingComponentsException(rm, leftMeasures);
						if (rm == null)
							throw new VTLMissingComponentsException(lm, rightMeasures);
						if (!NUMBERDS.isAssignableFrom(lm.getDomain()))
							throw new UnsupportedOperationException("Expected numeric measure but found: " + lm);
						if (!NUMBERDS.isAssignableFrom(rm.getDomain()))
							throw new UnsupportedOperationException("Expected numeric measure but found: " + rm);
					}))
				// if at least one components is floating point, use floating point otherwise integer
				.map(splitting((lm, rm) -> INTEGERDS.isAssignableFrom(lm.getDomain()) 
						? INTEGERDS.isAssignableFrom(rm.getDomain())
						? lm : rm : lm))
				.collect(toSet());
		}
		
		return new DataStructureBuilder().addComponents(left.getComponents(Identifier.class))
				.addComponents(right.getComponents(Identifier.class))
				.addComponents(resultMeasures)
				.build();
	}
	
	@Override
	public String toString()
	{
		if (getOperator().isInfix())
			return getLeftOperand().toString() + getOperator() + getRightOperand();
		else
			return getOperator() + "(" + getLeftOperand().toString() + ", " + getRightOperand() + ")";
	}

	public ArithmeticOperator getOperator()
	{
		return operator;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof ArithmeticTransformation)) return false;
		ArithmeticTransformation other = (ArithmeticTransformation) obj;
		if (operator != other.operator) return false;
		return true;
	}
}
