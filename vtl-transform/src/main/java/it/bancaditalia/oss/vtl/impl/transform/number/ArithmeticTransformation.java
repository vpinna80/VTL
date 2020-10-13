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
package it.bancaditalia.oss.vtl.impl.transform.number;

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.util.Utils.reverseIf;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
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
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
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
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
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
	protected VTLValue evalTwoScalars(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		if (left instanceof IntegerValue && right instanceof IntegerValue)
			return getOperator().applyAsInt((NumberValue<?, ?, ?>) left, (NumberValue<?, ?, ?>) right);
		else
			return getOperator().applyAsDouble((NumberValue<?, ?, ?>) left, (NumberValue<?, ?, ?>) right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?> scalar)
	{
		DataSetMetadata metadata = (DataSetMetadata) getMetadata();
		Set<String> measureNames = dataset.getComponents(Measure.class, NUMBERDS).stream().map(DataStructureComponent::getName).collect(toSet());

		Predicate<String> bothIntegers = name -> metadata.getComponent(name)
					.map(DataStructureComponent::getDomain)
					.map(c -> INTEGERDS.isAssignableFrom(c))
					.orElseThrow(() -> new VTLMissingComponentsException(name, metadata)) 
				&& INTEGERDS.isAssignableFrom(scalar.getDomain());
		
		// must remember which is the left operand because some operators are not commutative
		BiFunction<? super DataPoint, ? super String, ScalarValue<?, ?, ?>> finisher = (dp, name) -> 
			reverseIf(!datasetIsLeftOp, bothIntegers.test(name) ? getOperator()::applyAsInt : getOperator()::applyAsDouble)
				.apply(dp.get(dataset.getComponent(name)
						.map(c -> c.as(NUMBERDS))
						.orElseThrow(() -> new VTLMissingComponentsException(name, metadata))
						), scalar);
		
		return dataset.mapKeepingKeys(metadata, dp -> Utils.getStream(measureNames)
				.collect(toConcurrentMap(name -> metadata.getComponent(name)
						.map(c -> c.as(Measure.class))
						.orElseThrow(() -> new VTLMissingComponentsException(name, metadata)), name -> finisher.apply(dp, name))));
	}

	@Override
	protected VTLValue evalTwoDatasets(DataSet left, DataSet right)
	{
		DataSetMetadata metadata = (DataSetMetadata) getMetadata();
		// index (as right operand) the one with less keys and stream the other (as left operand)
		boolean swap = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));
		DataSet streamed = swap ? right : left;
		DataSet indexed = swap ? left : right;

		if (metadata == null)
		{
			DataStructureComponent<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> leftMeasure = streamed.getComponents(Measure.class, NUMBERDS).iterator().next();
			DataStructureComponent<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> rightMeasure = indexed.getComponents(Measure.class, NUMBERDS).iterator().next();
			DataStructureComponentImpl<Measure, ? extends NumberDomainSubset<? extends NumberDomain>, ? extends NumberDomain> resultComp;
			if (INTEGERDS.isAssignableFrom(leftMeasure.getDomain()) && INTEGERDS.isAssignableFrom(rightMeasure.getDomain()))
				resultComp = new DataStructureComponentImpl<>(INTEGERDS.getVarName(), Measure.class, INTEGERDS);
			else
				resultComp = new DataStructureComponentImpl<>(NUMBERDS.getVarName(), Measure.class, NUMBERDS);
			
			DataSetMetadata newStructure = new DataStructureBuilder(streamed.getComponents(Identifier.class))
					.addComponent(resultComp)
					.build();
			
			boolean intResult = INTEGERDS.isAssignableFrom(resultComp.getDomain());
			return streamed.filteredMappedJoin(newStructure, indexed, (dpl, dpr) -> true /* no filter */, (dpl, dpr) -> new DataPointBuilder()
						.add(resultComp, compute(swap, intResult, dpl.get(leftMeasure), dpr.get(rightMeasure)))
						.addAll(dpl.getValues(Identifier.class))
						.addAll(dpr.getValues(Identifier.class))
						.build(newStructure));
		}
		else
		{
			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> resultMeasures = metadata.getComponents(Measure.class);
			// Scan the dataset with less identifiers and find the matches
			
			return streamed.filteredMappedJoin(metadata, indexed, (dpl, dpr) -> true /* no filter */,
				(dpl, dpr) -> new DataPointBuilder(resultMeasures.stream()
						.map(compToCalc -> new SimpleEntry<>(compToCalc, compute(swap, INTEGERDS.isAssignableFrom(compToCalc.getDomain()), 
								dpl.get(streamed.getComponent(compToCalc.getName()).get()), 
								dpr.get(indexed.getComponent(compToCalc.getName()).get()))))
						.collect(Utils.entriesToMap()))		
					.addAll(dpl.getValues(Identifier.class))
					.addAll(dpr.getValues(Identifier.class))
					//.add(resultMeasure, finalOperator.apply(dp1.get(indexedMeasure), dp2.get(streamedMeasure)))
					.build(metadata));
		}
	}

	// take account of the order of parameters because some operators are not commutative 
	private ScalarValue<?, ?, ?> compute(boolean swap, boolean intResult, ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		if (left instanceof NullValue || right instanceof NullValue)
			return NullValue.instance((NumberDomainSubset<? extends NumberDomain>)(intResult ? INTEGERDS : NUMBERDS));
		
		return reverseIf(swap, intResult
					? getOperator()::applyAsInt 
					: getOperator()::applyAsDouble)
			.apply((NumberValue<?, ?, ?>) left, (NumberValue<?, ?, ?>) right);
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?> left, ScalarValueMetadata<?> right)
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
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?> scalar)
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

		DataStructureComponent<? extends Measure, ?, ?> firstLeft = leftMeasures.iterator().next();
		DataStructureComponent<? extends Measure, ?, ?> firstRight = rightMeasures.iterator().next();
		
		ValueDomainSubset<?> firstLeftDomain = firstLeft.getDomain();
		ValueDomainSubset<?> firstRightDomain = firstRight.getDomain();
		
		boolean areFirstCompatible = !firstLeft.getName().equals(firstRight.getName()) && 
				(firstLeftDomain.isAssignableFrom(firstRightDomain) || firstRight.getDomain().isAssignableFrom(firstLeft.getDomain()));
		
		if (areFirstCompatible && leftMeasures.size() == 1 && rightMeasures.size() == 1)
			return NUMBER;

		if (!left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class))
				&& !right.getComponents(Identifier.class).containsAll(left.getComponents(Identifier.class)))
			throw new UnsupportedOperationException("One dataset must have all the identifiers of the other.");

		Map<String, ? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasuresMap = Utils.getStream(leftMeasures).collect(toMap(DataStructureComponent::getName, identity()));
		Map<String, ? extends DataStructureComponent<? extends Measure, ?, ?>> rightMeasuresMap = Utils.getStream(rightMeasures).collect(toMap(DataStructureComponent::getName, identity()));
		
		Set<DataStructureComponent<? extends Measure, ?, ?>> measures = Stream.concat(leftMeasuresMap.keySet().stream(), rightMeasuresMap.keySet().stream())
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
			.map(splitting((lm, rm) -> INTEGERDS.isAssignableFrom(lm.getDomain()) 
					? INTEGERDS.isAssignableFrom(rm.getDomain())
					? lm : rm : lm))
			.collect(toSet());
		
		return new DataStructureBuilder().addComponents(left.getComponents(Identifier.class))
				.addComponents(right.getComponents(Identifier.class))
				.addComponents(measures)
				.build();
	}
	
	@Override
	public String toString()
	{
		if (getOperator().isInfix())
			return leftOperand.toString() + getOperator() + rightOperand;
		else
			return getOperator() + "(" + leftOperand.toString() + ", " + rightOperand + ")";
	}

	public ArithmeticOperator getOperator()
	{
		return operator;
	}
}
