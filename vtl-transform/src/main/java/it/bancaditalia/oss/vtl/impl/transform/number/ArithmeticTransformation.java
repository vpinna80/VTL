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

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.INT_VAR;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.NUM_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBER;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIV;
import static it.bancaditalia.oss.vtl.util.SerBiFunction.reverseIf;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLExpectedRoleException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.scope.ThisScope;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

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
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata resultMetadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left.isNull() || right.isNull())
			if (operator != DIV && left.getDomain() instanceof IntegerDomain && right.getDomain() instanceof IntegerDomain)
				return NullValue.instance(INTEGERDS);
			else
				return NullValue.instance(NUMBERDS);
		else 
			return operator != DIV && left.getDomain() instanceof IntegerDomain && right.getDomain() instanceof IntegerDomain
					? operator.applyAsInteger(left, right) 
					: operator.applyAsNumber(left, right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata resultMetadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataSetStructure dsMeta = (DataSetStructure) resultMetadata;
		Set<VTLAlias> measureNames = dataset.getMetadata().getComponents(Measure.class, NUMBERDS).stream()
				.map(DataSetComponent::getAlias)
				.collect(toSet());
		
		// check if both a measure and the scalar are integers
		SerPredicate<DataSetComponent<?, ?, ?>> bothIntegers = comp -> scalar.getDomain() instanceof IntegerDomain
				&& comp.getDomain() instanceof IntegerDomain;
		
		// must remember which is the left operand because some operators are not commutative
		SerBiFunction<DataPoint, DataSetComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> finisher = (dp, comp) -> 
			reverseIf(operator != DIV && bothIntegers.test(comp) ? operator::applyAsInteger : operator::applyAsNumber, !datasetIsLeftOp)
				.apply(dp.get(comp), scalar);
		
		SerUnaryOperator<Lineage> enricher = LineageNode.lineageEnricher(this);
		return dataset.mapKeepingKeys(dsMeta, lineage -> enricher.apply(LineageCall.of(lineage)), dp -> { 
				Map<DataSetComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>();
				for (VTLAlias name: measureNames)
				{
					DataSetComponent<Measure, ?, ?> comp = dsMeta.getComponent(name)
							.orElseThrow(() -> new VTLMissingComponentsException(dp.keySet(), name)).asRole(Measure.class);
					result.put(comp, finisher.apply(dp, comp));
				}
				return result;
			});
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata resultMetadata, DataSet left, DataSet right)
	{
		// index (as right operand) the one with less keys and stream the other (as left operand)
		boolean swap = left.getMetadata().getIDs().containsAll(right.getMetadata().getIDs());
		DataSet streamed = swap ? right : left;
		DataSet indexed = swap ? left : right;
		ArithmeticOperator operator = this.operator;

		if (resultMetadata == null)
		{
			DataSetComponent<Measure, ?, ?> leftMeasure = streamed.getMetadata().getComponents(Measure.class, NUMBERDS).iterator().next();
			DataSetComponent<Measure, ?, ?> rightMeasure = indexed.getMetadata().getComponents(Measure.class, NUMBERDS).iterator().next();
			DataSetComponent<Measure, ?, ?> resultComp;
			if (operator != DIV && leftMeasure.getDomain() instanceof IntegerDomain && rightMeasure.getDomain() instanceof IntegerDomain)
				resultComp = INT_VAR;
			else
				resultComp = NUM_VAR;
			
			DataSetStructure newStructure = new DataSetStructureBuilder(streamed.getMetadata().getIDs())
					.addComponent(resultComp)
					.build();
			
			boolean intResult = resultComp.getDomain() instanceof IntegerDomain;
			return streamed.mappedJoin(newStructure, indexed, (dpl, dpr) -> new DataPointBuilder()
				.add(resultComp, compute(operator, swap, intResult, dpl.get(leftMeasure), dpr.get(rightMeasure)))
				.addAll(dpl.getValues(Identifier.class))
				.addAll(dpr.getValues(Identifier.class))
				.build(LineageNode.of(this, LineageCall.of(dpl.getLineage(), dpr.getLineage())), newStructure));
		}
		else
		{
			Set<DataSetComponent<Measure, ?, ?>> resultMeasures = ((DataSetStructure) resultMetadata).getMeasures();
			
			if (resultMeasures.size() == 1)
			{
				DataSetComponent<Measure, ?, ?> resultMeasure = resultMeasures.iterator().next(); 
				DataSetComponent<Measure, ?, ?> streamedMeasure = streamed.getMetadata().getMeasures().iterator().next(); 
				DataSetComponent<Measure, ?, ?> indexedMeasure = indexed.getMetadata().getMeasures().iterator().next(); 
				
				// at component level, source measures can have different names but there is only 1 for each operand
				return streamed.mappedJoin((DataSetStructure) resultMetadata, indexed, (dpl, dpr) -> {
						boolean isResultInt = operator != DIV && resultMeasure.getDomain() instanceof IntegerDomain;
						ScalarValue<?, ?, ?, ?> leftVal = dpl.get(streamedMeasure);
						ScalarValue<?, ?, ?, ?> rightVal = dpr.get(indexedMeasure);
						return new DataPointBuilder()
								.add(resultMeasure, compute(operator, swap, isResultInt, leftVal, rightVal))
								.addAll(dpl.getValues(Identifier.class))
								.addAll(dpr.getValues(Identifier.class))
								.build(LineageNode.of(ArithmeticTransformation.this, LineageCall.of(dpl.getLineage(), dpr.getLineage())), (DataSetStructure) resultMetadata);						
					});
			}
			else
			{
				// Scan the dataset with less identifiers and find the matches
				DataSetStructure streamedStructure = streamed.getMetadata();
				DataSetStructure indexedStructure = indexed.getMetadata();
				return streamed.mappedJoin((DataSetStructure) resultMetadata, indexed, (dpl, dpr) -> {
						return new DataPointBuilder(resultMeasures.stream()
								.map(toEntryWithValue(compToCalc -> {
									return compute(operator, swap, compToCalc.getDomain() instanceof IntegerDomain, 
											dpl.get(streamedStructure.getComponent(compToCalc.getAlias()).get()), 
											dpr.get(indexedStructure.getComponent(compToCalc.getAlias()).get()));
								})).collect(entriesToMap()))		
							.addAll(dpl.getValues(Identifier.class))
							.addAll(dpr.getValues(Identifier.class))
							.build(LineageNode.of(this, LineageCall.of(dpl.getLineage(), dpr.getLineage())), (DataSetStructure) resultMetadata);
					});
			}
		}
	}

	// take account of the order of parameters because some operators are not commutative 
	private static ScalarValue<?, ?, ?, ?> compute(ArithmeticOperator operator, boolean swap, boolean intResult, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		if (left.isNull() || right.isNull())
			return intResult ? NullValue.instance(INTEGERDS) : NullValue.instance(NUMBERDS);
		
		return reverseIf(intResult && operator != DIV ? operator::applyAsInteger : operator::applyAsNumber, swap)
			.apply((NumberValue<?, ?, ?, ?>) left, (NumberValue<?, ?, ?, ?>) right);
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		ValueDomain domainLeft = left.getDomain();
		ValueDomain domainRight = right.getDomain();
		
		if (domainLeft instanceof IntegerDomain && domainRight instanceof IntegerDomain)
			return INTEGER;
		else if (NUMBERDS.isAssignableFrom(domainLeft) && NUMBERDS.isAssignableFrom(domainRight))
			return NUMBER;
		else
			throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, 
					!NUMBERDS.isAssignableFrom(domainLeft) ? domainLeft : domainRight);
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (dataset.getMeasures().size() == 0)
			throw new VTLExpectedRoleException(Measure.class, dataset);
		
		dataset.getMeasures().stream()
			.filter(c -> !NUMBERDS.isAssignableFrom(c.getDomain()))
			.map(c -> new VTLIncompatibleTypesException(operator.toString().toLowerCase(), c, NUMBERDS))
			.forEach(e -> { throw e; });

		ValueDomainSubset<?, ?> scalarDomain = scalar.getDomain();
		if (!(scalarDomain instanceof IntegerDomain))
		{
			if (!NUMBERDS.isAssignableFrom(scalarDomain))
				throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), NUMBERDS, scalarDomain);
			
			for (DataSetComponent<Measure, ?, ?> measure: dataset.getMeasures())
				if (measure.getDomain() instanceof IntegerDomain)
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), NUMBERDS, measure);
		}
		
		return new DataSetStructureBuilder(dataset)
				.removeComponents(dataset.getComponents(Attribute.class))
				.addComponents(dataset.getComponents(ViralAttribute.class))
				.build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(TransformationScheme scheme, DataSetStructure left, DataSetStructure right)
	{
		Set<DataSetComponent<Measure, ?, ?>> leftMeasures = left.getMeasures();
		Set<DataSetComponent<Measure, ?, ?>> rightMeasures = right.getMeasures();
		
		if (leftMeasures.size() == 0)
			throw new VTLExpectedRoleException(Measure.class, NUMBERDS, leftMeasures);
		if (rightMeasures.size() == 0)
			throw new VTLExpectedRoleException(Measure.class, NUMBERDS, rightMeasures);

		if (!left.getIDs().containsAll(right.getIDs())
				&& !right.getIDs().containsAll(left.getIDs()))
			throw new VTLInvariantIdentifiersException(operator.toString(), left.getIDs(), right.getIDs());

		// check if measures are the same, unless we are at component level
		Set<DataSetComponent<Measure, ?, ?>> resultMeasures;
		if (leftMeasures.size() == 1 && rightMeasures.size() == 1 && !leftMeasures.equals(rightMeasures))
		{
			final DataSetComponent<Measure, ?, ?> leftMeasure = leftMeasures.iterator().next();
			final DataSetComponent<Measure, ?, ?> rightMeasure = rightMeasures.iterator().next();
			ValueDomainSubset<?, ?> leftDomain = leftMeasure.getDomain();
			ValueDomainSubset<?, ?> rightDomain = rightMeasure.getDomain();

			if (!NUMBERDS.isAssignableFrom(leftDomain))
				throw new VTLIncompatibleTypesException(operator.toString(), leftMeasure, NUMBERDS);
			if (!NUMBERDS.isAssignableFrom(rightDomain))
				throw new VTLIncompatibleTypesException(operator.toString(), rightMeasure, NUMBERDS);
			if (!(scheme instanceof ThisScope) && !leftMeasure.getAlias().equals(rightMeasure.getAlias()))
				throw new VTLIncompatibleTypesException(operator.toString(), leftMeasure, rightMeasure);

			resultMeasures = rightDomain instanceof IntegerDomain ? leftMeasures : rightMeasures;
		}
		else
		{
			Map<VTLAlias, DataSetComponent<Measure, ?, ?>> leftMeasuresMap = leftMeasures.stream().collect(toMapWithKeys(m -> m.getAlias()));
			Map<VTLAlias, DataSetComponent<Measure, ?, ?>> rightMeasuresMap = rightMeasures.stream().collect(toMapWithKeys(m -> m.getAlias()));
			
			resultMeasures = Stream.concat(leftMeasuresMap.keySet().stream(), rightMeasuresMap.keySet().stream())
				.map(name -> new SimpleEntry<>(leftMeasuresMap.get(name), rightMeasuresMap.get(name)))
				.peek(splittingConsumer((lm, rm) -> {
						if (lm == null)
							throw new VTLMissingComponentsException(leftMeasures, rm);
						if (rm == null)
							throw new VTLMissingComponentsException(rightMeasures, lm);
						if (!NUMBERDS.isAssignableFrom(lm.getDomain()))
							throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, lm);
						if (!NUMBERDS.isAssignableFrom(rm.getDomain()))
							throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, rm);
					}))
				// if at least one components is floating point, use floating point otherwise integer
				.map(splitting((lm, rm) -> lm.getDomain() instanceof IntegerDomain 
						? rm.getDomain() instanceof IntegerDomain
						? lm : rm : lm))
				.collect(toSet());
		}
		
		return new DataSetStructureBuilder()
			.addComponents(left.getIDs())
			.addComponents(right.getIDs())
			.addComponents(resultMeasures)
			.addComponents(left.getComponents(ViralAttribute.class))
			.addComponents(right.getComponents(ViralAttribute.class))
			.build();
	}
	
	@Override
	public String toString()
	{
		if (operator.isInfix())
			return getLeftOperand().toString() + operator + getRightOperand();
		else
			return operator + "(" + getLeftOperand().toString() + ", " + getRightOperand() + ")";
	}
}
