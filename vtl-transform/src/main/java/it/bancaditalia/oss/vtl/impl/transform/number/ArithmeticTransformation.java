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
import static it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator.DIV;
import static it.bancaditalia.oss.vtl.util.SerBiFunction.reverseIf;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toMapWithKeys;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toSet;
import static it.bancaditalia.oss.vtl.util.Utils.splitting;
import static it.bancaditalia.oss.vtl.util.Utils.splittingConsumer;
import static it.bancaditalia.oss.vtl.util.Utils.toEntryWithValue;
import static java.util.Collections.singleton;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.exceptions.VTLExpectedRoleException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.impl.types.operators.ArithmeticOperator;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.ViralAttribute;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.ValueDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerBiFunction;
import it.bancaditalia.oss.vtl.util.SerPredicate;

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
		if (left.isNull() || right.isNull())
			if (operator != DIV && INTEGERDS.isAssignableFrom(left.getDomain()) && INTEGERDS.isAssignableFrom(right.getDomain()))
				return NullValue.instance(INTEGERDS);
			else
				return NullValue.instance(NUMBERDS);
		else 
			return operator != DIV && INTEGERDS.isAssignableFrom(left.getDomain()) && INTEGERDS.isAssignableFrom(right.getDomain())
					? operator.applyAsInteger(left, right) 
					: operator.applyAsNumber(left, right);
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		DataSetMetadata dsMeta = (DataSetMetadata) metadata;
		Set<VTLAlias> measureNames = dataset.getMetadata().getComponents(Measure.class, NUMBERDS).stream()
				.map(DataStructureComponent::getVariable)
				.map(Variable::getAlias)
				.collect(toSet());
		
		// check if both a measure and the scalar are integers
		SerPredicate<DataStructureComponent<?, ?, ?>> bothIntegers = comp -> INTEGERDS.isAssignableFrom(scalar.getDomain())
				&& INTEGERDS.isAssignableFrom(comp.getVariable().getDomain());
		
		// must remember which is the left operand because some operators are not commutative
		SerBiFunction<DataPoint, DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> finisher = (dp, comp) -> 
			reverseIf(operator != DIV && bothIntegers.test(comp) ? operator::applyAsInteger : operator::applyAsNumber, !datasetIsLeftOp)
				.apply(dp.get(comp), scalar);
		
		String lineageDescriptor = datasetIsLeftOp ? "x" + operator.toString() + scalar : scalar + operator.toString() + "x"; 
		return dataset.mapKeepingKeys(dsMeta, lineage -> LineageNode.of(lineageDescriptor, lineage), dp -> { 
				Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>();
				for (VTLAlias name: measureNames)
				{
					DataStructureComponent<Measure, ?, ?> comp = dsMeta.getComponent(name)
							.orElseThrow(() -> new VTLMissingComponentsException(name, dp.keySet())).asRole(Measure.class);
					result.put(comp, finisher.apply(dp, comp));
				}
				return result;
			});
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		// index (as right operand) the one with less keys and stream the other (as left operand)
		boolean swap = left.getMetadata().getIDs().containsAll(right.getMetadata().getIDs());
		DataSet streamed = swap ? right : left;
		DataSet indexed = swap ? left : right;

		if (metadata == null)
		{
			DataStructureComponent<Measure, ?, ?> leftMeasure = streamed.getMetadata().getComponents(Measure.class, NUMBERDS).iterator().next();
			DataStructureComponent<Measure, ?, ?> rightMeasure = indexed.getMetadata().getComponents(Measure.class, NUMBERDS).iterator().next();
			DataStructureComponent<Measure, ?, ?> resultComp;
			if (operator != DIV && INTEGERDS.isAssignableFrom(leftMeasure.getVariable().getDomain()) && INTEGERDS.isAssignableFrom(rightMeasure.getVariable().getDomain()))
				resultComp = INTEGERDS.getDefaultVariable().as(Measure.class);
			else
				resultComp = NUMBERDS.getDefaultVariable().as(Measure.class);
			
			DataSetMetadata newStructure = new DataStructureBuilder(streamed.getMetadata().getIDs())
					.addComponent(resultComp)
					.build();
			
			boolean intResult = INTEGERDS.isAssignableFrom(resultComp.getVariable().getDomain());
			return streamed.mappedJoin(newStructure, indexed,  
					(dpl, dpr) -> new DataPointBuilder()
						.add(resultComp, compute(swap, intResult, dpl.get(leftMeasure), dpr.get(rightMeasure)))
						.addAll(dpl.getValues(Identifier.class))
						.addAll(dpr.getValues(Identifier.class))
						.build(LineageNode.of(this, LineageCall.of(dpl.getLineage(), dpr.getLineage())), newStructure), false);
		}
		else
		{
			Set<DataStructureComponent<Measure, ?, ?>> resultMeasures = ((DataSetMetadata) metadata).getMeasures();
			
			if (resultMeasures.size() == 1)
			{
				DataStructureComponent<Measure, ?, ?> resultMeasure = resultMeasures.iterator().next(); 
				DataStructureComponent<Measure, ?, ?> streamedMeasure = streamed.getMetadata().getMeasures().iterator().next(); 
				DataStructureComponent<Measure, ?, ?> indexedMeasure = indexed.getMetadata().getMeasures().iterator().next(); 
				
				// at component level, source measures can have different names but there is only 1 for each operand
				return streamed.mappedJoin((DataSetMetadata) metadata, indexed, (dpl, dpr) -> {
						boolean isResultInt = operator != DIV && INTEGERDS.isAssignableFrom(resultMeasure.getVariable().getDomain());
						ScalarValue<?, ?, ?, ?> leftVal = dpl.get(streamedMeasure);
						ScalarValue<?, ?, ?, ?> rightVal = dpr.get(indexedMeasure);
						return new DataPointBuilder()
								.add(resultMeasure, compute(swap, isResultInt, leftVal, rightVal))
								.addAll(dpl.getValues(Identifier.class))
								.addAll(dpr.getValues(Identifier.class))
								.build(LineageNode.of(ArithmeticTransformation.this, LineageCall.of(dpl.getLineage(), dpr.getLineage())), (DataSetMetadata) metadata);						
					}, false);
			}
			else
				// Scan the dataset with less identifiers and find the matches
				return streamed.mappedJoin((DataSetMetadata) metadata, indexed, 
					(dpl, dpr) -> new DataPointBuilder(resultMeasures.stream()
							.map(toEntryWithValue(compToCalc -> compute(swap, INTEGERDS.isAssignableFrom(compToCalc.getVariable().getDomain()), 
									dpl.get(streamed.getComponent(compToCalc.getVariable().getAlias()).get()), 
									dpr.get(indexed.getComponent(compToCalc.getVariable().getAlias()).get()))
							)).collect(entriesToMap()))		
						.addAll(dpl.getValues(Identifier.class))
						.addAll(dpr.getValues(Identifier.class))
						.build(LineageNode.of(this, LineageCall.of(dpl.getLineage(), dpr.getLineage())), (DataSetMetadata) metadata), false);
		}
	}

	// take account of the order of parameters because some operators are not commutative 
	private ScalarValue<?, ?, ?, ?> compute(boolean swap, boolean intResult, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
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
		
		if (INTEGERDS.isAssignableFrom(domainLeft) && INTEGERDS.isAssignableFrom(domainRight))
			return INTEGER;
		else if (NUMBERDS.isAssignableFrom(domainLeft) && NUMBERDS.isAssignableFrom(domainRight))
			return NUMBER;
		else
			throw new VTLIncompatibleTypesException(operator.toString(), NUMBERDS, 
					!NUMBERDS.isAssignableFrom(domainLeft) ? domainLeft : domainRight);
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (dataset.getMeasures().size() == 0)
			throw new VTLExpectedRoleException(Measure.class, dataset);
		
		dataset.getMeasures().stream()
			.filter(c -> !NUMBERDS.isAssignableFrom(c.getVariable().getDomain()))
			.map(c -> new VTLIncompatibleTypesException(operator.toString().toLowerCase(), c, NUMBERDS))
			.forEach(e -> { throw e; });

		ValueDomainSubset<?, ?> scalarDomain = scalar.getDomain();
		if (!INTEGERDS.isAssignableFrom(scalarDomain))
		{
			if (!NUMBERDS.isAssignableFrom(scalarDomain))
				throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), NUMBERDS, scalarDomain);
			
			for (DataStructureComponent<Measure, ?, ?> measure: dataset.getMeasures())
				if (INTEGERDS.isAssignableFrom(measure.getVariable().getDomain()))
					throw new VTLIncompatibleTypesException(operator.toString().toLowerCase(), NUMBERDS, measure);
		}
		
		return new DataStructureBuilder(dataset).removeComponents(dataset.getComponents(Attribute.class)).build();
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		Set<DataStructureComponent<Measure, ?, ?>> leftMeasures = left.getMeasures();
		Set<DataStructureComponent<Measure, ?, ?>> rightMeasures = right.getMeasures();
		
		if (leftMeasures.size() == 0)
			throw new VTLExpectedRoleException(Measure.class, NUMBERDS, leftMeasures);
		if (rightMeasures.size() == 0)
			throw new VTLExpectedRoleException(Measure.class, NUMBERDS, rightMeasures);

		if (!left.getIDs().containsAll(right.getIDs())
				&& !right.getIDs().containsAll(left.getIDs()))
			throw new UnsupportedOperationException("One dataset must have all the identifiers of the other.");

		// check if measures are the same, unless we are at component level
		Set<DataStructureComponent<Measure, ?, ?>> resultMeasures;
		if (leftMeasures.size() == 1 && rightMeasures.size() == 1 && !leftMeasures.equals(rightMeasures))
		{
			ValueDomainSubset<?, ?> leftDomain = leftMeasures.iterator().next().getVariable().getDomain();
			ValueDomainSubset<?, ?> rightDomain = rightMeasures.iterator().next().getVariable().getDomain();
			
			ValueDomainSubset<?, ?> domain = INTEGERDS.isAssignableFrom(leftDomain) && INTEGERDS.isAssignableFrom(rightDomain) ? INTEGERDS : NUMBERDS;
			
			resultMeasures = singleton(domain.getDefaultVariable().as(Measure.class));
		}
		else
		{
			Map<VTLAlias, DataStructureComponent<Measure, ?, ?>> leftMeasuresMap = leftMeasures.stream().collect(toMapWithKeys(m -> m.getVariable().getAlias()));
			Map<VTLAlias, DataStructureComponent<Measure, ?, ?>> rightMeasuresMap = rightMeasures.stream().collect(toMapWithKeys(m -> m.getVariable().getAlias()));
			
			resultMeasures = Stream.concat(leftMeasuresMap.keySet().stream(), rightMeasuresMap.keySet().stream())
				.map(name -> new SimpleEntry<>(leftMeasuresMap.get(name), rightMeasuresMap.get(name)))
				.peek(splittingConsumer((lm, rm) -> 
					{
						if (lm == null)
							throw new VTLMissingComponentsException(rm, leftMeasures);
						if (rm == null)
							throw new VTLMissingComponentsException(lm, rightMeasures);
						if (!NUMBERDS.isAssignableFrom(lm.getVariable().getDomain()))
							throw new UnsupportedOperationException("Expected numeric measure but found: " + lm);
						if (!NUMBERDS.isAssignableFrom(rm.getVariable().getDomain()))
							throw new UnsupportedOperationException("Expected numeric measure but found: " + rm);
					}))
				// if at least one components is floating point, use floating point otherwise integer
				.map(splitting((lm, rm) -> INTEGERDS.isAssignableFrom(lm.getVariable().getDomain()) 
						? INTEGERDS.isAssignableFrom(rm.getVariable().getDomain())
						? lm : rm : lm))
				.collect(toSet());
		}
		
		return new DataStructureBuilder().addComponents(left.getIDs())
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
