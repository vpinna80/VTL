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
package it.bancaditalia.oss.vtl.impl.transform.string;

import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.STRING_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.SerCollectors.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.util.Collections.singletonMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageCall;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLAlias;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.SerBinaryOperator;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class ConcatTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final SerBinaryOperator<ScalarValue<?, ?, ?, ?>> CONCAT = ConcatTransformation::concat;
	
	private static ScalarValue<?, ?, ?, ?> concat(ScalarValue<?, ?, ?, ?> l, ScalarValue<?, ?, ?, ?> r)
	{
		return StringValue.of(new StringBuilder().append(coalesce(l.get(), "")).append(coalesce(r.get(), "")).toString());
	}

	public ConcatTransformation(Transformation left, Transformation right)
	{
		super(left, right);
	}

	@Override
	protected ScalarValue<?, ?, ?, ?> evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		return concat(STRINGDS.cast(left), STRINGDS.cast(right));
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		SerBinaryOperator<ScalarValue<?, ?, ?, ?>> function = CONCAT.reverseIf(!datasetIsLeftOp);
		DataSetStructure structure = dataset.getMetadata();
		DataSetComponent<Measure, ?, ?> measure = structure.getComponents(Measure.class, STRINGDS).iterator().next();

		SerUnaryOperator<Lineage> lineageFunc = lineage -> LineageNode.of(this, LineageCall.of(lineage));
		return dataset.mapKeepingKeys(structure, lineageFunc, dp -> singletonMap(measure, 
						function.apply(STRINGDS.cast(dp.get(measure)), STRINGDS.cast(scalar)))); 
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getMetadata().getIDs().containsAll(right.getMetadata().getIDs());

		DataSet streamed = leftHasMoreIdentifiers ? right: left;
		DataSet indexed = leftHasMoreIdentifiers ? left: right;
		Set<DataSetComponent<Measure, EntireStringDomainSubset, StringDomain>> resultMeasures = ((DataSetStructure) metadata).getComponents(Measure.class, STRINGDS);
		
		SerBinaryOperator<Lineage> enricher = LineageNode.lineage2Enricher(this);
		if (resultMeasures.size() == 1 && (!left.getMetadata().containsAll(resultMeasures) || !right.getMetadata().containsAll(resultMeasures)))
		{
			DataSetComponent<Measure, ?, ?> resultMeasure = resultMeasures.iterator().next();
			DataSetComponent<Measure, ?, ?> streamedMeasure = streamed.getMetadata().getComponents(Measure.class, STRINGDS).iterator().next();
			DataSetComponent<Measure, ?, ?> indexedMeasure = indexed.getMetadata().getComponents(Measure.class, STRINGDS).iterator().next();
			
			BinaryOperator<ScalarValue<?, ?, ?, ?>> finalOperator = CONCAT.reverseIf(!leftHasMoreIdentifiers);
			return streamed.mappedJoin((DataSetStructure) metadata, indexed, (dps, dpi) -> new DataPointBuilder()
				.add(resultMeasure, finalOperator.apply(dps.get(streamedMeasure), dpi.get(indexedMeasure)))
				.addAll(dpi.getValues(Identifier.class))
				.addAll(dps.getValues(Identifier.class))
				.build(enricher.apply(dps.getLineage(), dpi.getLineage()), (DataSetStructure) metadata));
		}
		else
		{
			// must remember which is the left operand because some operators are not commutative
			BinaryOperator<ScalarValue<?, ?, ?, ?>> finalOperator = CONCAT.reverseIf(!leftHasMoreIdentifiers);  
	
			// Keep the map of components from the result and the two operands
			Map<DataSetComponent<?, ?, ?>, Entry<DataSetComponent<?, ?, ?>, DataSetComponent<?, ?, ?>>> measuresMap = new HashMap<>();
			for (DataSetComponent<?, ?, ?> resultMeasure: resultMeasures)
			{
				VTLAlias rmAlias = resultMeasure.getAlias();
				measuresMap.put(resultMeasure, new SimpleEntry<>(
						streamed.getMetadata().getComponent(rmAlias).get(),
						indexed.getMetadata().getComponent(rmAlias).get()));
			}
			
			// Scan the dataset with less identifiers and find the matches
			return streamed.mappedJoin((DataSetStructure) metadata, indexed, (dps, dpi) -> new DataPointBuilder(resultMeasures.stream()
						.map(rm -> new SimpleEntry<>(rm, finalOperator.apply(
								STRINGDS.cast(dpi.get(measuresMap.get(rm).getKey())), 
								STRINGDS.cast(dps.get(measuresMap.get(rm).getValue())))))
						.collect(entriesToMap()))		
					.addAll(dpi.getValues(Identifier.class))
					.addAll(dps.getValues(Identifier.class))
					.build(enricher.apply(dps.getLineage(), dpi.getLineage()), (DataSetStructure) metadata));
		}
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?, ?> left, ScalarValueMetadata<?, ?> right)
	{
		if (!(STRINGDS.isAssignableFrom(left.getDomain())))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, left.getDomain());
		else if (!(STRINGDS.isAssignableFrom(right.getDomain())))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, right.getDomain());
		else
			return STRING;
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetStructure dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (!STRINGDS.isAssignableFrom(scalar.getDomain()))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, scalar.getDomain());
		
		final Set<? extends DataSetComponent<? extends Measure, ?, ?>> measures = dataset.getMeasures();
		Optional<? extends ValueDomainSubset<?, ?>> errorDomain = measures.stream() 
			.map(DataSetComponent::getDomain)
			.filter(d -> !STRINGDS.isAssignableFrom(d))
			.findAny();

		if (errorDomain.isPresent())
			throw new VTLIncompatibleTypesException("concat", STRINGDS, errorDomain.get());
		
		return dataset;
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetStructure left, DataSetStructure right)
	{
		Set<? extends DataSetComponent<? extends Identifier, ?, ?>> leftIds = left.getIDs();
		Set<? extends DataSetComponent<? extends Identifier, ?, ?>> rightIds = right.getIDs();

		if (!leftIds.containsAll(rightIds) && !rightIds.containsAll(leftIds))
			throw new VTLException("One dataset must have at least all the identifiers of the other.");
		
		Set<? extends DataSetComponent<? extends Measure, ?, ?>> leftMeasures = left.getMeasures();
		Set<? extends DataSetComponent<? extends Measure, ?, ?>> rightMeasures = right.getMeasures();
		
		Stream.concat(leftMeasures.stream(), rightMeasures.stream()) 
			.map(DataSetComponent::getDomain)
			.filter(d -> !STRINGDS.isAssignableFrom(d))
			.forEach(d -> { throw new VTLIncompatibleTypesException("concat", STRINGDS, d); });

		if (leftMeasures.size() == 1 && rightMeasures.size() == 1 && !leftMeasures.equals(rightMeasures))
			return new DataSetStructureBuilder()
				.addComponents(leftIds)
				.addComponents(rightIds)
				.addComponent(STRING_VAR)
				.build();
		else if (!leftMeasures.equals(rightMeasures))
			throw new VTLException("The two datasets must have the same measures.");
		else
			return new DataSetStructureBuilder()
				.addComponents(leftIds)
				.addComponents(rightIds)
				.addComponents(leftMeasures)
				.build();
	}
	
	@Override
	public String toString()
	{
		return getLeftOperand() + " || " + getRightOperand();
	}
}
