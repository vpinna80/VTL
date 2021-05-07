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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRING;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.entriesToMap;
import static it.bancaditalia.oss.vtl.util.Utils.reverseIf;
import static java.util.Collections.singletonMap;

import java.util.AbstractMap.SimpleEntry;
import java.util.Optional;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

public class ConcatTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;
	private static final BinaryOperator<ScalarValue<?, ?, ? extends StringDomainSubset<?>, ? extends StringDomain>> CONCAT = (l, r) -> l instanceof NullValue || r instanceof NullValue 
			? NullValue.instance(STRINGDS)
			: StringValue.of(l.get().toString() + r.get().toString());

	public ConcatTransformation(Transformation left, Transformation right)
	{
		super(left, right);
	}

	@Override
	protected ScalarValue<?, ?, ? extends StringDomainSubset<?>, ? extends StringDomain> evalTwoScalars(VTLValueMetadata metadata, ScalarValue<?, ?, ?, ?> left, ScalarValue<?, ?, ?, ?> right)
	{
		return CONCAT.apply(STRINGDS.cast(left), STRINGDS.cast(right));
	}

	@Override
	protected VTLValue evalDatasetWithScalar(VTLValueMetadata metadata, boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?, ?> scalar)
	{
		BinaryOperator<ScalarValue<?, ?, ? extends StringDomainSubset<?>, ? extends StringDomain>> function = Utils.reverseIf(CONCAT, !datasetIsLeftOp);
		DataSetMetadata structure = dataset.getMetadata();
		DataStructureComponent<Measure, ? extends StringDomainSubset<?>, StringDomain> measure = structure.getComponents(Measure.class, STRINGDS).iterator().next();
		Lineage scalarLineage = (datasetIsLeftOp ? getRightOperand() : getLeftOperand()).getLineage();
		Function<DataPoint, Lineage> lineageFunc = dp -> datasetIsLeftOp
				? LineageNode.of(this, dp.getLineage(), scalarLineage)
				: LineageNode.of(this, scalarLineage, dp.getLineage());
		
		return dataset.mapKeepingKeys(structure, lineageFunc, dp -> singletonMap(measure, 
						function.apply(STRINGDS.cast(dp.get(measure)), STRINGDS.cast(scalar)))); 
	}

	@Override
	protected VTLValue evalTwoDatasets(VTLValueMetadata metadata, DataSet left, DataSet right)
	{
		boolean leftHasMoreIdentifiers = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));

		DataSet streamed = leftHasMoreIdentifiers ? right: left;
		DataSet indexed = leftHasMoreIdentifiers ? left: right;
		Set<DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain>> resultMeasures = ((DataSetMetadata) metadata).getComponents(Measure.class, STRINGDS);
		
		if (resultMeasures.size() == 1 && (!left.getMetadata().containsAll(resultMeasures) || !right.getMetadata().containsAll(resultMeasures)))
		{
			DataStructureComponent<Measure, ? extends StringDomainSubset<?>, StringDomain> resultMeasure = resultMeasures.iterator().next();
			DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> streamedMeasure = streamed.getComponents(Measure.class, STRINGDS).iterator().next();
			DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> indexedMeasure = indexed.getComponents(Measure.class, STRINGDS).iterator().next();
			
			BinaryOperator<ScalarValue<?, ?, ? extends StringDomainSubset<?>, ? extends StringDomain>> finalOperator = reverseIf(CONCAT, !leftHasMoreIdentifiers);
			
			return streamed.mappedJoin((DataSetMetadata) metadata, indexed, (dps, dpi) -> new DataPointBuilder()
				.add(resultMeasure, finalOperator.apply(dps.getValue(streamedMeasure), dpi.getValue(indexedMeasure)))
				.addAll(dpi.getValues(Identifier.class))
				.addAll(dps.getValues(Identifier.class))
				.build(getLineage(), (DataSetMetadata) metadata));
		}
		else
		{
			// must remember which is the left operand because some operators are not commutative
			BinaryOperator<ScalarValue<?, ?, ? extends StringDomainSubset<?>, ? extends StringDomain>> finalOperator = reverseIf(CONCAT, !leftHasMoreIdentifiers);  
	
			// Scan the dataset with less identifiers and find the matches
			return streamed.mappedJoin((DataSetMetadata) metadata, indexed, (dps, dpi) -> new DataPointBuilder(resultMeasures.stream()
						.map(rm -> new SimpleEntry<>(rm, finalOperator
								.apply(STRINGDS.cast(dpi.get(indexed.getComponent(rm.getName()).get())), 
										STRINGDS.cast(dps.get(streamed.getComponent(rm.getName()).get())))))
						.collect(entriesToMap()))		
					.addAll(dpi.getValues(Identifier.class))
					.addAll(dps.getValues(Identifier.class))
					.build(getLineage(), (DataSetMetadata) metadata));
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
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?, ?> scalar)
	{
		if (!STRINGDS.isAssignableFrom(scalar.getDomain()))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, scalar.getDomain());
		
		final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
		Optional<? extends ValueDomainSubset<?, ?>> errorDomain = measures.stream() 
			.map(DataStructureComponent::getDomain)
			.filter(d -> !STRINGDS.isAssignableFrom(d))
			.findAny();

		if (errorDomain.isPresent())
			throw new VTLIncompatibleTypesException("concat", STRINGDS, errorDomain.get());
		
		return dataset;
	}
	
	@Override
	protected VTLValueMetadata getMetadataTwoDatasets(DataSetMetadata left, DataSetMetadata right)
	{
		Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> leftIds = left.getComponents(Identifier.class);
		Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> rightIds = right.getComponents(Identifier.class);

		if (!leftIds.containsAll(rightIds) && !rightIds.containsAll(leftIds))
			throw new VTLException("One dataset must have at least all the identifiers of the other.");
		
		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> leftMeasures = left.getComponents(Measure.class);
		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> rightMeasures = right.getComponents(Measure.class);
		
		Stream.concat(leftMeasures.stream(), rightMeasures.stream()) 
			.map(DataStructureComponent::getDomain)
			.filter(d -> !STRINGDS.isAssignableFrom(d))
			.forEach(d -> { throw new VTLIncompatibleTypesException("concat", STRINGDS, d); });

		if (leftMeasures.size() == 1 && rightMeasures.size() == 1 && !leftMeasures.equals(rightMeasures))
			return new DataStructureBuilder()
				.addComponents(leftIds)
				.addComponents(rightIds)
				.addComponent(new DataStructureComponentImpl<>(Measure.class, STRINGDS))
				.build();
		else if (!leftMeasures.equals(rightMeasures))
			throw new VTLException("The two datasets must have the same measures.");
		else
			return new DataStructureBuilder()
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

	@Override
	public int hashCode()
	{
		return super.hashCode();
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!super.equals(obj)) return false;
		if (!(obj instanceof ConcatTransformation)) return false;
		return true;
	}
}
