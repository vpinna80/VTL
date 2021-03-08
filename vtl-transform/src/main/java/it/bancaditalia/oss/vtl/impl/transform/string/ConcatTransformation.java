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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.reverseIfBOp;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.Set;
import java.util.function.BinaryOperator;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.impl.transform.BinaryTransformation;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.util.Utils;

public class ConcatTransformation extends BinaryTransformation
{
	private static final long serialVersionUID = 1L;
	private final static BinaryOperator<ScalarValue<?, ? extends StringDomainSubset, ? extends StringDomain>> concat = (l, r) -> l instanceof NullValue || r instanceof NullValue 
			? NullValue.instance(STRINGDS)
			: new StringValue(l.get().toString() + r.get().toString());

	public ConcatTransformation(Transformation left, Transformation right)
	{
		super(left, right);
	}

	@Override
	protected VTLValue evalTwoScalars(ScalarValue<?, ?, ?> left, ScalarValue<?, ?, ?> right)
	{
		return concat.apply((StringValue) STRINGDS.cast(left), (StringValue) STRINGDS.cast(right));
	}

	@Override
	protected VTLValue evalDatasetWithScalar(boolean datasetIsLeftOp, DataSet dataset, ScalarValue<?, ?, ?> scalar)
	{
		BinaryOperator<ScalarValue<?, ? extends StringDomainSubset, ? extends StringDomain>> function = Utils.reverseIfBOp(!datasetIsLeftOp, concat);
		DataSetMetadata structure = dataset.getMetadata();
		DataStructureComponent<Measure, StringDomainSubset, StringDomain> measure = structure.getComponents(Measure.class, Domains.STRINGDS).iterator().next();
		
		return dataset.mapKeepingKeys(structure, dp -> Collections.singletonMap(measure, 
				function.apply(STRINGDS.cast(dp.get(measure)), STRINGDS.cast(scalar)))); 
	}

	@Override
	protected VTLValue evalTwoDatasets(DataSet left, DataSet right)
	{
		DataSetMetadata metadata = (DataSetMetadata) getMetadata();
		boolean leftHasMoreIdentifiers = left.getComponents(Identifier.class).containsAll(right.getComponents(Identifier.class));

		DataSet streamed = leftHasMoreIdentifiers ? right: left;
		DataSet indexed = leftHasMoreIdentifiers ? left: right;
		Set<? extends DataStructureComponent<? extends Measure, ?, ?>> resultMeasures = metadata.getComponents(Measure.class);
		
		// must remember which is the left operand because some operators are not commutative
		BinaryOperator<ScalarValue<?, ? extends StringDomainSubset, ? extends StringDomain>> finalOperator = reverseIfBOp(!leftHasMoreIdentifiers, concat);  

		// Scan the dataset with less identifiers and find the matches
		return indexed.filteredMappedJoin(metadata, streamed, (dp1, dp2) -> true /* no filter */,
			(dp1, dp2) -> new DataPointBuilder(resultMeasures.stream()
					.map(rm -> new SimpleEntry<>(rm, finalOperator
							.apply(STRINGDS.cast(dp1.get(indexed.getComponent(rm.getName()).get())), 
									STRINGDS.cast(dp2.get(streamed.getComponent(rm.getName()).get())))))
					.collect(Utils.entriesToMap()))		
				.addAll(dp1.getValues(Identifier.class))
				.addAll(dp2.getValues(Identifier.class))
				.build(metadata));
	}

	@Override
	protected VTLValueMetadata getMetadataTwoScalars(ScalarValueMetadata<?> left, ScalarValueMetadata<?> right)
	{
		if (!(STRINGDS.isAssignableFrom(left.getDomain())))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, left.getDomain());
		else if (!(STRINGDS.isAssignableFrom(right.getDomain())))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, right.getDomain());
		else
			return Domains.STRING;
	}
	
	@Override
	protected VTLValueMetadata getMetadataDatasetWithScalar(boolean datasetIsLeftOp, DataSetMetadata dataset, ScalarValueMetadata<?> scalar)
	{
		if (!STRINGDS.isAssignableFrom(scalar.getDomain()))
			throw new VTLIncompatibleTypesException("concat", STRINGDS, scalar.getDomain());
		
		final Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class);
		if (measures.size() != 1)
			throw new VTLSingletonComponentRequiredException(Measure.class, measures);
		
		DataStructureComponent<? extends Measure, ?, ?> measure = measures.iterator().next();
		if (!STRINGDS.isAssignableFrom(measure.getDomain()))
			throw new VTLExpectedComponentException(Measure.class, STRINGDS, measures);
		
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
		
		if (!leftMeasures.equals(rightMeasures))
			throw new VTLException("The two datasets must have the same measures.");
		
		leftMeasures.stream()
				.forEach(m -> {
					if (!STRINGDS.isAssignableFrom(m.getDomain()))
						throw new VTLIncompatibleTypesException("concat", STRINGDS, m.getDomain());
				});
		
		return new DataStructureBuilder()
				.addComponents(leftIds)
				.addComponents(rightIds)
				.addComponents(leftMeasures)
				.build();
	}
}
