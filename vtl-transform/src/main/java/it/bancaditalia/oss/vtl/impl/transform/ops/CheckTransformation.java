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

import static it.bancaditalia.oss.vtl.impl.transform.ops.BooleanUnaryTransformation.BooleanUnaryOperator.CHECK;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckTransformation.CheckOutput.ALL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.transform.ops.BooleanUnaryTransformation.BooleanUnaryOperator;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointImpl.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureImpl.Builder;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSet.VTLDataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CheckTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final BooleanUnaryOperator function = CHECK;
	private static final DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> BOOL_VAR = new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS); 
	private static final DataStructureComponent<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> IMBALANCE = new DataStructureComponentImpl<>("imbalance", Measure.class, NUMBERDS); 
	private static final DataStructureComponent<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> ERRORCODE = new DataStructureComponentImpl<>("errorcode", Measure.class, NUMBERDS); 
	private static final DataStructureComponent<Measure, NumberDomainSubset<NumberDomain>, NumberDomain> ERRORLEVEL = new DataStructureComponentImpl<>("errorlevel", Measure.class, NUMBERDS); 

	public enum CheckOutput
	{
		INVALID, ALL;
	}

	private final Transformation operand;
	private final Transformation errorcode;
	private final Transformation errorlevel;
	private final Transformation imbalance;
	private final CheckOutput output;
	private VTLDataSetMetadata metadata;
	
	public CheckTransformation(Transformation operand, Transformation errorcode, Transformation errorlevel, Transformation imbalance, CheckOutput output)
	{
		this.operand = operand;

		if (errorcode != null)
			throw new UnsupportedOperationException("Errorcode not implemented.");
		if (errorlevel != null)
			throw new UnsupportedOperationException("Errorvalue not implemented.");
		if (output != ALL)
			throw new UnsupportedOperationException("Invalid not implemented.");
		
		this.errorcode = errorcode;
		this.errorlevel = errorlevel;
		this.imbalance = imbalance;
		this.output = output;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		DataSet dataset = (DataSet) operand.eval(session);

		if (imbalance == null)
			// TODO: INVALID
			return dataset.mapKeepingKeys(metadata, dp -> {
				Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?>> result = new HashMap<>(); 
				result.put(BOOL_VAR, function.apply(BOOLEANDS.cast(dp.get(BOOL_VAR))));
				result.put(ERRORCODE, NullValue.instance(NUMBERDS));
				result.put(ERRORLEVEL, NullValue.instance(NUMBERDS));
				return result;
			});
		else
		{
			DataSet imbalanceDataset = (DataSet) imbalance.eval(session);
			
			// TODO: INVALID
			BiPredicate<DataPoint, DataPoint> filter = (a, b) -> true;
			return dataset.filteredMappedJoin(metadata, imbalanceDataset, filter, (a, b) -> new DataPointBuilder()
				.addAll(a.getValues(Identifier.class))
				.add(BOOL_VAR, function.apply(BOOLEANDS.cast(a.get(BOOL_VAR))))
				.add(IMBALANCE, b.getValues(Measure.class).values().iterator().next())
				.add(ERRORCODE, NullValue.instance(NUMBERDS))
				.add(ERRORLEVEL, NullValue.instance(NUMBERDS))
				.build(metadata));
		}
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata meta = operand.getMetadata(session),
				imbalanceValue = imbalance != null ? imbalance.getMetadata(session) : null;

		if (meta instanceof VTLScalarValueMetadata)
			throw new VTLInvalidParameterException(meta, VTLDataSetMetadata.class);
		else if (imbalance instanceof VTLScalarValueMetadata)
			throw new VTLInvalidParameterException(imbalanceValue, VTLDataSetMetadata.class);
		else
		{
			VTLDataSetMetadata dataset = (VTLDataSetMetadata) meta;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class, BOOLEANDS);
			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, BOOLEANDS, measures);

			Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> identifiers = dataset.getComponents(Identifier.class);
			Builder metadata = new Builder().addComponents(identifiers);
			
			if (imbalanceValue != null)
			{
				VTLDataSetMetadata imbalanceDataset = (VTLDataSetMetadata) imbalanceValue;
				Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> imbalanceIdentifiers = imbalanceDataset.getComponents(Identifier.class);

				if (!identifiers.equals(imbalanceIdentifiers))
					throw new VTLInvariantIdentifiersException("check imbalance", identifiers, imbalanceIdentifiers);
				
				Set<? extends DataStructureComponent<? extends Measure, ?, ?>> imbalanceMeasures = imbalanceDataset.getComponents(Measure.class, NUMBERDS);
				if (imbalanceMeasures.size() != 1)
					throw new VTLExpectedComponentException(Measure.class, NUMBERDS, measures);
				
				metadata.addComponent(IMBALANCE);
			}

			if (output == ALL)
				metadata.addComponent(BOOL_VAR);

			return this.metadata = metadata
					.addComponent(ERRORCODE)
					.addComponent(ERRORLEVEL)
					.build();
		}
	}
	
	@Override
	public String toString()
	{
		return "CHECK(" + operand + (imbalance != null ? " IMBALANCE " + imbalance : "") 
				+ (errorlevel != null ? " ERRORLEVEL " + errorlevel: "")
				+ (errorcode != null ? " ERRORCODE " + errorcode: "")
				+ " " + output + ")";
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		// TODO: add imbalance, errorcode and errorlevel terminals
		return operand.getTerminals();
	}
}
