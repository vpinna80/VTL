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

import static it.bancaditalia.oss.vtl.impl.transform.bool.BooleanUnaryTransformation.BooleanUnaryOperator.CHECK;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckTransformation.CheckOutput.ALL;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.bool.BooleanUnaryTransformation.BooleanUnaryOperator;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLInvariantIdentifiersException;
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
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerBiPredicate;

public class CheckTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final BooleanUnaryOperator function = CHECK;
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_VAR = new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS); 
	private static final DataStructureComponent<Measure, EntireNumberDomainSubset, NumberDomain> IMBALANCE = new DataStructureComponentImpl<>("imbalance", Measure.class, NUMBERDS); 
	private static final DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> ERRORCODE = new DataStructureComponentImpl<>("errorcode", Measure.class, STRINGDS); 
	private static final DataStructureComponent<Measure, EntireNumberDomainSubset, NumberDomain> ERRORLEVEL = new DataStructureComponentImpl<>("errorlevel", Measure.class, NUMBERDS); 

	public enum CheckOutput
	{
		INVALID, ALL;
	}

	private final Transformation operand;
	private final Transformation errorcodeExpr;
	private final Transformation errorlevelExpr;
	private final Transformation imbalanceExpr;
	private final CheckOutput output;
	
	public CheckTransformation(Transformation operand, Transformation errorcode, Transformation errorlevel, Transformation imbalance, CheckOutput output)
	{
		this.operand = operand;
		this.output = coalesce(output, ALL);

		if (errorcode != null)
			throw new UnsupportedOperationException("Errorcode not implemented.");
		if (errorlevel != null)
			throw new UnsupportedOperationException("Errorvalue not implemented.");
		if (this.output != ALL)
			throw new UnsupportedOperationException("Invalid not implemented.");
		
		this.errorcodeExpr = errorcode;
		this.errorlevelExpr = errorlevel;
		this.imbalanceExpr = imbalance;
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);
		DataSetMetadata metadata = (DataSetMetadata) getMetadata(scheme);
		
		if (imbalanceExpr == null)
			// TODO: INVALID
			return dataset.mapKeepingKeys(metadata, dp -> LineageNode.of(this, dp.getLineage()), dp -> {
				Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>(); 
				result.put(BOOL_VAR, function.apply(BOOLEANDS.cast(dp.get(BOOL_VAR))));
				result.put(ERRORCODE, NullValue.instance(NUMBERDS));
				result.put(ERRORLEVEL, NullValue.instance(NUMBERDS));
				return result;
			});
		else
		{
			DataSet imbalanceDataset = (DataSet) imbalanceExpr.eval(scheme);
			
			// TODO: INVALID
			SerBiPredicate<DataPoint, DataPoint> filter = (a, b) -> true;
			return dataset.filteredMappedJoin(metadata, imbalanceDataset, filter, (a, b) -> new DataPointBuilder()
				.addAll(a.getValues(Identifier.class))
				.add(BOOL_VAR, function.apply(BOOLEANDS.cast(a.get(BOOL_VAR))))
				.add(IMBALANCE, b.getValues(Measure.class).values().iterator().next())
				.add(ERRORCODE, NullValue.instance(STRINGDS))
				.add(ERRORLEVEL, NullValue.instance(NUMBERDS))
				.build(getLineage(), metadata), false);
		}
	}
	
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme),
				imbalanceValue = imbalanceExpr != null ? imbalanceExpr.getMetadata(scheme) : null;

		if (meta instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(meta, DataSetMetadata.class);
		else if (imbalanceExpr instanceof ScalarValueMetadata)
			throw new VTLInvalidParameterException(imbalanceValue, DataSetMetadata.class);
		else
		{
			DataSetMetadata dataset = (DataSetMetadata) meta;

			Set<? extends DataStructureComponent<? extends Measure, ?, ?>> measures = dataset.getComponents(Measure.class, BOOLEANDS);
			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, BOOLEANDS, measures);

			Set<? extends DataStructureComponent<? extends Identifier, ?, ?>> identifiers = dataset.getComponents(Identifier.class);
			DataStructureBuilder metadata = new DataStructureBuilder().addComponents(identifiers);
			
			if (imbalanceValue != null)
			{
				DataSetMetadata imbalanceDataset = (DataSetMetadata) imbalanceValue;
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

			return metadata
					.addComponent(ERRORCODE)
					.addComponent(ERRORLEVEL)
					.build();
		}
	}
	
	@Override
	public String toString()
	{
		return "check(" + operand 
				+ (errorlevelExpr != null ? " errorlevel " + errorlevelExpr: "")
				+ (errorcodeExpr != null ? " errorcode " + errorcodeExpr: "")
				+ (imbalanceExpr != null ? " imbalance " + imbalanceExpr : "") 
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

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((errorcodeExpr == null) ? 0 : errorcodeExpr.hashCode());
		result = prime * result + ((errorlevelExpr == null) ? 0 : errorlevelExpr.hashCode());
		result = prime * result + ((imbalanceExpr == null) ? 0 : imbalanceExpr.hashCode());
		result = prime * result + ((operand == null) ? 0 : operand.hashCode());
		result = prime * result + ((output == null) ? 0 : output.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj) return true;
		if (!(obj instanceof CheckTransformation)) return false;
		CheckTransformation other = (CheckTransformation) obj;
		if (errorcodeExpr == null)
		{
			if (other.errorcodeExpr != null) return false;
		}
		else if (!errorcodeExpr.equals(other.errorcodeExpr)) return false;
		if (errorlevelExpr == null)
		{
			if (other.errorlevelExpr != null) return false;
		}
		else if (!errorlevelExpr.equals(other.errorlevelExpr)) return false;
		if (imbalanceExpr == null)
		{
			if (other.imbalanceExpr != null) return false;
		}
		else if (!imbalanceExpr.equals(other.imbalanceExpr)) return false;
		if (operand == null)
		{
			if (other.operand != null) return false;
		}
		else if (!operand.equals(other.operand)) return false;
		if (output != other.output) return false;
		return true;
	}
	
	@Override
	public Lineage computeLineage()
	{
		throw new UnsupportedOperationException();
	}
}