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
import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataSetComponentImpl.BOOL_VAR;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORCODE;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.ERRORLEVEL;
import static it.bancaditalia.oss.vtl.impl.types.domain.CommonComponents.IMBALANCE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode.lineageEnricher;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.exceptions.VTLInvariantIdentifiersException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataSetStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetComponent;
import it.bancaditalia.oss.vtl.model.data.DataSetStructure;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.SerUnaryOperator;

public class CheckTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

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
		this.errorcodeExpr = errorcode;
		this.errorlevelExpr = errorlevel;
		this.imbalanceExpr = imbalance;
	}

	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);
		DataSetStructure metadata = (DataSetStructure) getMetadata(scheme);
		
		if (imbalanceExpr == null)
			dataset = dataset.mapKeepingKeys(metadata, lineageEnricher(this), dp -> {
				Map<DataSetComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>(); 
				result.put(BOOL_VAR, CHECK.apply(BOOLEANDS.cast(dp.get(BOOL_VAR))));
				result.put(ERRORCODE.get(), NullValue.instance(STRINGDS));
				result.put(ERRORLEVEL.get(), NullValue.instance(NUMBERDS));
				return result;
			});
		else
		{
			DataSet imbalanceDataset = (DataSet) imbalanceExpr.eval(scheme);
			DataSetComponent<Measure, ?, ?> imbalanceMeasure = imbalanceDataset.getMetadata().getSingleton(Measure.class);
			
			SerUnaryOperator<Lineage> enricher = lineageEnricher(this);
			dataset = dataset.mappedJoin(metadata, imbalanceDataset, (dp1, dp2) -> {
				return new DataPointBuilder()
					.addAll(dp1.getValues(Identifier.class))
					.add(BOOL_VAR, CHECK.apply(BOOLEANDS.cast(dp1.get(BOOL_VAR))))
					.add(IMBALANCE, NUMBERDS.cast(dp2.get(imbalanceMeasure)))
					.add(ERRORCODE, NullValue.instance(STRINGDS))
					.add(ERRORLEVEL, NullValue.instance(INTEGERDS))
					.build(enricher.apply(dp1.getLineage()), metadata);
			});
		}
		
		return output == ALL ? dataset : dataset.filter(dp -> dp.getValue(BOOL_VAR) != TRUE, lineageEnricher(this));
	}
	
	protected VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata meta = operand.getMetadata(scheme),
				imbalanceValue = imbalanceExpr != null ? imbalanceExpr.getMetadata(scheme) : null;

		if (!meta.isDataSet())
			throw new VTLInvalidParameterException(meta, DataSetStructure.class);
		else if (!imbalanceValue.isDataSet())
			throw new VTLInvalidParameterException(imbalanceValue, DataSetStructure.class);
		else
		{
			DataSetStructure dataset = (DataSetStructure) meta;
			dataset.getSingleton(Measure.class, BOOLEANDS);

			Set<? extends DataSetComponent<? extends Identifier, ?, ?>> identifiers = dataset.getIDs();
			DataSetStructureBuilder metadata = new DataSetStructureBuilder().addComponents(identifiers);
			
			if (imbalanceValue != null)
			{
				DataSetStructure imbalanceDataset = (DataSetStructure) imbalanceValue;
				Set<? extends DataSetComponent<? extends Identifier, ?, ?>> imbalanceIdentifiers = imbalanceDataset.getIDs();

				if (!identifiers.equals(imbalanceIdentifiers))
					throw new VTLInvariantIdentifiersException("check imbalance", identifiers, imbalanceIdentifiers);
				
				imbalanceDataset.getSingleton(Measure.class, NUMBERDS);
				metadata.addComponents(IMBALANCE);
			}

			return metadata
					.addComponents(BOOL_VAR)
					.addComponents(ERRORCODE, ERRORLEVEL)
					.build();
		}
	}
	
	@Override
	public boolean hasAnalytic()
	{
		return false;
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
}