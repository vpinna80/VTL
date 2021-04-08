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
package it.bancaditalia.oss.vtl.impl.transform.bool;

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightF2DataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Attribute;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ConditionalTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;

	public ConditionalTransformation(Transformation condition, Transformation trueExpr, Transformation falseExpr)
	{
		this.condition = condition;
		this.thenExpr = trueExpr;
		this.elseExpr = falseExpr;
	}

	protected final Transformation	condition;
	protected final Transformation	thenExpr, elseExpr;

	private VTLValueMetadata		metadata;

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValue cond = condition.eval(session);
		
		if (metadata == null)
			metadata = getMetadata(session);
		
		if (cond instanceof ScalarValue)
			return TRUE == BOOLEANDS.cast((ScalarValue<?, ?, ?, ?>) cond) 
					? thenExpr.eval(session)
					: elseExpr.eval(session);
		else
		{
			DataSet condD = (DataSet) cond;
			VTLValue thenV = thenExpr.eval(session);
			VTLValue elseV = elseExpr.eval(session);
			DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> booleanConditionMeasure = condD.getComponents(Measure.class, BOOLEANDS).iterator().next();

			if (thenV instanceof DataSet && elseV instanceof DataSet) // Two datasets
				return evalTwoDatasets(condD, (DataSet) thenV, (DataSet) elseV, booleanConditionMeasure);
			else // One dataset and one scalar
			{
				DataSet dataset = thenV instanceof DataSet ? (DataSet) thenV : (DataSet) elseV;
				ScalarValue<?, ?, ?, ?> scalar = thenV instanceof ScalarValue ? (ScalarValue<?, ?, ?, ?>) thenV : (ScalarValue<?, ?, ?, ?>) elseV;
				return condD.mappedJoin((DataSetMetadata) metadata, dataset, (dpCond, dp) -> 
						evalDatasetAndScalar(checkCondition(dpCond.get(booleanConditionMeasure)) ^ thenV == dataset, dp, scalar, booleanConditionMeasure));
			}
		}
	}

	private DataPoint evalDatasetAndScalar(boolean cond, DataPoint dp, ScalarValue<?, ?, ?, ?> scalar, 
			DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> booleanConditionMeasure)
	{
		if (cond)
		{
			// condition is true and 'then' is the scalar or condition is false and 'else' is the scalar
			Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> nonIdValues = new ConcurrentHashMap<>(dp.getValues(Measure.class));
			// replace all measures values in the datapoint with the scalar 
			nonIdValues.replaceAll((c, v) -> scalar);
			
			return new DataPointBuilder(dp.getValues(Identifier.class))
					.addAll(dp.getValues(Attribute.class))
					.addAll(nonIdValues)
					.build((DataSetMetadata) metadata);
		}
		else
			return dp;
	}

	private VTLValue evalTwoDatasets(DataSet condD, DataSet thenD, DataSet elseD, DataStructureComponent<Measure, ? extends BooleanDomainSubset<?>, BooleanDomain> booleanConditionMeasure)
	{
		Map<Boolean, Set<Map<DataStructureComponent<Identifier, ?, ?>, ScalarValue<?, ?, ?, ?>>>> partitions;
		Set<DataStructureComponent<Identifier, ?, ?>> valueIDs = thenD.getComponents(Identifier.class);
		
		try (Stream<DataPoint> stream = condD.stream())
		{
			partitions = stream.collect(partitioningBy(dpCond -> checkCondition(dpCond.get(booleanConditionMeasure)),
					mapping(dp -> dp.getValues(valueIDs, Identifier.class), toSet())));
		}
		
		DataSet thenFiltered = thenD.filter(dp -> partitions.get(true).contains(dp.getValues(Identifier.class)));
		DataSet elseFiltered = elseD.filter(dp -> partitions.get(false).contains(dp.getValues(Identifier.class)));
		
		return new LightF2DataSet<>((DataSetMetadata) metadata, 
				(dsThen, dsElse) -> {
					final Stream<DataPoint> thenStream = dsThen.stream();
					final Stream<DataPoint> elseStream = dsElse.stream();
					return concat(thenStream, elseStream)
							.onClose(() -> {
								thenStream.close();
								elseStream.close();
							});
				}, thenFiltered, elseFiltered);
	}

	private boolean checkCondition(ScalarValue<?, ?, ?, ?> value)
	{
		return value instanceof BooleanValue && ((BooleanValue<?>) value).get();
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return concat(condition.getTerminals().stream(), 
				concat(thenExpr.getTerminals().stream(), elseExpr.getTerminals().stream()))
					.collect(toSet());
	}

	@Override
	public VTLValueMetadata getMetadata(TransformationScheme session)
	{
		VTLValueMetadata cond = condition.getMetadata(session);
		VTLValueMetadata left = thenExpr.getMetadata(session);
		VTLValueMetadata right = elseExpr.getMetadata(session);

		if (cond instanceof UnknownValueMetadata || left instanceof UnknownValueMetadata || right instanceof UnknownValueMetadata)
			return INSTANCE;
		else if (cond instanceof ScalarValueMetadata && BOOLEANDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) cond).getDomain()))
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return metadata = left;
			else
				throw new UnsupportedOperationException("Incompatible types in conditional expression: " + left + ", " + right);
		else // if (cond instanceof VTLDataSetMetadata)
		{
			// both 'then' and 'else' are scalar
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return metadata = left;
			
			// one is a dataset, first check it
			Set<? extends DataStructureComponent<?, ?, ?>> condMeasures = ((DataSetMetadata) cond).getComponents(Measure.class, BOOLEANDS);
			DataSetMetadata dataset = (DataSetMetadata) (left instanceof DataSetMetadata ? left : right);
			VTLValueMetadata other = left instanceof DataSetMetadata ? right : left;

			if (condMeasures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, BOOLEANDS, condMeasures);

			if (!dataset.getComponents(Identifier.class).equals(((DataSetMetadata) cond).getComponents(Identifier.class)))
				throw new UnsupportedOperationException("Condition must have same identifiers as other expressions: " + dataset.getComponents(Identifier.class) + " -- " + ((DataSetMetadata) cond).getComponents(Identifier.class));

			if (other instanceof DataSetMetadata)
			{
				// the other is a dataset too, check structures are equal
				if (!dataset.equals(other))
				{
					// check that they have same structure
					Set<DataStructureComponent<?, ?, ?>> missing = new HashSet<>(dataset);
				    missing.addAll((DataSetMetadata) other);
				    Set<DataStructureComponent<?, ?, ?>> tmp = new HashSet<>(dataset);
				    tmp.retainAll((DataSetMetadata) other);
				    missing.removeAll(tmp);
				    
				    if (!dataset.containsAll(missing))
				    {
				    	missing.removeAll(dataset);
				    	throw new VTLSyntaxException("Then and Else expressions must have the same structure.", new VTLMissingComponentsException(missing, dataset)); 
				    }
				    else
				    {
				    	missing.removeAll((DataSetMetadata) other);
				    	throw new VTLSyntaxException("Then and Else expressions must have the same structure.", new VTLMissingComponentsException(missing, (DataSetMetadata) other));
				    }
				}
			}
			else 
				// the other is a scalar, all measures in dataset must be assignable from the scalar
				if (!dataset.getComponents(Measure.class).stream().allMatch(c -> c.getDomain().isAssignableFrom(((ScalarValueMetadata<?, ?>) other).getDomain())))
					throw new UnsupportedOperationException("All measures must be assignable from " + ((ScalarValueMetadata<?, ?>) other).getDomain() + ": " + dataset.getComponents(Measure.class));

			return metadata = dataset;
		}
	}
	
	@Override
	public String toString()
	{
		return "if " + condition + " then " + thenExpr + " else " + elseExpr;
	}
}
