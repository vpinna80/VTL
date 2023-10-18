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

import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.FALSE;
import static it.bancaditalia.oss.vtl.impl.types.data.BooleanValue.TRUE;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
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

	protected final Transformation condition;
	protected final Transformation thenExpr, elseExpr;

	private static final DataStructureComponent<Identifier, EntireBooleanDomainSubset, BooleanDomain> COND_ID = DataStructureComponentImpl.of("$cond$", Identifier.class, BOOLEANDS);

	public ConditionalTransformation(Transformation condition, Transformation trueExpr, Transformation falseExpr)
	{
		this.condition = condition;
		this.thenExpr = trueExpr;
		this.elseExpr = falseExpr;
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		VTLValueMetadata metadata = getMetadata(session);
		VTLValue cond = condition.eval(session);
		
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
				return evalTwoDatasets((DataSetMetadata) metadata, condD, (DataSet) thenV, (DataSet) elseV, booleanConditionMeasure);
			else // One dataset and one scalar
			{
				DataSet dataset = thenV instanceof DataSet ? (DataSet) thenV : (DataSet) elseV;
				ScalarValue<?, ?, ?, ?> scalar = thenV instanceof ScalarValue ? (ScalarValue<?, ?, ?, ?>) thenV : (ScalarValue<?, ?, ?, ?>) elseV;
				return condD.mappedJoin((DataSetMetadata) metadata, dataset, (dpCond, dp) -> 
						evalDatasetAndScalar((DataSetMetadata) metadata, thenV == dataset ^ checkCondition(dpCond.get(booleanConditionMeasure)), dp, scalar, booleanConditionMeasure), false);
			}
		}
	}

	private DataPoint evalDatasetAndScalar(DataSetMetadata metadata, boolean cond, DataPoint dp, ScalarValue<?, ?, ?, ?> scalar, 
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
					.build(LineageNode.of("if", dp.getLineage()), (DataSetMetadata) metadata);
		}
		else
			// condition is false and 'then' is the scalar or condition is true and 'else' is the scalar
			return dp;
	}

	private VTLValue evalTwoDatasets(DataSetMetadata metadata, DataSet condD, DataSet thenD, DataSet elseD, DataStructureComponent<Measure, ? extends BooleanDomainSubset<?>, BooleanDomain> booleanConditionMeasure)
	{
		DataSetMetadata joinIds = new DataStructureBuilder(condD.getMetadata().getComponents(Identifier.class)).addComponent(COND_ID).build();
		DataSetMetadata enriched = new DataStructureBuilder(thenD.getMetadata()).addComponent(COND_ID).build();
		
		DataSet condResolved = condD.mapKeepingKeys(joinIds, dp -> LineageNode.of(thenExpr, dp.getLineage()), dp -> singletonMap(COND_ID, BooleanValue.of(checkCondition(dp.get(booleanConditionMeasure)))));
		DataSet thenResolved = thenD.mapKeepingKeys(enriched, DataPoint::getLineage, dp -> {
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>(dp);
			result.put(COND_ID, TRUE);
			return result;	
		});
		DataSet elseResolved = elseD.mapKeepingKeys(enriched, DataPoint::getLineage, dp -> {
			Map<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> result = new HashMap<>(dp);
			result.put(COND_ID, FALSE);
			return result;	
		});
		
		return condResolved.mappedJoin(enriched, thenResolved, (a, b) -> b).subspace(singletonMap(COND_ID, TRUE), DataPoint::getLineage)
			.union(DataPoint::getLineage, singletonList(condResolved.mappedJoin(enriched, elseResolved, (a, b) -> b).subspace(singletonMap(COND_ID, FALSE), DataPoint::getLineage)));
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
	
	public VTLValueMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata cond = condition.getMetadata(scheme);
		VTLValueMetadata left = thenExpr.getMetadata(scheme);
		VTLValueMetadata right = elseExpr.getMetadata(scheme);

		if (cond instanceof UnknownValueMetadata || left instanceof UnknownValueMetadata || right instanceof UnknownValueMetadata)
			return INSTANCE;
		else if (cond instanceof ScalarValueMetadata && BOOLEANDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) cond).getDomain()))
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return left;
			else
				throw new UnsupportedOperationException("Incompatible types in conditional expression: " + left + ", " + right);
		else // if (cond instanceof VTLDataSetMetadata)
		{
			// both 'then' and 'else' are scalar
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return left;
			
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

			return dataset;
		}
	}
	
	@Override
	public String toString()
	{
		return "if " + condition + " then " + thenExpr + " else " + elseExpr;
	}

	public Transformation getCondition()
	{
		return condition;
	}

	public Transformation getThenExpr()
	{
		return thenExpr;
	}

	public Transformation getElseExpr()
	{
		return elseExpr;
	}
}
