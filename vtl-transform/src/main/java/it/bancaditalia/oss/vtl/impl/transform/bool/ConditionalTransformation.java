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
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleParametersException;
import it.bancaditalia.oss.vtl.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.exceptions.VTLNestedException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.Component.Attribute;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.data.Variable;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class ConditionalTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Identifier, EntireBooleanDomainSubset, BooleanDomain> COND_ID = BOOLEANDS.getDefaultVariable().as(Identifier.class);

	// protected: used by NVL Transformation
	protected final Transformation condition;
	protected final Transformation thenExpr, elseExpr;

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
		{
			VTLValueMetadata thenMeta = thenExpr.getMetadata(session);
			VTLValueMetadata elseMeta = elseExpr.getMetadata(session);

			ValueDomainSubset<?, ?> thenDomain;
			if (thenMeta instanceof Variable)
				thenDomain = ((Variable<?, ?>) thenMeta).getDomain();
			else if (thenMeta instanceof ScalarValueMetadata)
				thenDomain = ((ScalarValueMetadata<?, ?>) thenMeta).getDomain();
			else
				throw new IllegalStateException(thenMeta.getClass().getName());

			ValueDomainSubset<?, ?> elseDomain;
			if (elseMeta instanceof Variable)
				elseDomain = ((Variable<?, ?>) elseMeta).getDomain();
			else if (elseMeta instanceof ScalarValueMetadata)
				elseDomain = ((ScalarValueMetadata<?, ?>) elseMeta).getDomain();
			else
				throw new IllegalStateException(elseMeta.getClass().getName());
			
			ValueDomainSubset<?, ?> castDomain = thenDomain.isAssignableFrom(elseDomain) ? thenDomain : elseDomain;
			
			if (TRUE == BOOLEANDS.cast((ScalarValue<?, ?, ?, ?>) cond))
				return castDomain.cast((ScalarValue<?, ?, ?, ?>) thenExpr.eval(session));
			else
				return castDomain.cast((ScalarValue<?, ?, ?, ?>) elseExpr.eval(session));
		}
		else
		{
			DataSet condD = (DataSet) cond;
			VTLValue thenV = thenExpr.eval(session);
			VTLValue elseV = elseExpr.eval(session);
			DataStructureComponent<Measure, ?, ?> booleanConditionMeasure = condD.getMetadata().getComponents(Measure.class, BOOLEANDS).iterator().next();

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

	private DataPoint evalDatasetAndScalar(DataSetMetadata metadata, boolean cond, DataPoint dp, ScalarValue<?, ?, ?, ?> scalar, DataStructureComponent<Measure, ?, ?> booleanConditionMeasure)
	{
		if (cond)
		{
			// condition is true and 'then' is the scalar or condition is false and 'else' is the scalar
			Map<DataStructureComponent<Measure, ?, ?>, ScalarValue<?, ?, ?, ?>> nonIdValues = new ConcurrentHashMap<>(dp.getValues(Measure.class));
			
			// replace all measures values in the datapoint with the scalar casted to the component domain
			nonIdValues.replaceAll((c, v) -> c.getVariable().getDomain().cast(scalar));
			
			return new DataPointBuilder(dp.getValues(Identifier.class))
					.addAll(dp.getValues(Attribute.class))
					.addAll(nonIdValues)
					.build(LineageNode.of("if", dp.getLineage()), (DataSetMetadata) metadata);
		}
		else
			// condition is false and 'then' is the scalar or condition is true and 'else' is the scalar
			return dp;
	}

	private VTLValue evalTwoDatasets(DataSetMetadata metadata, DataSet condD, DataSet thenD, DataSet elseD, DataStructureComponent<Measure, ?, ?> booleanConditionMeasure)
	{
		DataSetMetadata joinIds = new DataStructureBuilder(condD.getMetadata().getIDs()).addComponent(COND_ID).build();
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
		VTLValueMetadata[] metas = new VTLValueMetadata[3];
		int i = 0;
		for (Transformation expr: List.of(condition, thenExpr, elseExpr))
			try
			{
				metas[i++] = expr.getMetadata(scheme);
			}
			catch (Exception e)
			{
				throw new VTLNestedException("Error evaluating expression " + expr, e);
			}
		
		VTLValueMetadata cond = metas[0]; 
		VTLValueMetadata left = metas[1]; 
		VTLValueMetadata right = metas[2]; 

		if (cond instanceof ScalarValueMetadata && BOOLEANDS.isAssignableFrom(((ScalarValueMetadata<?, ?>) cond).getDomain()))
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return left;
			else
				throw new VTLIncompatibleParametersException("if-then-else", left, right);
		else // if (cond instanceof VTLDataSetMetadata)
		{
			// both 'then' and 'else' are scalar
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return left;
			
			// one is a dataset, first check it
			((DataSetMetadata) cond).getSingleton(Measure.class, BOOLEANDS);
			DataSetMetadata dataset = (DataSetMetadata) (left instanceof DataSetMetadata ? left : right);
			VTLValueMetadata other = left instanceof DataSetMetadata ? right : left;

			if (!dataset.getIDs().equals(((DataSetMetadata) cond).getIDs()))
				throw new UnsupportedOperationException("Condition must have same identifiers as other expressions: " + dataset.getIDs() + " -- " + ((DataSetMetadata) cond).getIDs());

			if (other instanceof DataSetMetadata)
			{
				// if structures are not equal, each dataset must have only one measure and they must be of compatible domains
				if (!dataset.equals(other))
				{
					DataStructureComponent<Measure, ?, ?> leftMeasure = dataset.getSingleton(Measure.class);
					DataStructureComponent<Measure, ?, ?> rightMeasure = ((DataSetMetadata) other).getSingleton(Measure.class);
					if (!leftMeasure.getVariable().getDomain().equals(rightMeasure.getVariable().getDomain()))
						if (!leftMeasure.getVariable().getDomain().isAssignableFrom(rightMeasure.getVariable().getDomain())
								&& rightMeasure.getVariable().getDomain().isAssignableFrom(leftMeasure.getVariable().getDomain()))
							dataset = new DataStructureBuilder(dataset)
								.removeComponent(leftMeasure)
								.addComponent(rightMeasure)
								.build();
						else
							throw new VTLIncompatibleTypesException("if-then-else", leftMeasure, rightMeasure);
				}
			}
			else
			{
				// If other is a scalar check that all measures are compatible with the scalar valuedomain
				Optional<? extends ValueDomainSubset<?, ?>> wrongMeasure = dataset.getMeasures().stream()
						.map(c -> c.getVariable().getDomain())
						.filter(d -> !d.isAssignableFrom(((ScalarValueMetadata<?, ?>) other).getDomain()))
						.findAny();
				if (wrongMeasure.isPresent())
					throw new VTLIncompatibleTypesException("if-then-else", ((ScalarValueMetadata<?, ?>) other).getDomain(), wrongMeasure.get());
			}

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
