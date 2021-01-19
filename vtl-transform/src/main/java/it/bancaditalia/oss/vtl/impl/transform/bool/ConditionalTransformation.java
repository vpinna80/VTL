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

import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.model.data.UnknownValueMetadata.INSTANCE;
import static it.bancaditalia.oss.vtl.util.Utils.toMapWithValues;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.concat;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLSyntaxException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.LightF2DataSet;
import it.bancaditalia.oss.vtl.model.data.Component.Identifier;
import it.bancaditalia.oss.vtl.model.data.Component.Measure;
import it.bancaditalia.oss.vtl.model.data.Component.NonIdentifier;
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
		
		if (metadata instanceof ScalarValueMetadata)
			return BOOLEANDS.cast((ScalarValue<?, ?, ?>) cond).get() 
					? thenExpr.eval(session)
					: elseExpr.eval(session);
		else
		{
			DataSet condD = (DataSet) cond;
			Set<DataStructureComponent<Identifier, ?, ?>> keys = ((DataSetMetadata) metadata).getComponents(Identifier.class);
			VTLValue thenV = thenExpr.eval(session);
			VTLValue elseV = elseExpr.eval(session);
			DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> booleanConditionMeasure = condD.getComponents(Measure.class, BOOLEANDS).iterator().next();

			Function<DataPoint, Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>>> lambda;

			if (thenV instanceof DataSet && elseV instanceof DataSet) // Two datasets
			{
				DataSet joinedThen = condD.filter(dpCond -> checkCondition(dpCond.get(booleanConditionMeasure))).mappedJoin((DataSetMetadata) metadata, (DataSet) thenV, (dpCond, dpThen) -> dpThen);
				DataSet joinedElse = condD.filter(dpCond -> !checkCondition(dpCond.get(booleanConditionMeasure))).mappedJoin((DataSetMetadata) metadata, (DataSet) elseV, (dpCond, dpElse) -> dpElse);
				return new LightF2DataSet<>((DataSetMetadata) metadata, (dsThen, dsElse) -> concat(dsThen.stream(), dsElse.stream()), joinedThen, joinedElse);
			}
			else // One dataset and one scalar
			{
				DataSet dataset = ((DataSet) (thenV instanceof DataSet ? thenV : elseV));
				Map<? extends DataStructureComponent<? extends NonIdentifier, ?, ?>, ? extends ScalarValue<?, ?, ?>> scalar = ((DataSetMetadata) metadata)
						.getComponents(NonIdentifier.class).stream()
						.collect(toMapWithValues(k -> (ScalarValue<?, ?, ?>) (thenV instanceof ScalarValue ? thenV : elseV)));

				lambda = dpCond -> (checkCondition(dpCond.get(booleanConditionMeasure)) ^ thenV != dataset)
						// condition true and 'then' is a dataset or condition false and 'else' is a dataset 
						? dataset.getMatching(dpCond.getValues(keys, Identifier.class))
								.stream()
								.findFirst()
								.get()
								.getValues(NonIdentifier.class)
						: scalar;
			}

			return condD.mapKeepingKeys((DataSetMetadata) metadata, lambda);
		}
	}

	private boolean checkCondition(ScalarValue<?, ?, ?> value)
	{
		return value instanceof BooleanValue && (boolean) (Boolean) value.get();
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
		else if (cond instanceof ScalarValueMetadata && BOOLEANDS.isAssignableFrom(((ScalarValueMetadata<?>) cond).getDomain()))
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return metadata = left;
			else
				throw new UnsupportedOperationException("Incompatible types in conditional expression: " + left + ", " + right);
		else // if (cond instanceof VTLDataSetMetadata)
		{
			if (left instanceof ScalarValueMetadata && right instanceof ScalarValueMetadata)
				return metadata = left;
			Set<? extends DataStructureComponent<?, ?, ?>> measures = ((DataSetMetadata) cond).getComponents(Measure.class, BOOLEANDS);
			DataSetMetadata dataset = (DataSetMetadata) (left instanceof DataSetMetadata ? left : right);
			VTLValueMetadata other = left instanceof DataSetMetadata ? right : left;

			if (measures.size() != 1)
				throw new VTLExpectedComponentException(Measure.class, BOOLEANDS, measures);

			if (!dataset.getComponents(Identifier.class).equals(((DataSetMetadata) cond).getComponents(Identifier.class)))
				throw new UnsupportedOperationException("Condition must have same identifiers as other expressions: " + dataset.getComponents(Identifier.class) + " -- " + ((DataSetMetadata) cond).getComponents(Identifier.class));

			if (other instanceof DataSetMetadata)
			{
				if (!dataset.equals(other))
				{
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
				if (!dataset.getComponents(Measure.class).stream()
						.allMatch(c -> ((ScalarValueMetadata<?>) other).getDomain().isAssignableFrom(c.getDomain())))
				throw new UnsupportedOperationException("All measures must be assignable from " + ((ScalarValueMetadata<?>) other).getDomain() + ": " + dataset.getComponents(Measure.class));

			return metadata = dataset;
		}
	}
	
	@Override
	public String toString()
	{
		return "if " + condition + " then " + thenExpr + " else " + elseExpr;
	}
}
