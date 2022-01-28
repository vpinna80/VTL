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

import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Input.DATASET_PRIORITY;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.INVALID;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.VarIDOperand;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLExpectedComponentException;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.domain.Domains;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.Hierarchy;
import it.bancaditalia.oss.vtl.model.data.Lineage;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;

public class CheckHierarchyTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = LoggerFactory.getLogger(CheckHierarchyTransformation.class);
	
	public enum Input
	{
		DATASET, DATASET_PRIORITY;
	}
	
	public enum Output
	{
		ALL, INVALID, ALL_MEASURES;
	}
	
	private final Transformation operand;
	private final VarIDOperand hierarchyId;
	private final Output output;
	private final Hierarchy.CheckMode mode;
	private final Input input;
	
//	private DataStructureComponent<? extends Measure, ?, ?> measure;
//	private DataStructureComponent<?, ?, ?> ruleKey;
	
	public CheckHierarchyTransformation(Transformation operand, VarIDOperand hierarchyId, Hierarchy.CheckMode mode, Input input, Output output)
	{
//		LOGGER.warn("check_hierarchy: Implementation diverges from specification: datapoints outside {} will not be checked.", operand);
//		
//		this.operand = operand;
//		this.hierarchyId = hierarchyId;
//		this.mode = mode == null ? NON_NULL : mode;
//		this.input = input == null ? DATASET : input;
//		this.output = output == null ? INVALID : output;
		
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isTerminal()
	{
		return false;
	}

	@Override
	public Set<LeafTransformation> getTerminals()
	{
		return operand.getTerminals();
	}

	@Override
	public VTLValueMetadata computeMetadata(TransformationScheme session)
	{
		if (input == DATASET_PRIORITY)
			throw new UnsupportedOperationException("check_hierarchy: " + input + " not supported");	

		VTLValueMetadata opValue = operand.getMetadata(session);
		VTLValueMetadata hierValue = hierarchyId.getMetadata(session);
		
		if (!(opValue instanceof DataSetMetadata))
			throw new VTLInvalidParameterException(opValue, DataSetMetadata.class);
		if (!(hierValue instanceof Hierarchy))
			throw new VTLInvalidParameterException(hierValue, Hierarchy.class);
		
		DataSetMetadata dataset = (DataSetMetadata) operand.getMetadata(session);
		Hierarchy hierarchy = (Hierarchy) hierValue;

		if (dataset.getComponents(Measure.class).size() != 1)
			throw new VTLExpectedComponentException(Measure.class, dataset.getComponents(Measure.class));
		
		DataStructureComponent<Measure, ?, ?> measure = dataset.getComponents(Measure.class).iterator().next();
		DataStructureComponent<?, ?, ?> ruleKey = hierarchy.selectComponent(dataset);
		
		LOGGER.trace("Measure is {} and rule key is {}", measure, ruleKey);
		
		if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
			throw new VTLIncompatibleTypesException("check_hierarchy", NUMBERDS, measure.getDomain());

		DataStructureBuilder builder = new DataStructureBuilder(dataset.getComponents(Identifier.class))
				.addComponent("ruleid",     Identifier.class, Domains.STRINGDS)
				.addComponent("imbalance",  Measure.class,    measure.getDomain())
				.addComponent("errorcode",  Measure.class,    Domains.STRINGDS)
				.addComponent("errorlevel", Measure.class,    Domains.INTEGERDS)
				.removeComponent(ruleKey);
		
		if (output != ALL)
			builder = builder.addComponent(measure);
		
		if (output != INVALID)
			builder = builder.addComponent(new DataStructureComponentImpl<>("bool_var", Measure.class, Domains.BOOLEANDS));

		return builder.build();
	}

	@Override
	public VTLValue eval(TransformationScheme session)
	{
		/*DataSet dataset = (DataSet) operand.eval(session.uniqueName(), session);
		Hierarchy hierarchy = (Hierarchy) hierarchyId.eval(session.uniqueName(), session);
		Set<DataStructureComponent<Identifier, ?, ?>> identifiers = new HashSet<>(metadata.get().getComponents(Identifier.class));
		identifiers.remove(ruleKey);
		DataSetIndex byIds = dataset.getIndex(identifiers);
		DataStructureComponent<Measure, BooleanDomainSubset, BooleanDomain> bool_var = new DataStructureComponentImpl<>("bool_var", Measure.class, Domains.BOOLEANDS);
		
		Stream<DataPoint> stream = byIds.streamByKeys()
			.flatMap(e -> Utils.getStream(hierarchy.getRuleItems())
					.map(rule -> new Triple<>(rule, e)))
			.map(splitting((rule, keys, group) -> { 
					HashMap<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?, ?>> res = new HashMap<>(keys); 
					res.put(ruleKey, StringValue.of(rule.getCodeItem()));
					return new Triple<>(rule, res, group); 
			// ==> rule, keys, dataset 
			})).map(splitting((rule, keys, group) -> new SimpleEntry<>(keys, rule.validate(measure, mode, group.get()
					.collect(toConcurrentMap(dp -> dp.get(ruleKey), dp -> dp.get(measure)))))))
			.filter(byValue(map -> !map.isEmpty()))
			.filter(byValue(map -> output != INVALID || (Boolean) map.get(bool_var).get()))
			.map(splitting((k, v) -> new DataPointBuilder(k).addAll(v)))
			.map(dpb -> dpb.delete(ruleKey))
			.map(dpb -> output == INVALID ? dpb.delete(bool_var) : dpb)
			.map(dpb -> output == ALL ? dpb.delete(measure) : dpb)
			.map(dpb -> dpb.build(metadata.get()));
		
		return null; //AbstractDataSet.from(alias, metadata.get(), () -> );*/
		throw new UnsupportedOperationException("check hierarchical ruleset not implemented");
	}
	
	@Override
	public String toString()
	{
		return "CHECK_HIERARCHY(" + operand + ", " + hierarchyId + " " + mode + " " + input + " " + output + ")"; 
	}
	
	@Override
	public Lineage computeLineage()
	{
		throw new UnsupportedOperationException(); 
	}
}
