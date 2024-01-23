package it.bancaditalia.oss.vtl.impl.transform.ops;

import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_NULL;
import static it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode.NON_ZERO;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Input.DATASET;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Input.DATASET_PRIORITY;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.ALL;
import static it.bancaditalia.oss.vtl.impl.transform.ops.CheckHierarchyTransformation.Output.INVALID;
import static it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder.Option.DONT_SYNC;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.BOOLEANDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.INTEGERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.NUMBERDS;
import static it.bancaditalia.oss.vtl.impl.types.domain.Domains.STRINGDS;
import static it.bancaditalia.oss.vtl.model.data.DataStructureComponent.normalizeAlias;
import static it.bancaditalia.oss.vtl.util.ConcatSpliterator.concatenating;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toConcurrentMap;
import static it.bancaditalia.oss.vtl.util.SerCollectors.toList;
import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import it.bancaditalia.oss.vtl.exceptions.VTLException;
import it.bancaditalia.oss.vtl.exceptions.VTLMissingComponentsException;
import it.bancaditalia.oss.vtl.impl.transform.TransformationImpl;
import it.bancaditalia.oss.vtl.impl.transform.aggregation.HierarchyTransformation.HierarchyMode;
import it.bancaditalia.oss.vtl.impl.transform.exceptions.VTLInvalidParameterException;
import it.bancaditalia.oss.vtl.impl.types.data.BooleanValue;
import it.bancaditalia.oss.vtl.impl.types.data.DoubleValue;
import it.bancaditalia.oss.vtl.impl.types.data.NullValue;
import it.bancaditalia.oss.vtl.impl.types.data.StringValue;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataPointBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureComponentImpl;
import it.bancaditalia.oss.vtl.impl.types.dataset.StreamWrapperDataSet;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireBooleanDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireIntegerDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireNumberDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.domain.EntireStringDomainSubset;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLIncompatibleTypesException;
import it.bancaditalia.oss.vtl.impl.types.exceptions.VTLSingletonComponentRequiredException;
import it.bancaditalia.oss.vtl.impl.types.lineage.LineageNode;
import it.bancaditalia.oss.vtl.model.data.CodeItem;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Identifier;
import it.bancaditalia.oss.vtl.model.data.ComponentRole.Measure;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.NumberValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.domain.BooleanDomain;
import it.bancaditalia.oss.vtl.model.domain.IntegerDomain;
import it.bancaditalia.oss.vtl.model.domain.NumberDomain;
import it.bancaditalia.oss.vtl.model.domain.StringDomain;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet;
import it.bancaditalia.oss.vtl.model.rules.HierarchicalRuleSet.Rule;
import it.bancaditalia.oss.vtl.model.transform.LeafTransformation;
import it.bancaditalia.oss.vtl.model.transform.Transformation;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.util.Utils;

public class CheckHierarchyTransformation extends TransformationImpl
{
	private static final long serialVersionUID = 1L;
	private static final DataStructureComponent<Identifier, EntireStringDomainSubset, StringDomain> RULEID = new DataStructureComponentImpl<>("ruleid", Identifier.class, STRINGDS); 
	private static final DataStructureComponent<Measure, EntireBooleanDomainSubset, BooleanDomain> BOOL_VAR = new DataStructureComponentImpl<>("bool_var", Measure.class, BOOLEANDS); 
	private static final DataStructureComponent<Measure, EntireNumberDomainSubset, NumberDomain> IMBALANCE = new DataStructureComponentImpl<>("imbalance", Measure.class, NUMBERDS); 
	private static final DataStructureComponent<Measure, EntireStringDomainSubset, StringDomain> ERRORCODE = new DataStructureComponentImpl<>("errorcode", Measure.class, STRINGDS); 
	private static final DataStructureComponent<Measure, EntireIntegerDomainSubset, IntegerDomain> ERRORLEVEL = new DataStructureComponentImpl<>("errorlevel", Measure.class, INTEGERDS); 

	public enum Input
	{
		DATASET, DATASET_PRIORITY;
	}
	
	public enum Output
	{
		INVALID, ALL, ALL_MEASURES;
	}

	private final Transformation operand;
	private final String rulesetID;
	private final List<String> conditions;
	private final String id;
	private final HierarchyMode mode;
	private final Input input;
	private final Output output;
	
	public CheckHierarchyTransformation(Transformation operand, String rulesetID, List<String> conditions, String id, HierarchyMode mode, Input input, Output output)
	{
		this.operand = operand;
		this.rulesetID = normalizeAlias(requireNonNull(rulesetID));
		this.conditions = coalesce(conditions, new ArrayList<>()).stream().map(DataStructureComponent::normalizeAlias).collect(toList());
		
		this.id = id != null ? normalizeAlias(id) : null;
		this.mode = coalesce(mode, NON_NULL);
		this.input = coalesce(input, DATASET);
		this.output = coalesce(output, INVALID);
		
		if (!this.conditions.isEmpty())
			throw new UnsupportedOperationException("check_hierarchy conditioning components not implemented.");
		if (this.input == DATASET_PRIORITY)
			throw new UnsupportedOperationException("check_hierarchy dataset_priority option not implemented.");
		if (this.mode != NON_NULL && this.mode != NON_ZERO)
			throw new UnsupportedOperationException("hierarchy " + this.mode.toString().toLowerCase() + " option is not implemented.");
	}
	
	@Override
	public VTLValue eval(TransformationScheme scheme)
	{
		DataSet dataset = (DataSet) operand.eval(scheme);
		
		HierarchicalRuleSet<?, ?, ?, ?, ?> ruleset = scheme.findHierarchicalRuleset(rulesetID);
		if (ruleset.isValueDomainHierarchy())
		{
			List<Rule<?, ?, ?, ?>> allRules = ruleset.getRules().values().stream().flatMap(List::stream).collect(toList());
			Set<DataStructureComponent<Identifier, ?, ?>> ids = new HashSet<>(dataset.getMetadata().getIDs());
			DataStructureComponent<?, ?, ?> idComp = dataset.getComponent(id).orElseThrow(() -> new VTLMissingComponentsException(id, ids));
			ids.remove(idComp);
			DataStructureComponent<Measure, ?, ?> measure = dataset.getMetadata().getMeasures().iterator().next();
			DataSetMetadata newStructure = (DataSetMetadata) this.getMetadata(scheme);

			List<DataPoint> results = dataset.streamByKeys(ids, toConcurrentMap(dp -> (CodeItem<?, ?, ?, ?>) dp.get(idComp), dp -> dp.get(measure)), (codeVals, kv) -> {
					List<DataPoint> result = new ArrayList<>();
					for (Rule<?, ?, ?, ?> rule: allRules)
					{
						double imbalance = 0;
						for (CodeItem<?, ?, ?, ?> rightCode: rule.getRightCodeItems())
						{
							ScalarValue<?, ?, ?, ?> dpValue = codeVals.get(rightCode);
							if (dpValue == null && mode == NON_NULL || dpValue instanceof NullValue)
								imbalance = NaN;
							else if (dpValue != null && !(dpValue instanceof NullValue))
								imbalance += (rule.isPlusSign(rightCode) ? 1 : -1) * ((Number) dpValue.get()).doubleValue();
						}
						
						ScalarValue<?, ?, ?, ?> original = codeVals.get(rule.getLeftCodeItem());
						if (original instanceof NumberValue && !Double.isNaN(imbalance))
							imbalance -= ((Number) original.get()).doubleValue();
						boolean sat = Double.compare(imbalance, 0.0) != 0;
						
						if (output != INVALID || sat && !Double.isNaN(imbalance))
						{
							DataPointBuilder builder = new DataPointBuilder(kv, DONT_SYNC);
							if (output != ALL)
								builder = builder.add(measure, original);
							if (output != INVALID)
								builder = builder.add(BOOL_VAR, BooleanValue.of(sat));
	
							result.add(builder.add(idComp, rule.getLeftCodeItem())
									.add(RULEID, StringValue.of(rule.getName()))
									.add(IMBALANCE, DoubleValue.of(imbalance))
									.add(ERRORCODE, rule.getErrorCode())
									.add(ERRORLEVEL, rule.getErrorLevel())
									.build(LineageNode.of("hierarchy"), newStructure));
						}
					}
					
					return result;
				}).map(Utils::getStream)
				.collect(concatenating(false))
				.collect(toList());
			
			return new StreamWrapperDataSet(newStructure, () -> Utils.getStream(results)); 
		}
		else
			throw new UnsupportedOperationException("check_hierarchy using ruleset on variable not supported");
	}
	
	@Override
	protected DataSetMetadata computeMetadata(TransformationScheme scheme)
	{
		VTLValueMetadata metadata = operand.getMetadata(scheme);
		
		if (metadata instanceof DataSetMetadata)
		{
			DataSetMetadata opMeta = (DataSetMetadata) metadata;
			
			HierarchicalRuleSet<?, ?, ?, ?, ?> ruleset = scheme.findHierarchicalRuleset(rulesetID);
			if (ruleset != null && ruleset.isValueDomainHierarchy())
			{
				DataStructureComponent<?, ?, ?> idComp = opMeta.getComponent(id).orElseThrow(() -> new VTLMissingComponentsException(id, opMeta.getIDs()));
				
				if (!ruleset.getDomain().isAssignableFrom(idComp.getDomain()))
					throw new VTLIncompatibleTypesException("check_hierarchy", idComp, ruleset.getDomain());
				
				if (opMeta.getMeasures().size() != 1)
					throw new VTLSingletonComponentRequiredException(Measure.class, NUMBERDS, opMeta);
				
				DataStructureComponent<Measure, ?, ?> measure = opMeta.getMeasures().iterator().next();
				if (!NUMBERDS.isAssignableFrom(measure.getDomain()))
					throw new VTLIncompatibleTypesException("check_hierarchy", measure, NUMBERDS);
			}
			else if (ruleset != null) 
				throw new UnsupportedOperationException("hierarchy on variable ruleset not implemented");
			else
				throw new VTLException("Hierarchical ruleset " + rulesetID + " not found.");
			
			DataStructureBuilder builder = new DataStructureBuilder(opMeta.getComponents(Identifier.class));
			if (output != ALL)
				builder = builder.addComponents(opMeta.getComponents(Measure.class));
			if (output != INVALID)
				builder = builder.addComponent(BOOL_VAR);
			
			return builder
					.addComponent(RULEID)
					.addComponent(IMBALANCE)
					.addComponent(ERRORCODE)
					.addComponent(ERRORLEVEL)
					.addComponents(opMeta.getComponents(Measure.class))
					.build();
		}
		else
			throw new VTLInvalidParameterException(metadata, DataSetMetadata.class);
	}

	@Override
	public String toString()
	{
		return 	"check_hierarchy(" + operand + ", " + rulesetID + (conditions.isEmpty() ? "" : " condition " + String.join(", ", conditions)) + (id == null ? "" : " rule " + id)
				+ " " + mode.toString().toLowerCase() + " " + input.toString().toLowerCase() + " " + output.toString().toLowerCase() + "\")";
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
}
