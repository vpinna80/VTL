package it.bancaditalia.oss.vtl.impl.transform.scope;

import static it.bancaditalia.oss.vtl.util.Utils.byKey;
import static java.util.stream.Collectors.toConcurrentMap;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;
import it.bancaditalia.oss.vtl.model.data.ScalarValue.VTLScalarValueMetadata;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.model.data.VTLValue.VTLValueMetadata;
import it.bancaditalia.oss.vtl.model.transform.TransformationScheme;
import it.bancaditalia.oss.vtl.session.MetadataRepository;

public class JoinApplyScope implements TransformationScheme
{
	private final Map<String, ScalarValue<?, ?, ?>> joinValues;
	private final Map<String, VTLScalarValueMetadata<?>> joinMeta;
	private final TransformationScheme parent;

	public JoinApplyScope(TransformationScheme parent, String measureName, DataPoint joinedDataPoint)
	{
		this.parent = parent;
		this.joinValues = joinedDataPoint.entrySet().stream()
				.filter(byKey(c -> measureName.equals(c.getName().replaceAll("^.*#", ""))))
				.collect(toConcurrentMap(e -> e.getKey().getName().replaceAll("#.*", ""), Entry::getValue));
		this.joinMeta = null;
	}

	public JoinApplyScope(TransformationScheme parent, String measureName, Set<DataStructureComponent<?, ?, ?>> joinedComponents)
	{
		this.parent = parent;
		this.joinValues = null;
		this.joinMeta = joinedComponents.stream()
				.filter(c -> measureName.equals(c.getName().replaceAll("^.*#", "")))
				.collect(toConcurrentMap(c -> c.getName().replaceAll("#.*", ""), c -> (VTLScalarValueMetadata<?>) c::getDomain));
	}

	@Override
	public MetadataRepository getRepository()
	{
		return parent.getRepository();
	}

	@Override
	public VTLValue resolve(String node)
	{
		if (joinValues != null && joinValues.containsKey(node))
			return joinValues.get(node);
		else
			return parent.resolve(node);
	}

	@Override
	public VTLValueMetadata getMetadata(String node)
	{
		if (joinValues != null && joinValues.containsKey(node))
			return joinValues.get(node).getMetadata();
		else if (joinMeta != null && joinMeta.containsKey(node))
			return joinMeta.get(node);
		else
			return parent.getMetadata(node);
	}

	@Override
	public Statement getRule(String node)
	{
		return parent.getRule(node);
	}
}
