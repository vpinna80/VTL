package it.bancaditalia.oss.vtl.impl.domains;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import it.bancaditalia.oss.vtl.model.data.ValueDomainSubset;
import it.bancaditalia.oss.vtl.model.domain.StringCodeList;

public class CSVMetadataRepository extends InMemoryMetadataRepository
{
	public static final String METADATA_CSV_SOURCE = "vtl.metadata.csv.source";
	
	private final Map<String, StringCodeList> metadata = new ConcurrentHashMap<>();

	public CSVMetadataRepository() throws IOException
	{
		try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(System.getProperty(METADATA_CSV_SOURCE)), UTF_8)))
		{
			reader.lines()
				.map(l -> new SimpleEntry<>(l.split(",", 2)[0], l.split(",", 2)[1]))
				.collect(groupingByConcurrent(Entry::getKey, mapping(Entry::getValue, toSet())))
				.forEach(defineDomainOf(StringCodeListImpl.class));
		}
	}

	@Override
	public Collection<ValueDomainSubset<?>> getValueDomains()
	{
		Set<ValueDomainSubset<?>> result = new HashSet<>(metadata.values());
		result.addAll(super.getValueDomains());
		return result;
	}

	@Override
	public boolean isDomainDefined(String alias)
	{
		return metadata.containsKey(alias) || super.isDomainDefined(alias);
	}

	@Override
	public ValueDomainSubset<?> getDomain(String alias)
	{
		return metadata.containsKey(alias) ? metadata.get(alias) : super.getDomain(alias);
	}
}
