package it.bancaditalia.oss.vtl.config;

import static java.util.Collections.singletonList;

import java.util.Arrays;
import java.util.List;

public interface VTLProperty
{
	public String getName();

	public String getValue();

	public void setValue(String newValue);

	public String getDescription();

	public String getPlaceholder();

	public boolean isMultiple();

	public boolean isRequired();
	
	public default List<String> getValues()
	{
		return getValue() == null ? null : isMultiple() ? Arrays.asList(getValue().split(",")) : singletonList(getValue());
	}
}