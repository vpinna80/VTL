package it.bancaditalia.oss.vtl.spring.rest.result;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import it.bancaditalia.oss.vtl.model.data.DataPoint;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.DataStructureComponent;
import it.bancaditalia.oss.vtl.model.data.ScalarValue;

public class DataSetResultBean extends ResultBean
{
	private static final long serialVersionUID = 1L;
	
	private final List<ComponentBean> structure;
	private final Stream<DataPoint> datapoints;
	
	private static class DataPointSerializer extends JsonSerializer<Stream<DataPoint>> 
	{
	    @Override
	    public void serialize(Stream<DataPoint> stream, JsonGenerator gen, SerializerProvider serializers) throws IOException, JsonProcessingException
	    {
	    	gen.writeStartArray();
	    	try
	    	{
	    		stream.forEach(dp -> serializeSingle(gen, dp));
	    	}
	    	catch (UncheckedIOException e)
	    	{
	    		throw (IOException) e.getCause();
	    	}
	    	finally
	    	{
	    		stream.close();
	    	}
	    	gen.writeEndArray();
	    }
	    
	    public synchronized void serializeSingle(JsonGenerator gen, DataPoint dp) throws UncheckedIOException
	    {
	    	try
	    	{
	    		gen.writeStartObject();
	    		
		    	for (Entry<DataStructureComponent<?, ?, ?>, ScalarValue<?, ?, ?>> entry: dp.entrySet())
		    	{
		    		gen.writeFieldName(entry.getKey().getName());
		    		gen.writeRawValue(entry.getValue().toString());
		    	}
	    		
		    	gen.writeEndObject();
	    	}
	    	catch (IOException e)
	    	{
	    		throw new UncheckedIOException(e);
	    	}
	    }
	}

	public DataSetResultBean(DataSet dataset)
	{
		super("DATASET");
		
		structure = dataset.getMetadata().stream().map(ComponentBean::new).collect(toList());
		datapoints = dataset.stream();
	}

	public List<ComponentBean> getStructure()
	{
		return structure;
	}

	@JsonSerialize(using = DataPointSerializer.class)
	public Stream<DataPoint> getDatapoints()
	{
		return datapoints;
	}
}
