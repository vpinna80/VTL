package it.bancaditalia.oss.vtl.spring.rest.result;

import java.io.Serializable;
import java.util.UUID;

import org.springframework.lang.NonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class UUIDBean implements Serializable
{
	private static final long serialVersionUID = 1L;

	private final UUID uuid; 
	
	@JsonCreator
	public UUIDBean(@JsonProperty("uuid") @NonNull UUID uuid)
	{
		this.uuid = uuid;
	}

	@JsonProperty("uuid")
	public @NonNull UUID getUuid()
	{
		return uuid;
	}
}
