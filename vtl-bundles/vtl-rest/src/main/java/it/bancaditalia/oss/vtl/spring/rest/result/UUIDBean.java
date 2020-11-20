/**
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
