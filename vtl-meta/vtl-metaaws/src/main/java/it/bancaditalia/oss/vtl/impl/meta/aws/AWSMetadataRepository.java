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
package it.bancaditalia.oss.vtl.impl.meta.aws;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3ObjectId;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import it.bancaditalia.oss.vtl.impl.meta.InMemoryMetadataRepository;
import it.bancaditalia.oss.vtl.impl.types.dataset.DataStructureBuilder;
import it.bancaditalia.oss.vtl.model.data.DataSetMetadata;

public class AWSMetadataRepository extends InMemoryMetadataRepository
{
	private static final long serialVersionUID = 1L;
	private static final Map<String, String> JSONTOVTL = new HashMap<>();
	
	static
	{
		JSONTOVTL.put("number", "number");
	}
	
	private final AmazonS3 client;

	public AWSMetadataRepository()
	{
		super();
		
		client = AmazonS3ClientBuilder.defaultClient();
	}
	
	@Override
	public DataSetMetadata getStructure(String name)
	{
		try
		{
			ObjectMetadata metadata = client.getObject(new GetObjectRequest(new S3ObjectId("bucket", "name"))).getObjectMetadata();
			
			JsonNode root = new ObjectMapper().readTree(metadata.getUserMetaDataOf("?????"));
			
			DataStructureBuilder builder = new DataStructureBuilder();
			for (String node: (Iterable<String>) root::fieldNames)
			{
				
			}
			
			return builder.build();
		}
		catch (Exception e)
		{
			throw new IllegalStateException("Exception on getting metadata from s3 for '" + name + "'", e);
		}
	}
}
