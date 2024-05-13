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
package it.bancaditalia.oss.vtl.impl.cli;

import static it.bancaditalia.oss.vtl.util.Utils.coalesce;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

import it.bancaditalia.oss.vtl.config.ConfigurationManager;
import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.model.data.DataSet;
import it.bancaditalia.oss.vtl.model.data.VTLValue;
import it.bancaditalia.oss.vtl.session.VTLSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "java -jar vtl-cli.jar", mixinStandardHelpOptions = true, description = "Parses a set of VTL rules and computes one or more results.",
		separator = " ")
public class VTLShell implements Callable<Void>
{
	private static final String FILE_DESC = "Input VTL script. If not specified, read from stdin.";
	private static final String NAMES_DESC = "Rule names whose values will be printed. If none is specified, print the values of all rules.";
	
	@Option(names = { "-f", "--file" }, description = FILE_DESC, paramLabel = "file")
	File file;
	
	@Parameters(description = NAMES_DESC, paramLabel = "rulename") 
	String[] names;
	
	public static void main(String[] args)
	{
		System.setProperty("vtl.r", "disable");
		new CommandLine(VTLShell.class).execute(args);
	}

	public Void call() throws Exception
	{
		Path properties = Path.of(System.getProperty("user.home"), ".vtlStudio.properties");
		if (!Files.exists(properties))
			throw new IllegalStateException("Please first configure your working environment with VTL Studio.");
		if (names == null)
			throw new InvalidParameterException("Please specify one or more rules to compute and retrieve.");
		
		try (BufferedReader reader = Files.newBufferedReader(properties))
		{
			ConfigurationManagerFactory.loadConfiguration(reader);
		}
		
		ConfigurationManager manager = ConfigurationManagerFactory.getInstance();
		
		VTLSession session;
		try (Reader reader = file != null ? Files.newBufferedReader(file.toPath(), UTF_8) : new BufferedReader(new InputStreamReader(System.in, UTF_8)))
		{
			session = manager.createSession(reader);
			session.compile();
		}
			
		List<String> outNames = coalesce(Arrays.asList(names), session.getWorkspace().getRules().stream().map(Statement::getAlias).collect(toList())); 
		for (String name: outNames)
		{
			final VTLValue result = session.resolve(name);
			System.out.println(result + " := {");
			if (result instanceof DataSet)
				((DataSet) result).forEach(dp -> { System.out.print("\t"); System.out.println(dp); });
			System.out.println("}\n");
		}
		
		return null;
	}
}