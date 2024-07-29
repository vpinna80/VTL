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
package it.bancaditalia.oss.vtl.impl.jupyter;

import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.ENVIRONMENT_IMPLEMENTATION;
import static it.bancaditalia.oss.vtl.config.VTLGeneralProperties.SESSION_IMPLEMENTATION;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.function.Predicate.not;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.json.JsonMapper;

import it.bancaditalia.oss.vtl.config.ConfigurationManagerFactory;
import it.bancaditalia.oss.vtl.environment.Workspace;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

@Command(name = "java -jar vtl-jupyter.jar", mixinStandardHelpOptions = true, description = "Launches or installs the VTL jupyter kernel.", 
		separator = " ", versionProvider = VTLKernelLauncher.class)
public class VTLKernelLauncher implements Runnable, IVersionProvider
{
	static class Operation
	{
		@Option(names = { "-i", "--install" }, description = "Installs this kernel into the Jupyter data path.")
		boolean install;
		@Option(names = { "-e", "--exec" }, description = "Run the VTL E&E kernel. Should only be called by Jupyter.")
		File exec;
	}

	@Option(names = { "--jupyter" }, description = "Location of Jupyter executable.", paramLabel = "jupyter", defaultValue = "jupyter")
	Path jupyter;

	@Option(names = { "-c", "--conf" }, description = "The configuration file to use. The file must be accessible both at installation and at run time", paramLabel = "conf")
	Path conf;
	
	@ArgGroup(exclusive = true, multiplicity = "1")
	Operation operation;

	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException
	{
		new CommandLine(VTLKernelLauncher.class).execute(args);
	}

	@Override
	public void run()
	{
		try
		{
			if (operation.install)
				install();
			else
				exec();
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	private void exec() throws IOException, InvalidKeyException, NoSuchAlgorithmException
	{
		Map<String, Object> connInfo;
		try (JsonParser parser = JsonFactory.builder().build().setCodec(new JsonMapper()).createParser(operation.exec))
		{
			connInfo = parser.readValueAs(Map.class);
		}

		if (conf == null)
			conf = Paths.get(System.getProperty("user.home") + "/.vtlStudio.properties");
		if (Files.exists(conf) && Files.isRegularFile(conf) && Files.isReadable(conf));
			ConfigurationManagerFactory.loadConfiguration(Files.newBufferedReader(conf));

		// Change the session implementation with the Jupyter specific one
		SESSION_IMPLEMENTATION.setValue(VTLJupyterSession.class.getName());
		
		// Change the standard Workspace implementation class with the Jupyter specific one
		ENVIRONMENT_IMPLEMENTATION.setValues(
				Stream.concat(ENVIRONMENT_IMPLEMENTATION.getValues().stream()
					.filter(not(cls -> {
						try
						{
							return Workspace.class.isAssignableFrom(Class.forName(cls, true, Thread.currentThread().getContextClassLoader()));
						}
						catch (ClassNotFoundException e)
						{
							throw new ExceptionInInitializerError(e);
						}
					})),
					Stream.of(JupyterWorkspace.class.getName())).toArray(String[]::new));

		new VTLJupyterKernel(connInfo);
	}

	private void install() throws IOException, InterruptedException
	{
		String[] cp = System.getProperty("java.class.path").split(File.pathSeparator);
		if (cp.length > 1)
		{
			System.err.println("The program must be launched with the -jar option.");
			return;
		}
		String java = ProcessHandle.current().info().command().orElse("java");
		Path jar = Paths.get(cp[0]);
		if (!Files.isRegularFile(jar))
		{
			System.err.println("The program must be launched with the -jar option.");
			return;
		}

		Process process = Runtime.getRuntime().exec("\"" + jupyter + "\" --data");
		if (processError(process))
			return;

		@SuppressWarnings("resource")
		Path kernelPath = Paths.get(new BufferedReader(new InputStreamReader(process.getInputStream(), UTF_8)).readLine()).resolve("kernels").resolve("VTL_EnE");
		if (!Files.isDirectory(kernelPath))
			Files.createDirectory(kernelPath);
		Path copyJar = Files.copy(jar, kernelPath.resolve("vtl-jupyter.jar"), REPLACE_EXISTING);

		Map<String, Object> kernelspec = new LinkedHashMap<>();
		kernelspec.put("argv", List.of(java, "-jar", copyJar.toString(), "--exec", "{connection_file}"));
		kernelspec.put("display_name", "VTL E&E");
		kernelspec.put("interrupt_mode", "message");
		kernelspec.put("language", "VTL");

		if (conf == null)
			conf = Paths.get(System.getProperty("user.home") + "/.vtlStudio.properties");
		if (!Files.exists(conf) || !Files.isRegularFile(conf) || !Files.isReadable(conf));
			ConfigurationManagerFactory.loadConfiguration(Files.newBufferedReader(conf));
		
		try (FileWriter writer = new FileWriter(kernelPath.resolve("kernel.json").toFile());
				JsonGenerator generator = JsonFactory.builder().build().setCodec(new JsonMapper()).createGenerator(writer))
		{
			generator.useDefaultPrettyPrinter().writeObject(kernelspec);
		}
		
		System.out.println("VTL E&E Kernel has been installed in " + kernelPath.toString());
	}

	private boolean processError(Process process) throws IOException, InterruptedException
	{
		if (process.waitFor() != 0)
			try (InputStream error = process.getErrorStream())
			{
				byte buffer[] = new byte[10000];
				for (int c = 0; (c = error.read(buffer)) > 0;)
					System.err.write(buffer, 0, c);
				System.err.flush();
				return true;
			}
		else
			return false;
	}

	@Override
	public String[] getVersion()
	{
		return new String[] { VTLKernelLauncher.class.getPackage().getImplementationVersion() };
	}
}
