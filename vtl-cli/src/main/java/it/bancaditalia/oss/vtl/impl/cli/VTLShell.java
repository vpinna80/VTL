package it.bancaditalia.oss.vtl.impl.cli;

import static java.util.stream.Collectors.toList;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Callable;

import it.bancaditalia.oss.vtl.engine.Statement;
import it.bancaditalia.oss.vtl.session.VTLSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "java -jar vtl-cli.jar", mixinStandardHelpOptions = true, description = "Parses a set of VTL rules and computes one or more results.",
		separator = " ")
public class VTLShell implements Callable<Void>
{
	private static final String FILE_DESC = "Input VTL script. If missing, read rules from stdin.";
	private static final String NAMES_DESC = "Rule names whose values will be printed. If none is specified, print the values of all rules.";
	
	@Option(names = { "-f", "--file" }, description = FILE_DESC, paramLabel = "file")
	File file;
	
	@Parameters(description = NAMES_DESC, paramLabel = "rulename") 
	String[] names;
	
	public static void main(String... args)
	{
		System.setProperty("vtl.r", "disable");
		new CommandLine(VTLShell.class).execute(args);
	}

	public Void call() throws Exception
	{
		VTLSession session = VTLSession.getInstances().iterator().next();
		try (Reader reader = new InputStreamReader(file != null ? new FileInputStream(file) : System.in, StandardCharsets.UTF_8))
		{
			session.addStatements(reader);
			
			if (names != null)
				for (String name: names)
					System.out.println(session.resolve(name));
			else
				for (String name: session.getWorkspace().getRules().stream().map(Statement::getId).collect(toList()))
					System.out.println(session.resolve(name));
		}
		
		return null;
	}
}