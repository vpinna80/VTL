package it.bancaditalia.oss.vtl.eclipse.menus;

import static org.eclipse.e4.ui.workbench.modeling.EPartService.PartState.ACTIVATE;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.inject.Inject;

import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.ui.model.application.ui.basic.MPart;
import org.eclipse.e4.ui.workbench.modeling.EPartService;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Shell;

public class MenuFileOpen
{
	@Inject EPartService partService;
	
	@Execute
	public void execute(Shell shell) throws IOException
	{
        FileDialog fileDialog = new FileDialog(shell, SWT.OPEN);
        fileDialog.setText("Open...");
        fileDialog.setFilterExtensions(new String[] { "*.vtl", "*.*" });
        fileDialog.setFilterNames(new String[] { "VTL Scripts (*.vtl)", "All files (*.*)" });
        String fileName = fileDialog.open();
        
        if (fileName != null && Files.isRegularFile(Paths.get(fileName)))
        {
        	MPart editorPart = partService.createPart("vtl-eclipse-app.editor.template");
        	editorPart.getTransientData().put("fileName", new File(fileName));
        	partService.showPart(editorPart, ACTIVATE);
        }
	}
}
