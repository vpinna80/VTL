package it.bancaditalia.oss.vtl.eclipse.menus;

import static it.bancaditalia.oss.vtl.eclipse.parts.NavigatorPart.NAVIGATOR_PART_ID;

import java.io.IOException;

import javax.inject.Inject;

import org.eclipse.e4.core.di.annotations.CanExecute;
import org.eclipse.e4.core.di.annotations.Execute;
import org.eclipse.e4.ui.workbench.modeling.EPartService;
import org.eclipse.jface.wizard.Wizard;
import org.eclipse.jface.wizard.WizardDialog;
import org.eclipse.swt.widgets.Shell;

import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLFolder;
import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLProject;
import it.bancaditalia.oss.vtl.eclipse.parts.NavigatorPart;
import it.bancaditalia.oss.vtl.eclipse.wizards.NewFolderSelectorPage;

public class MenuNewFolder
{
	@Inject
	private EPartService partService;

	@Execute
	public void execute(Shell shell) throws IOException
	{
		NavigatorPart navigator = ((NavigatorPart) partService.findPart(NAVIGATOR_PART_ID).getObject());
		Wizard wizard = new Wizard() {
			@Override
			public boolean performFinish()
			{
				return true;
			}
		};
		wizard.addPage(new NewFolderSelectorPage(navigator.getProjects(), navigator.getSelection()));
		if (new WizardDialog(shell, wizard).open() == WizardDialog.OK)
		{
			navigator.addItemToSelection(new VTLFolder("New Folder"));
		}
	}
	
	@CanExecute
	public boolean canExecute()
	{
		NavigatorPart navigator = ((NavigatorPart) partService.findPart(NAVIGATOR_PART_ID).getObject());
		Object selected = navigator.getSelection().getPaths()[0].getLastSegment();
		return selected instanceof VTLProject || selected instanceof VTLFolder;
	}
}
