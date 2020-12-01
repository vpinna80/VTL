package it.bancaditalia.oss.vtl.eclipse.wizards;

import java.util.List;

import org.eclipse.jface.viewers.ITreeSelection;

import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLProject;

public class NewVTLScriptSelectorPage extends NewResourceSelectorPage
{
	public NewVTLScriptSelectorPage(List<VTLProject> projectTree, ITreeSelection selection)
	{
		super(projectTree, selection, "VTL Script", "org.eclipse.ui.ide", "/icons/full/wizban/newfile_wiz.png");
	}
}
