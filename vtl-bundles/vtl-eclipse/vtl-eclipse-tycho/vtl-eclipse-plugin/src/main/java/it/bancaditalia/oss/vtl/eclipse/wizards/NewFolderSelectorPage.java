package it.bancaditalia.oss.vtl.eclipse.wizards;

import java.util.List;

import org.eclipse.jface.viewers.ITreeSelection;

import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLProject;

public class NewFolderSelectorPage extends NewResourceSelectorPage
{
	public NewFolderSelectorPage(List<VTLProject> projectTree, ITreeSelection selection)
	{
		super(projectTree, selection, "Folder", "org.eclipse.ui.ide", "/icons/full/wizban/newfolder_wiz.png");
	}
}
