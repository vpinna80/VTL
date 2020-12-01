package it.bancaditalia.oss.vtl.eclipse.parts;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import javax.inject.Singleton;

import org.eclipse.e4.ui.services.EMenuService;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.jface.viewers.LabelProvider;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.TreeNode;
import org.eclipse.jface.viewers.TreeNodeContentProvider;
import org.eclipse.jface.viewers.TreePath;
import org.eclipse.jface.viewers.TreeSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;

import it.bancaditalia.oss.vtl.eclipse.impl.projects.VTLProject;

@Singleton
public class NavigatorPart
{
	public static final String NAVIGATOR_PART_ID = "vtl-eclipse-app.navigator";
	private static final String PROJECT_CONTEXT_MENU_ID = "vtl-eclipse-app.navigator.popupmenu.project";
	
	private final List<VTLProject> projects = new ArrayList<>();

	@Inject 
	private EMenuService menuService;
	
	private TreeViewer viewer;
	
	@PostConstruct
	public final void createComposite(final Composite parent)
	{
		projects.add(new VTLProject("Test1"));
		projects.add(new VTLProject("Test2"));
		
		viewer = new TreeViewer(parent, SWT.NONE);
		viewer.setContentProvider(new TreeNodeContentProvider());
		viewer.setLabelProvider(new LabelProvider() {
			public Image getImage(Object element)
			{
				try
				{
					return ((ImageProvider) element).getImage();
				} 
				catch (MalformedURLException e)
				{
					throw new IllegalStateException(e);
				}
			}
		});

		viewer.setUseHashlookup(true);
		viewer.setInput(projects.toArray(new TreeNode[0]));
		parent.setLayout(new FillLayout());
		viewer.addPostSelectionChangedListener(this::selectionChangedHandler);
	}

	private void selectionChangedHandler(SelectionChangedEvent event)
	{
		ITreeSelection selection = (ITreeSelection) event.getSelection();
		if (selection.getFirstElement() instanceof VTLProject)
			menuService.registerContextMenu(viewer.getControl(), PROJECT_CONTEXT_MENU_ID);
	}

	public void addItemToSelection(TreeNode item)
	{
		TreePath[] paths = ((TreeSelection) viewer.getSelection()).getPaths();
		TreeNode selection = (TreeNode) paths[0].getLastSegment();
		List<TreeNode> children = selection.hasChildren() ? new ArrayList<>(Arrays.asList(selection.getChildren())) : new ArrayList<>();
		children.add(item);
		item.setParent(selection);
		selection.setChildren(children.toArray(new TreeNode[0]));
		viewer.refresh(selection);
	}

	public ITreeSelection getSelection()
	{
		return viewer.getStructuredSelection();
	}

	public List<VTLProject> getProjects()
	{
		return projects;
	}
}
