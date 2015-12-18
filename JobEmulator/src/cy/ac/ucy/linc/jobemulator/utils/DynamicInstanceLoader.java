package cy.ac.ucy.linc.jobemulator.utils;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

import cy.ac.ucy.linc.jobemulator.job.IJob;

/**
 * From JCatascopia Monitoring System
 * 
 * https://github.com/dtrihinas/JCatascopia 
 * 
 **/
public class DynamicInstanceLoader {
		
	public static IJob newInstance (String path, String myclass) throws Exception {
		
		URLClassLoader tmp = new URLClassLoader( new URL[] { getClassPath() }) { 
			
			@Override
			public synchronized Class<?> loadClass(String name) throws ClassNotFoundException {
				return super.loadClass(name);
			}
		};

		try {
			
			path += (!path.endsWith(".")) ? "." : "";
			
			return (IJob) tmp.loadClass(path + myclass).newInstance();
			
		} 
		catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.out.println("job type (" + myclass + ") in path (" + path + ")does not exist");
		}
		catch (Exception e) {
			e.printStackTrace(); 
		}
		return null;
	}

	private static URL getClassPath() {
		
		String resName = DynamicInstanceLoader.class.getName().replace('.', '/') + ".class";
		String loc = DynamicInstanceLoader.class.getClassLoader().getResource(resName).toExternalForm();    
		URL cp;
		try {
			cp = new URL(loc.substring(0, loc.length() - resName.length()));
		} 
		catch (MalformedURLException e) {
			throw new RuntimeException(e);
		}
		return cp;
	}
}