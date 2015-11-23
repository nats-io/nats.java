package io.nats.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NATSServer implements Runnable
{
	final static String GNATSD = "/Users/larry/Dropbox/workspace/go/bin/gnatsd";
    // Enable this for additional server debugging info.
    boolean debug = false;
    Process p;
    
    class ProcessStartInfo {
    	List<String> arguments = new ArrayList<String>();
    	String stringVal = new String();

    	public ProcessStartInfo(String command) {
    		this.arguments.add(command);
		}

		String[] getArgsAsArray() {
    		return arguments.toArray(new String[arguments.size()]);
    	}
		
    	String getArgsAsString() {
    		for (String s : arguments)
    			stringVal = stringVal.concat(s);
    		return stringVal.trim();
    	}
    	
    	public String toString() {
    		return getArgsAsString();
    	}
    }

    public NATSServer()
    {
        ProcessStartInfo psInfo = createProcessStartInfo();
        try {
			this.p = Runtime.getRuntime().exec(
					psInfo.arguments.toArray(new String[psInfo.arguments.size()])
					);
	        Thread.sleep(500);
		} catch (IOException e) {
			
		} 
          catch (InterruptedException e) {}
    }

    private void addArgument(ProcessStartInfo psInfo, String arg)
    {
    	psInfo.arguments.addAll(Arrays.asList(arg.split("\\s+")));
    }

    public NATSServer(int port)
    {
        ProcessStartInfo psInfo = createProcessStartInfo();

        addArgument(psInfo, "-p " + port);

        try {
			this.p = Runtime.getRuntime().exec(
					psInfo.arguments.toArray(new String[psInfo.arguments.size()])
					);
		} catch (IOException e) {}
    }

    private String buildConfigFileName(String configFile)
    {
    	return configFile;
        // TODO:  There is a better way with TestContext.
//        Assembly assembly = Assembly.GetAssembly(this.GetType());
//        String codebase   = assembly.CodeBase.Replace("file:///", "");
//        return Path.GetDirectoryName(codebase) + "\\..\\..\\config\\" + configFile;
    }

    public NATSServer(String configFile)
    {
        ProcessStartInfo psInfo = this.createProcessStartInfo();
        addArgument(psInfo, " -config " + buildConfigFileName(configFile));
        try {
			p = Runtime.getRuntime().exec(psInfo.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    private ProcessStartInfo createProcessStartInfo()
    {
        ProcessStartInfo psInfo = new ProcessStartInfo(GNATSD);

        if (debug)
        {
            psInfo.arguments.add ("-DV");
        }
        return psInfo;
    }

    public void shutdown()
    {
        if (p == null)
            return;

        p.destroy();

        p = null;
    }

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}
}