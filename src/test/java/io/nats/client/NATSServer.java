package io.nats.client;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class NATSServer implements Runnable, AutoCloseable
{
	final static String GNATSD = "/Users/larry/Dropbox/workspace/go/bin/gnatsd";
//	final static String GNATSD = "gnatsd";
    // Enable this for additional server debugging info.
    boolean debug = false;
    ProcessBuilder pb;
    Process p;
    ProcessStartInfo psInfo;

    class ProcessStartInfo {
    	List<String> arguments = new ArrayList<String>();

    	public ProcessStartInfo(String command) {
    		this.arguments.add(command);
		}

        public void addArgument(String arg)
        {
        	this.arguments.addAll(Arrays.asList(arg.split("\\s+")));
        }

		String[] getArgsAsArray() {
    		return arguments.toArray(new String[arguments.size()]);
    	}
		
    	String getArgsAsString() {
    		String stringVal = new String();
    		for (String s : arguments)
    			stringVal = stringVal.concat(s+" ");
    		return stringVal.trim();
    	}
    	
    	public String toString() {
    		return getArgsAsString();
    	}
    }

    public NATSServer()
    {
    	this(-1);
    }


	public NATSServer(int port)
    {
        psInfo = this.createProcessStartInfo();

        if (port > 1023) {
	        psInfo.addArgument("-p " + String.valueOf(port));
        }
//        psInfo.addArgument("-m 8222");

        start();
    }

    private String buildConfigFileName(String configFile)
    {
//    	return new String("src/test/resources/"+configFile);
    	return configFile;
    }

    public NATSServer(String configFile)
    {
        psInfo = this.createProcessStartInfo();
        psInfo.addArgument("-config " + buildConfigFileName(configFile));
        start();
    }

    private ProcessStartInfo createProcessStartInfo()
    {
        psInfo = new ProcessStartInfo(GNATSD);

        if (debug)
        {
            psInfo.addArgument("-DV");
            psInfo.addArgument("-l gnatsd.log");
        }
        return psInfo;
    }

    public void start()
    {
        try {
        	pb = new ProcessBuilder(psInfo.arguments);
        	pb.directory(new File("src/test/resources"));
        	pb.inheritIO();
        	p = pb.start();
	        System.out.println("Started [" + psInfo + "]");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void shutdown()
    {
        if (p == null)
            return;

        p.destroy();
        System.out.println("Stopped [" + psInfo + "]");

        p = null;
    }

	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}


	@Override
	public void close() throws Exception {
		
	}
}