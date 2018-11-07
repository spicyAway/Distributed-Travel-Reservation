package Client;

import Server.Interface.*;
import Server.TransactionManager.*;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.rmi.NotBoundException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.*;
import java.io.*;

public class PerfClient extends Client
{
	IResourceManager m_resourceManager = null;
	private static String s_serverHost = "localhost";
	private static int s_serverPort = 1099;
	private static String s_serverName = "Server";
	private static int load=2;
	private static int loop=100;
	private static int testType=0;
	private static int clientCount=1;
	//TODO: REPLACE 'ALEX' WITH YOUR GROUP NUMBER TO COMPILE
	private static String s_rmiPrefix = "group34_";
	private static RandomString rgen = new RandomString(8, ThreadLocalRandom.current());
	public static void main(String args[])
	{	
		if (args.length > 0)
		{
			s_serverHost = args[0];
		}
		if (args.length > 1)
		{
			s_serverName = args[1];
		}
		
		if (args.length > 2)
		{
			testType=Integer.parseInt(args[2]);
		}
		if (args.length > 3)
		{
			clientCount=Integer.parseInt(args[3]);
		}
		if (args.length > 4)
		{
			load=Integer.parseInt(args[4]);
		}
		if (args.length > 5)
		{
			loop=Integer.parseInt(args[5]);
		}
		// Set the security policy
		if (System.getSecurityManager() == null)
		{
			System.setSecurityManager(new SecurityManager());
		}
		if(clientCount==1){
			// Get a reference to the RMIRegister
			try {
				PerfClient client = new PerfClient();
				client.connectServer();
				client.test(testType,loop,-1);
			} 
			catch (Exception e) {    
				System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0mUncaught exception");
				e.printStackTrace();
				System.exit(1);
			}
		}
		else{
			try {
				
				for(int i=0;i<clientCount;i++){
					final int c=i;
					Thread clientThread = new Thread(){
						public void run(){
							PerfClient client = new PerfClient();
							client.connectServer(s_serverHost, s_serverPort, s_serverName);
							client.test(testType,loop,load);
						}
					};
					clientThread.start();
				}
			} 
			catch (Exception e) {    
				System.err.println((char)27 + "[31;1mClient exception: " + (char)27 + "[0mUncaught exception");
				e.printStackTrace();
				System.exit(1);
			}
		}
	}

	
	public void test(int testType,int loop,int load)
	{
		int deadlock=0;
        try {
            switch (testType) {
                case 0: {
                	//single client single rm
                	int rmType = ThreadLocalRandom.current().nextInt(0, 3);
                	long totalTime=0;
                	for(int i=0;i<loop;i++){
                		long tSendTime = System.currentTimeMillis();
                		int xid = m_resourceManager.Start();
                		transAdd(rmType,xid);
                        m_resourceManager.Commit(xid);
                        long tEndTime = System.currentTimeMillis();
                        long spentTime=tEndTime-tSendTime;
                        totalTime+=spentTime;
                	}
                	long avgTime=totalTime/loop;
                	System.out.println("The average response time for one client and one RM is "+avgTime);
                	break;
                }
                case 1: {
                	//single client multiple rm
                	long totalTime=0;
                	for(int i=0;i<loop;i++){
                		long tSendTime = System.currentTimeMillis();
                		int xid = m_resourceManager.Start();
                		transMultiAdd(xid);
                        m_resourceManager.Commit(xid);
                        long tEndTime = System.currentTimeMillis();
                        long spentTime=tEndTime-tSendTime;
                        totalTime+=spentTime;
                	}
                	long avgTime=totalTime/loop;
                	System.out.println("The average response time for one client and multiple RM is "+avgTime);
                	break;
                }
                case 2: {
                	//multiple client single rm
                	long interval = 1000/(load/clientCount);
                	int rmType = ThreadLocalRandom.current().nextInt(0, 3);
                	long totalTime=0;
                	for(int i=0;i<loop;i++){
                		
                		long tSendTime = System.currentTimeMillis();
                		int xid = m_resourceManager.Start();
                		transAdd(rmType,xid);
                        m_resourceManager.Commit(xid);
                        long tEndTime = System.currentTimeMillis();
                        long spentTime=tEndTime-tSendTime;
                        int variation = ThreadLocalRandom.current().nextInt(-10, 10);
                        long sleepTime = interval+variation-spentTime;
                        if(sleepTime>0)
                        	Thread.sleep(sleepTime);
                        totalTime+=spentTime;
                	}
                	long avgTime=totalTime/loop;
                	System.out.println("The average response time for multiple client and one RM is "+avgTime);
                	break;
                }
                case 3: {
                	//multiple client single rm
                	int interval = 1000/(load/clientCount);
                	long totalTime=0;
                	for(int i=0;i<loop;i++){
                		
                		long tSendTime = System.currentTimeMillis();
                		int xid = m_resourceManager.Start();
                		transMultiAdd(xid);
                        m_resourceManager.Commit(xid);
                        long tEndTime = System.currentTimeMillis();
                        long spentTime=tEndTime-tSendTime;
                        int variation = ThreadLocalRandom.current().nextInt(-10, 10);
                        long sleepTime = interval+variation-spentTime;
                        if(sleepTime>0)
                        	Thread.sleep(sleepTime);
                        totalTime+=spentTime;
                	}
                	long avgTime=totalTime/loop;
                	System.out.println("The average response time for multiple client and multiple RM is "+avgTime);
                	break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            if (e.getMessage().contains("deadlocked:")) {
                deadLocks++;
            }
        }
        System.out.println("deadlock counter:"+deadLocks);
	}
	public void transAdd(int rmType,int xid){
		for(int i=0;i<10;i++){
			addRandomResources(rmType,xid);
		}
	}
	public void transMultiAdd(int xid){
		for(int i=0;i<10;i++){
			int rmType = ThreadLocalRandom.current().nextInt(0, 3);
			addRandomResources(rmType,xid);
		}
	}
	
	
    public void addRandomResources(int rmType,int xid) {
        try {
            switch (rmType) {
                case 0: {

                    int flightSeats = ThreadLocalRandom.current().nextInt(1, 100);
                    int flightPrice = ThreadLocalRandom.current().nextInt(100, 1000);
                    int flightNum=ThreadLocalRandom.current().nextInt(1, 100);
                    m_resourceManager.addFlight(xid, flightNum, flightSeats, flightPrice);

                    break;
                }
                case 1: {

                    int count = ThreadLocalRandom.current().nextInt(1, 100);
                    int price = ThreadLocalRandom.current().nextInt(100, 1000);
                    String location = rgen.nextString();
                    m_resourceManager.addRooms(xid, location, count, price);

                    break;
                }
                case 2: {

                    int count = ThreadLocalRandom.current().nextInt(1, 100);
                    int price = ThreadLocalRandom.current().nextInt(100, 1000);
                    String location = rgen.nextString();
                    m_resourceManager.addCars(xid, location, count, price);

                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
	public PerfClient()
	{
		super();
	}

	public void connectServer()
	{
		connectServer(s_serverHost, s_serverPort, s_serverName);
	}

	public void connectServer(String server, int port, String name)
	{
		try {
			boolean first = true;
			while (true) {
				try {
					Registry registry = LocateRegistry.getRegistry(server, port);
					m_resourceManager = (IResourceManager)registry.lookup(s_rmiPrefix + name);
					System.out.println("Connected to '" + name + "' server [" + server + ":" + port + "/" + s_rmiPrefix + name + "]");
					break;
				}
				catch (NotBoundException|RemoteException e) {
					if (first) {
						System.out.println("Waiting for '" + name + "' server [" + server + ":" + port + "/" + s_rmiPrefix + name + "]");
						first = false;
					}
				}
				Thread.sleep(500);
			}
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}
	}
}

