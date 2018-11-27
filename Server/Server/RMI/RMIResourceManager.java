// -------------------------------
// adapted from Kevin T. Manley
// CSE 593
// -------------------------------

package Server.RMI;

import Server.Interface.*;
import Server.Common.*;
import Server.TransactionManager.*;

import java.rmi.NotBoundException;
import java.util.*;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RMIResourceManager extends ResourceManager
{
	private static String s_serverName = "Server";
	//TODO: REPLACE 'ALEX' WITH YOUR GROUP NUMBER TO COMPILE
	private static String s_rmiPrefix = "group34_";

	public static void main(String args[])
	{
		if (args.length > 0)
		{
			s_serverName = args[0];
		}

		// Create the RMI server entry
		try {
			// Create a new Server object
			RMIResourceManager server = new RMIResourceManager(s_serverName);

			// Dynamically generate the stub (client proxy)
			IResourceManager resourceManager = (IResourceManager)UnicastRemoteObject.exportObject(server, 0);

			// Bind the remote object's stub in the registry
			Registry l_registry;
			try {
				l_registry = LocateRegistry.createRegistry(1099);
			} catch (RemoteException e) {
				l_registry = LocateRegistry.getRegistry(1099);
			}
			final Registry registry = l_registry;
			registry.rebind(s_rmiPrefix + s_serverName, resourceManager);

			Runtime.getRuntime().addShutdownHook(new Thread() {
				public void run() {
					try {
						registry.unbind(s_rmiPrefix + s_serverName);
						System.out.println("'" + s_serverName + "' resource manager unbound");
					}
					catch(Exception e) {
						System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
						e.printStackTrace();
					}
				}
			});
			System.out.println("'" + s_serverName + "' resource manager server ready and bound to '" + s_rmiPrefix + s_serverName + "'");
			//Time-to-live functionality
			Thread timeThread = new Thread(new Runnable(){
				@Override
				public void run() {
					try {
						server.countingTime();
					}catch (Exception e){
						System.out.print("Time to live error." + "\n");
						e.printStackTrace();
					}
				}
			});
			timeThread.start();
		}
		catch (Exception e) {
			System.err.println((char)27 + "[31;1mServer exception: " + (char)27 + "[0mUncaught exception");
			e.printStackTrace();
			System.exit(1);
		}

		// Create and install a security manager
		if (System.getSecurityManager() == null)
		{
			System.setSecurityManager(new SecurityManager());
		}
	}

	public RMIResourceManager(String name)
	{
		super(name);
	}

	public void countingTime()throws RemoteException, TransactionAbortedException, InvalidTransactionException {
			while (true) {
				synchronized(this.livingTime){
					if(this.livingTime.isEmpty()){
						continue;
					}
					for (Integer id : this.livingTime.keySet()) {
						if(!this.pre_images.containsKey(id)){
							continue;
						}
						P_Transaction current = this.pre_images.get(id);
						if(current.status == P_Status.COMMITED || current.status == P_Status.ABORTED){
							continue;
						}
						if (System.currentTimeMillis() > this.livingTime.get(id) + this.TIMEOUT) {
							this.Abort(id);
							System.out.print("Time-out Transaction " + id + "\n");
						}
					}
				try {
					Thread.sleep(100);
				}catch(Exception e){
					System.out.print("Sleep error.");
				}
			}
		}
	}
}
