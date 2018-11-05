
package Server.TransactionManager;
import Server.Interface.*;
import Server.Common.*;
import Server.RMI.*;
import Server.LockManager.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.lang.*;

import java.util.Vector;
import java.util.*;


public class TransactionManager {
    public static long TIMEOUT = 100000;
    private static int xid;
    private static HashMap<Integer, ArrayList<IResourceManager>> activeTransactions;
    private static HashMap<Integer, String> transaction_results;
    public static Hashtable<Integer, Long> livingTime;

    public TransactionManager() {
        xid = 0;
        activeTransactions = new HashMap<Integer, ArrayList<IResourceManager>>();
        transaction_results = new HashMap<Integer, String>();
        livingTime = new Hashtable<Integer, Long>();
    }

    public String getResults(int xid) {
        return transaction_results.get(xid);
    }

    public void setResults(int xid, String msg) {
        transaction_results.put(xid, msg);
    }

    public void addResults(int xid, String msg) {
        if (!transaction_results.containsKey(xid)) {
            setResults(xid, msg);
        } else {
            String result = transaction_results.get(xid);
            result += msg + "\n";
            transaction_results.put(xid, result);
        }
    }

    public int getXid() {
        return xid;
    }

    public boolean checkAlive(int id) {
        return activeTransactions.containsKey(id);
    }

    public void setXid(int id) {
        xid = id;
        ArrayList<IResourceManager> relatedRMs = new ArrayList<IResourceManager>();
        activeTransactions.put(id, relatedRMs);
    }

    public void addRM(int xid, IResourceManager rm) {
        ArrayList<IResourceManager> relatedRMs = activeTransactions.get(xid);
        relatedRMs.add(rm);
        activeTransactions.put(xid, relatedRMs);
    }

    public static void Abort(int xid) throws RemoteException, InvalidTransactionException {
        ArrayList<IResourceManager> relatedRMs = activeTransactions.get(xid);
        if(relatedRMs != null) {
            for (IResourceManager rm : relatedRMs) {
                rm.Abort(xid);
            }
        }
        activeTransactions.remove(xid);
        synchronized (livingTime) {
            livingTime.remove(xid);
        }
    }

    public boolean Commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        ArrayList<IResourceManager> relatedRMs = activeTransactions.get(xid);
        for (IResourceManager rm : relatedRMs) {
            rm.Commit(xid);
        }
        activeTransactions.remove(xid);
        synchronized (livingTime) {
            livingTime.remove(xid);
        }
        return true;
    }

    public void resetTime(int xid) {
        synchronized (livingTime) {
            livingTime.put(xid, System.currentTimeMillis());
        }
    }

    public int Start() {
        int xid = getXid();
        setXid(xid + 1);
        int current_id = xid + 1;
        synchronized (livingTime) {
            livingTime.put(current_id, System.currentTimeMillis());
        }
        return current_id;
    }

    public static void clean_up(int xid) throws RemoteException, InvalidTransactionException {
        Abort(xid);
    }
}