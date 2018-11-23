
package Server.TransactionManager;
import Server.Interface.*;
import Server.Common.*;
import Server.RMI.*;
import Server.LockManager.*;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.lang.*;
import java.util.Map.Entry;
import java.util.Vector;
import java.util.*;
import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.*;

@SuppressWarnings("unchecked")
public class TransactionManager {
    public static int RESPONSE_TIMEOUT = 10; //In seconds
    public static long TIMEOUT = 100000; //In milliseconds
    private static int xid;
    private static HashMap<Integer, Transaction> activeTransactions;
    public static Hashtable<Integer, Long> livingTime;
    private static DiskFile<HashMap<Integer, Transaction>> savedTransactions;

    public static enum Status{
      ACTIVE,
      IN_PREPARE,
      IN_COMMIT,
      COMMITED,
      ABORTED,
      IN_ABORT,
      TIMED_OUT
    }

    private static class Transaction implements Serializable {

      private Status status;
      private ArrayList<IResourceManager> rms;

      Transaction(){
        this.status = Status.ACTIVE;
        this.rms = new ArrayList<IResourceManager>();
      }
    }

    public TransactionManager(){
        xid = 0;
        activeTransactions = new HashMap<Integer, Transaction>();
        livingTime = new Hashtable<Integer, Long>();
        savedTransactions = new DiskFile(RMIMiddleware.mw_name, "savedTransactions");
        loadFile();
    }
    public void save(){
      try{
        savedTransactions.save(activeTransactions);
      }catch(IOException e){
        e.printStackTrace();
        System.out.print("Transaction Manager save file failed." + "\n");
      }
    }
    public void loadFile(){
      try{
        HashMap<Integer, Transaction> savedData = savedTransactions.read();
        for(Entry<Integer, Transaction> ts: savedData.entrySet()){
          xid = Math.max(xid, ts.getKey());
          activeTransactions.put(ts.getKey(), ts.getValue());
          switch(ts.getValue().status){
            case ACTIVE:
              resetTime(ts.getKey());
              break;
            case IN_PREPARE:
            case IN_COMMIT:
              Prepare(ts.getKey());
              break;
            case IN_ABORT:
              Abort(ts.getKey());
              break;
            default:
              break;
          }
        }
      }catch(InvalidTransactionException ive){
        System.out.print("Found invalid transaction!");
      }
      catch(IOException | ClassNotFoundException e){
        System.out.print("Transaction Manager create new disk file for saving transactions now." + "\n");
        save();
      }
    }
    public boolean Prepare(int xid)throws RemoteException, InvalidTransactionException{
      return true;
    }

    public static boolean Abort(int xid) throws RemoteException, InvalidTransactionException {
        ArrayList<IResourceManager> relatedRMs = activeTransactions.get(xid).rms;
        if(relatedRMs != null) {
            for (IResourceManager rm : relatedRMs) {
                rm.Abort(xid);
            }
        }
        activeTransactions.remove(xid);
        synchronized (livingTime) {
            livingTime.remove(xid);
        }
        return true;
    }
    public boolean Commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        Transaction currentT = activeTransactions.get(xid);
        // if(currentT.status != Status.IN_PREPARE || currentT.status != Status.IN_COMMIT){
        //   throw new InvalidTransactionException(xid, currentT.status.name());
        // }
        System.out.print("Start to commit now. " + "\n");
        //Save to disk performed in this method
        setStatus(xid, Status.IN_COMMIT);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        boolean result = true;
        ArrayList<IResourceManager> relatedRMs = activeTransactions.get(xid).rms;
        for (IResourceManager rm : relatedRMs) {
          try{
            Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                  System.out.print("Sending VOTE-REQ to RM: " + rm.getName());
                  return rm.Commit(xid);
                }
            });
            result &= future.get(RESPONSE_TIMEOUT, TimeUnit.SECONDS);
          }catch(Exception e){
            result = false;
            break;
          }
        }
        if(result){
          setStatus(xid, Status.COMMITED);
        }else{
          setStatus(xid, Status.IN_COMMIT);
        }
        // activeTransactions.remove(xid);
        // synchronized (livingTime) {
        //     livingTime.remove(xid);
        // }
        return result;
    }
    public int Start() {
        xid++;
        activeTransactions.put(xid, new Transaction());
        synchronized (livingTime) {
            livingTime.put(xid, System.currentTimeMillis());
        }
        return xid;
    }
    public Status getStatus(int xid){
      if(checkAlive(xid)){
        return activeTransactions.get(xid).status;
      }
      return null;
    }
    private void setStatus(int xid, Status new_status){
      if(checkAlive(xid)){
        activeTransactions.get(xid).status = new_status;
        save();
      }
    }
    public void addRM(int xid, IResourceManager rm) {
        Transaction currentT = activeTransactions.get(xid);
        ArrayList<IResourceManager> relatedRMs = currentT.rms;
        if(!relatedRMs.contains(rm)){
          relatedRMs.add(rm);
          currentT.rms = relatedRMs;
        }
    }
    public int getXid() {
        return xid;
    }

    public boolean checkAlive(int id) {
        return activeTransactions.containsKey(id);
    }
    //
    // public void setXid(int id) {
    //     xid = id;
    //     ArrayList<IResourceManager> relatedRMs = new ArrayList<IResourceManager>();
    //     activeTransactions.get(id).rms = relatedRMs;
    // }
    public void resetTime(int xid) {
        synchronized (livingTime) {
            livingTime.put(xid, System.currentTimeMillis());
        }
    }
}
