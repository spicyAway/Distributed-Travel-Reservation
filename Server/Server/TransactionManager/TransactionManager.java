
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
              Prepare(ts.getKey());
              break;
            case IN_COMMIT:
              Commit(ts.getKey());
              break;
            case IN_ABORT:
              Abort(ts.getKey());
              break;
            default:
              break;
          }
        }
      }catch(InvalidTransactionException|TransactionAbortedException ive){
        System.out.print("Found invalid or aborted transaction!");
      }
      catch(IOException | ClassNotFoundException e){
        System.out.print("Transaction Manager create new disk file for saving transactions now." + "\n");
        save();
      }
    }
    public boolean Prepare(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
      Transaction currentT = activeTransactions.get(xid);
      System.out.print("Preparing transaction with id:  " + xid + "\n");
      if(currentT.status != Status.ACTIVE && currentT.status != Status.IN_PREPARE){
        throw new InvalidTransactionException(xid, currentT.status.name());
      }
      setStatus(xid, Status.IN_PREPARE);
      ArrayList<IResourceManager> relatedRMs = currentT.rms;
      ExecutorService executorService = Executors.newSingleThreadExecutor();
      boolean result = true;
      for (IResourceManager rm : relatedRMs) {
        try{
          Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                System.out.print("Sending PREPARE-REQ to RM: " + rm.getName() + "\n");
                return rm.Prepare(xid);
              }
          });
          if(!future.get(RESPONSE_TIMEOUT, TimeUnit.SECONDS)){
            System.out.print("Received NO from: " + rm.getName());
            result = false;
            break;
          }
        }catch(Exception e){
          result = false;
          break;
        }
      }
      if(result){
        return Commit(xid);
      }else{
        setStatus(xid, Status.ACTIVE);
      }
      executorService.shutdown();
      return result;
    }

    public boolean Abort(int xid) throws RemoteException, InvalidTransactionException {

        Transaction currentT = activeTransactions.get(xid);
        if(currentT.status != Status.ACTIVE && currentT.status != Status.IN_ABORT){
          throw new InvalidTransactionException(xid, currentT.status.name());
        }
        System.out.print("Start to abort transaction with id: " + xid + "\n");
        setStatus(xid, Status.IN_ABORT);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        boolean result = true;
        ArrayList<IResourceManager> relatedRMs = currentT.rms;
        if(relatedRMs.size() > 0){
          for (IResourceManager rm : relatedRMs) {
            try{
              Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
                  @Override
                  public Boolean call() throws Exception {
                    System.out.print("Sending ABORT-REQ to RM: " + rm.getName() + "\n");
                    return rm.Abort(xid);
                  }
              });
              result &= future.get(RESPONSE_TIMEOUT, TimeUnit.SECONDS);
          }catch(Exception e){
            e.printStackTrace();
            result = false;
            break;
          }
        }
      }
      if(result){
        setStatus(xid, Status.ABORTED);
        synchronized (livingTime) {
            livingTime.remove(xid);
        }
      }else{
        setStatus(xid, Status.IN_ABORT);
      }
      executorService.shutdown();
      return result;
  }
    public boolean Commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {

        Transaction currentT = activeTransactions.get(xid);
        if(currentT.status != Status.IN_PREPARE && currentT.status != Status.IN_COMMIT){
          throw new InvalidTransactionException(xid, currentT.status.name());
        }
        System.out.print("Start to commit transaction with id: " + xid + "\n");
        //Save to disk performed in this method
        setStatus(xid, Status.IN_COMMIT);
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        boolean result = true;

        ArrayList<IResourceManager> relatedRMs = currentT.rms;
        for (IResourceManager rm : relatedRMs) {
          try{
            Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                  System.out.print("Sending COMMIT-REQ to RM: " + rm.getName() + "\n");
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
          synchronized (livingTime) {
              livingTime.remove(xid);
          }
        }else{
          setStatus(xid, Status.IN_COMMIT);
        }
        executorService.shutdown();
        return result;
    }
    public int Start() {
        xid++;
        activeTransactions.put(xid, new Transaction());
        setStatus(xid, Status.ACTIVE);
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
        System.out.print("****Logged transaction " + xid + " with status: " + new_status.name() + "\n");
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
