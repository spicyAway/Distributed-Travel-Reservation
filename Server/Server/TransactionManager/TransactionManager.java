
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
    public static int xid;
    public static HashMap<Integer, Transaction> activeTransactions;
    public static Hashtable<Integer, Long> livingTime;
    public static LogFile<HashMap<Integer, Transaction>> log_Transactions = new LogFile(RMIMiddleware.mw_name, "Logged-Transactions");
    public CoordinatorCrashManager ccm;
    private boolean tryflag = false;
    public HashMap<String, IResourceManager> managers;

    public static enum Status{
      ACTIVE,
      IN_PREPARE,
      IN_COMMIT,
      IN_ABORT,
      COMMITED,
      ABORTED,
      TIMED_OUT
    }

    public static class Transaction implements Serializable {
      public Status status;
      public ArrayList<String> rms;

      Transaction(){
        this.rms = new ArrayList<String>();
      }
    }

    public TransactionManager(){
        xid = 0;
        activeTransactions = new HashMap<Integer, Transaction>();
        livingTime = new Hashtable<Integer, Long>();
        ccm = new CoordinatorCrashManager();
        managers = new HashMap<String, IResourceManager>();
        log_Transactions = new LogFile(RMIMiddleware.mw_name, "Logged-Transactions");
        //loadFile();
    }
    public void save(){
      try{
        log_Transactions.save(activeTransactions);
      }catch(IOException e){
        e.printStackTrace();
        System.out.print("Transaction Manager save file failed." + "\n");
      }
    }
    public boolean Prepare(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
      Transaction currentT = activeTransactions.get(xid);
      System.out.print("Preparing transaction with id:  " + xid + "\n");
      //Crash before sending vote request
      ccm.before_vote();

      if(currentT.status != Status.ACTIVE && currentT.status != Status.IN_PREPARE){
        throw new InvalidTransactionException(xid, currentT.status.name());
      }
      setStatus(xid, Status.IN_PREPARE);
      ArrayList<String> relatedRMs = currentT.rms;
      //ExecutorService executorService = Executors.newSingleThreadExecutor();
      boolean result = true;
      for (String rm_name : relatedRMs) {
          IResourceManager rm = managers.get(rm_name);
          if(!rm.Prepare(xid)){
            System.out.print("Received NO from: " + rm.getName());
            result = false;
            break;
          }
          //Crash after sending vote request and before receiving any replies
          ccm.after_vote_before_rec();
          //Crash after receiving some replies but not all
          ccm.rec_some_rep();
          //Crash after receiving all replies but before deciding
          ccm.rec_all_before_dec();

      }
      if(result){
        return Commit(xid);
      }else{
        return Abort(xid);
      }
      //executorService.shutdown();
    }

    public boolean Abort(int xid) throws RemoteException, InvalidTransactionException {

        Transaction currentT = activeTransactions.get(xid);
        if(currentT.status != Status.ACTIVE && currentT.status != Status.IN_ABORT && currentT.status != Status.IN_PREPARE){
          throw new InvalidTransactionException(xid, currentT.status.name());
        }
        System.out.print("Start to abort transaction with id: " + xid + "\n");
        setStatus(xid, Status.IN_ABORT);
        //Crash after deciding but before sending decision
        ccm.after_dec_before_send();

        //ExecutorService executorService = Executors.newSingleThreadExecutor();
        boolean result = true;
        ArrayList<String> relatedRMs = currentT.rms;
        if(relatedRMs.size() > 0){
          for (String rm_name : relatedRMs) {
            try{
              //Crash after sending some but not all decisions
              IResourceManager rm = managers.get(rm_name);
              System.out.print("Sending ABORT-REQ to RM: " + rm.getName() + "\n");
              ccm.send_some_dec();
              //Crash after having sent all decisions
              ccm.after_send_all_dec();
              //result &= future.get(RESPONSE_TIMEOUT, TimeUnit.SECONDS);
              result &= rm.Abort(xid);
          }catch(Exception e){
            System.out.print("Possible crashes! Please wait." + "\n");
            resetTime(xid);
            result = true;
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
      //executorService.shutdown();
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
        //Crash after deciding but before sending decision
        ccm.after_dec_before_send();

        //ExecutorService executorService = Executors.newSingleThreadExecutor();
        boolean result = true;

        ArrayList<String> relatedRMs = currentT.rms;
        for (String rm_name : relatedRMs) {
          try{
            IResourceManager rm = managers.get(rm_name);
            System.out.print("Sending COMMIT-REQ to RM: " + rm.getName() + "\n");
            //Crash after sending some but not all decisions
            ccm.send_some_dec();
            //Crash after having sent all decisions
            ccm.after_send_all_dec();
            result &= rm.Commit(xid);

          }catch(Exception e){
            result = false;
            break;
          }
        }
        // if(result){
          setStatus(xid, Status.COMMITED);
          synchronized (livingTime) {
              livingTime.remove(xid);
          }
        //   }
        // }else{
        //   setStatus(xid, Status.IN_COMMIT);
        // }
        //executorService.shutdown();
        return true;
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
    public void addRM(int xid, String rm) {
        Transaction currentT = activeTransactions.get(xid);
        ArrayList<String> relatedRMs = currentT.rms;
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
