package Server.RMI;

import Server.Interface.*;
import Server.Common.*;
import Server.LockManager.*;
import Server.TransactionManager.*;
import java.util.Map.Entry;

import java.rmi.NotBoundException;
import java.util.*;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.io.IOException;
import java.io.Serializable;

public class RMIMiddleware extends ResourceManager
{
  private RMHashMap m_itemList;
  private Map<String, IResourceManager> managers;
  public static int managerSize = 3;
  public static String mw_name = "group34_Middleware";
  private static String s_rmiPrefix = "group34_";
  private static Map<String, String> addresses;
  private static int port_ = 1099;
  private static String[] types = new String[]{"Flights", "Cars", "Rooms"};
  private LockManager lm;
  private TransactionManager tm;


  //Constructor
  @SuppressWarnings("unchecked")
  public RMIMiddleware()
  {
    super(mw_name);
    this.managers = new HashMap<String, IResourceManager>();
    this.addresses = new HashMap<String, String>();
    this.lm = new LockManager();
    this.tm = new TransactionManager();
    this.tm.ccm = new CoordinatorCrashManager(mw_name);

  }
  //For debugging use
  public void printLocks(){
    for(int i=1; i<this.tm.xid+1; i++){
      System.out.print("**Debug locks: " + this.lm.info(i) + "\n");
    }
  }
  public void loadFile(){
    System.out.print("Recover old transactions now. " + "\n");
    //Crash during recovery
    tm.ccm.during_recovery();
    try{
      @SuppressWarnings("unchecked")
      HashMap<Integer, TransactionManager.Transaction> savedData = (HashMap<Integer, TransactionManager.Transaction>) tm.log_Transactions.read();
      for(Entry<Integer, TransactionManager.Transaction> ts: savedData.entrySet()){

        tm.xid = Math.max(tm.xid, ts.getKey());
        tm.activeTransactions.put(ts.getKey(), ts.getValue());

        switch(ts.getValue().status){
          case ACTIVE:{
            this.Abort(ts.getKey());
            break;
          }
          case IN_PREPARE:{
            this.Prepare(ts.getKey());
            break;
          }
          case IN_COMMIT:{
            this.Recommit(ts.getKey());
            break;
          }
          case IN_ABORT:{
            this.Abort(ts.getKey());
            break;
          }
          default:
            break;
        }
      }
    }catch(InvalidTransactionException|TransactionAbortedException ive){
      System.out.print("Found invalid or aborted transaction!");
    }
    catch(IOException | ClassNotFoundException e){
      System.out.print("Transaction Manager create new disk file for saving transactions now." + "\n");
      this.tm.save();
    }
  }

  public void resetCrashes() throws RemoteException{
    this.tm.ccm.mode = -1;
    System.out.print("*SET CRASH MODE: " + this.tm.ccm.mode + "\n");
    this.getCarManager().resetCrashes();
    this.getRoomManager().resetCrashes();
    this.getFlightManager().resetCrashes();
  }

  public void crashMiddleware(int mode) throws RemoteException{
    this.tm.ccm.mode = mode;
    this.tm.ccm.save();
    System.out.print("*SET CRASH MODE: " + this.tm.ccm.mode);
  }
  public void crashResourceManager(String name, int mode) throws RemoteException{
    this.managers.get(name).crashResourceManager(name, mode);
  }

  //Connect to the resource managers provided by the user
  public void connectResourceManagers (String[] resourceManagers, String[] providedAddresses) throws RemoteException{

    IResourceManager resourceManager_Proxy;

    for(int i=0; i<managerSize; i++){
      try {
        String hostName = resourceManagers[i];
        String address = providedAddresses[i];
        this.addresses.put(hostName, address);
        //Registry registry = LocateRegistry.getRegistry(hostName,port_);
        Registry registry = LocateRegistry.getRegistry(address,port_);
      //  System.out.print("Get the registry successfully!----------" + "\n");
        resourceManager_Proxy = (IResourceManager)registry.lookup(s_rmiPrefix + types[i]);
        System.out.print("Get the proxy from " + hostName + " successfully!" + "\n");
        this.managers.put(types[i], resourceManager_Proxy);
        this.tm.managers.put(types[i], resourceManager_Proxy);
      }
      catch (Exception e) {
        System.err.println("Middleware Manager exception: " + e.toString());
        e.printStackTrace();
        System.exit(1);
      }
    }
  }
  public void reconnect(String hostName) throws RemoteException{
    try {
      IResourceManager resourceManager_Proxy;
      boolean first = true;
      while (true) {
        try {
          String host_ = this.addresses.get(hostName);
          Registry registry = LocateRegistry.getRegistry(host_,port_);
          resourceManager_Proxy = (IResourceManager)registry.lookup(s_rmiPrefix + hostName);
          System.out.print("Reconnect from " + hostName + " successfully!" + "\n");
          this.managers.put(hostName, resourceManager_Proxy);
          break;
        }
        catch (NotBoundException|RemoteException e) {
          if (first) {
            System.out.println("Waiting for '" + hostName + "\n");
            first = false;
          }
        }
        Thread.sleep(500);
      }
    }
    catch (Exception e) {
      System.err.println("Reconnect Error!");
      e.printStackTrace();
      System.exit(1);
    }
  }
  //We laster use a Hearbeat, this method is dummy
  public boolean reconnectOnce(String hostName) throws RemoteException{
    IResourceManager resourceManager_Proxy;
    try {
      String host_ = this.addresses.get(hostName);
      Registry registry = LocateRegistry.getRegistry(host_,port_);
      resourceManager_Proxy = (IResourceManager)registry.lookup(s_rmiPrefix + hostName);
      System.out.print("Reconnect: " + hostName + " successfully!" + "\n");
      //this.managers.put(hostName, resourceManager_Proxy);
      return true;
    }catch(NotBoundException|RemoteException e) {
      return false;
    }
  }
  public boolean Heartbeat(String hostName) throws RemoteException{
    IResourceManager resourceManager_Proxy;
    try {
      String host_ = this.addresses.get(hostName);
      Registry registry = LocateRegistry.getRegistry(host_,port_);
      resourceManager_Proxy = (IResourceManager)registry.lookup(s_rmiPrefix + hostName);
      this.managers.put(hostName, resourceManager_Proxy);
      this.tm.managers.put(hostName, resourceManager_Proxy);
      return true;
    }catch(NotBoundException|RemoteException e) {
      return false;
    }
  }
  //Echo from the active Resource Managers
  public void EchoEM() throws RemoteException{
    try {
      String echoMessage = this.getFlightManager().getName() + " ";
      echoMessage += this.getCarManager().getName() + " ";
      echoMessage += this.getRoomManager().getName() + " ";
      System.out.print("The active resource managers are: " + echoMessage + "\n");
    }catch (Exception e) {
      System.err.println("Echo exception: " + e.toString());
      e.printStackTrace();
      System.exit(1);
    }
  }
  //Get the proxy of Flight Resource Manager
  public IResourceManager getFlightManager(){
    return this.managers.get("Flights");
  }
  //Get the proxy of Car Resource Manager
  public IResourceManager getCarManager(){
    return this.managers.get("Cars");
  }
  //Get the proxy of Room Resource Manager
  public IResourceManager getRoomManager(){
    return this.managers.get("Rooms");
  }

  //To reserve lock on data items given a transaction xid
  public void Lock(int xid, String lockKey, TransactionLockObject.LockType type)throws RemoteException,  TransactionAbortedException, InvalidTransactionException{
    if(!tm.checkAlive(xid)){
      throw new InvalidTransactionException(xid);
    }
    try{
      tm.resetTime(xid);
      boolean lock_result = this.lm.Lock(xid, lockKey, type);
      //this.saveLocks();
      this.lm.saveFile();
      AddManagers(xid, lockKey);
      if (!lock_result) {
        Abort(xid);
        throw new TransactionAbortedException(xid);
      }
    }catch (DeadlockException e) {
      Abort(xid);
      throw new TransactionAbortedException(xid);
    }
  }

  public void AddManagers(int xid, String lockKey){
    String ident = lockKey.substring(0, 3);
    switch(ident){
      case "fli":{
        //System.out.print("ADDED FLIGHT MANAGER TO TM" + "\n");
        tm.addRM(xid, "Flights");
        break;
      }
      case "roo":{
        //System.out.print("ADDED ROOM MANAGER TO TM" + "\n");
        tm.addRM(xid, "Rooms");
        break;
      }
      case "car":{
        //System.out.print("ADDED CAR MANAGER TO TM" + "\n");
        tm.addRM(xid, "Cars");
        break;
      }
      case "cus":{
        //System.out.print("ADDED ALL MANAGERS TO TM" + "\n");
        tm.addRM(xid, "Flights");
        tm.addRM(xid, "Rooms");
        tm.addRM(xid, "Cars");
        break;
      }
    }
  }
  //For all the following methods, request locks first then perform the operation
  public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice)throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {   Lock(xid, Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_WRITE);
      return this.getFlightManager().addFlight(xid, flightNum, flightSeats, flightPrice);
  }

  public boolean addCars(int xid, String location, int count, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid, Car.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
      return this.getCarManager().addCars(xid, location, count, price);
    }catch(RemoteException e){
      this.reconnect("Cars");
    }
    return false;
  }

  public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
      return this.getRoomManager().addRooms(xid, location, count, price);
    }catch(RemoteException e){
      this.reconnect("Rooms");
    }
    return false;
  }

  // Deletes flight
  public boolean deleteFlight(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_WRITE);
      return this.getFlightManager().deleteFlight(xid, flightNum);
    }catch(RemoteException e){
      this.reconnect("Flights");
    }
    return false;
  }

  // Delete cars at a location
  public boolean deleteCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
      return this.getCarManager().deleteCars(xid, location);
    }catch(RemoteException e){
      this.reconnect("Cars");
    }
    return false;
  }

  // Delete rooms at a location
  public boolean deleteRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
      return this.getRoomManager().deleteRooms(xid, location);
    }catch(RemoteException e){
      this.reconnect("Rooms");
    }
    return false;
  }

  // Returns the number of empty seats in this flight
  public int queryFlight(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_READ);
      return this.getFlightManager().queryFlight(xid, flightNum);
    }catch(RemoteException e){
      this.reconnect("Flights");
    }
    return -1;
  }

  // Returns the number of cars available at a location
  public int queryCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_READ);
      return this.getCarManager().queryCars(xid, location);
    }catch(RemoteException e){
      this.reconnect("Cars");
    }
    return -1;
  }

  // Returns the amount of rooms available at a location
  public int queryRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
    {
    try{
      Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_READ);
      return this.getRoomManager().queryRooms(xid, location);
    }catch(RemoteException e){
      this.reconnect("Rooms");
    }
    return -1;
  }

  // Returns price of a seat in this flight
  public int queryFlightPrice(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_READ);
      return this.getFlightManager().queryFlightPrice(xid, flightNum);
    }catch(RemoteException e){
      this.reconnect("Flights");
    }
    return -1;
  }

  // Returns price of cars at this location
  public int queryCarsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_READ);
      return this.getCarManager().queryCarsPrice(xid, location);
    }catch(RemoteException e){
      this.reconnect("Cars");
    }
    return -1;
  }

  // Returns room price at this location
  public int queryRoomsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    try{
      Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_READ);
      return this.getRoomManager().queryRoomsPrice(xid, location);
    }catch(RemoteException e){
      this.reconnect("Rooms");
    }
    return -1;
  }

  public String queryCustomerInfo(int xid, int customerID) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Customer.getKey(customerID), TransactionLockObject.LockType.LOCK_READ);
    return "Flight: " + this.getFlightManager().queryCustomerInfo(xid, customerID)
    + "\n" +"Car: " + this.getCarManager().queryCustomerInfo(xid, customerID)
    + "\n" +"Room: " + this.getRoomManager().queryCustomerInfo(xid, customerID) + "\n";
  }

  public int newCustomer(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    int cid = this.getFlightManager().newCustomer(xid);
    Lock(xid,Customer.getKey(cid), TransactionLockObject.LockType.LOCK_WRITE);
    this.getCarManager().newCustomer(xid, cid);
    this.getRoomManager().newCustomer(xid, cid);
    return cid;
  }

  public boolean newCustomer(int xid, int customerID) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Customer.getKey(customerID), TransactionLockObject.LockType.LOCK_WRITE);
    this.getFlightManager().newCustomer(xid, customerID);
    this.getCarManager().newCustomer(xid, customerID);
    this.getRoomManager().newCustomer(xid, customerID);
    return true;
  }

  public boolean deleteCustomer(int xid, int customerID) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Customer.getKey(customerID), TransactionLockObject.LockType.LOCK_WRITE);
    this.getFlightManager().deleteCustomer(xid, customerID);
    this.getCarManager().deleteCustomer(xid, customerID);
    this.getRoomManager().deleteCustomer(xid, customerID);
    return true;
  }

  // Adds flight reservation to this customer
  public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_WRITE);
    Lock(xid,Customer.getKey(customerID), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getFlightManager().reserveFlight(xid, customerID, flightNum);
  }

  // Adds car reservation to this customer
  public boolean reserveCar(int xid, int customerID, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    Lock(xid,Customer.getKey(customerID), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getCarManager().reserveCar(xid, customerID, location);
  }

  // Adds room reservation to this customer
  public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    Lock(xid,Customer.getKey(customerID), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getRoomManager().reserveRoom(xid, customerID, location);
  }

  // Reserve bundle
  public boolean bundle(int xid, int customerId, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    //Request all the locks first
    Lock(xid,Customer.getKey(customerId), TransactionLockObject.LockType.LOCK_WRITE);
    if(car) {
      Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    }
    if(room) {
      Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    }
    for(int i=0; i<flightNumbers.size(); i++){
      String flightNum = flightNumbers.get(i);
      Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_WRITE);
    }
    //Query if seats/rooms/cars available
    if(check(xid, flightNumbers, location, car, room)){
      //Do the operations
      boolean fb = true;
      boolean cb = true;
      boolean rb = true;
      if(car) {
        cb = this.reserveCar(xid, customerId, location);
      }
      if(room) {
        rb = this.reserveRoom(xid, customerId, location);
      }
      for(int i=0; i<flightNumbers.size(); i++){
        String flightNum = flightNumbers.get(i);
        fb = fb && this.reserveFlight(xid, customerId, Integer.parseInt(flightNum));
      }
      return fb && cb && rb;
    }else{
      return false;
    }
  }
  public boolean check(int xid, Vector<String>flightNumbers, String location, boolean car, boolean room) throws RemoteException, TransactionAbortedException, InvalidTransactionException{
    if(car){
      int carNum = queryCars(xid, location);
      if(carNum == 0) return false;
    }
    if(room){
      int roomNum = queryRooms(xid, location);
      if(roomNum == 0) return false;
    }
    for(int i=0; i<flightNumbers.size(); i++){
      int flightSeats = queryFlight(xid, Integer.parseInt(flightNumbers.get(i)));
      if(flightSeats == 0) return false;
    }
    return true;
  }

  public int Start()throws RemoteException{
    return tm.Start();
  }
  public boolean Abort(int xid)throws RemoteException, InvalidTransactionException{
    boolean result;
    if(!tm.checkAlive(xid)){
      throw new InvalidTransactionException(xid);
    }
    result = tm.Abort(xid);
    if(result){
      lm.UnlockAll(xid);
      this.lm.saveFile();
    }
  //  this.saveLocks();
    return result;
  }
  public boolean Prepare(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
    boolean result;
    boolean noforceAbort = true;
    try{
       result = tm.Prepare(xid);
    }catch(RemoteException e){
        try{
            System.out.print("^^Waiting for recovery and try again." + "\n");
            Thread.sleep(5000);
          }catch(Exception se){
            System.out.print("sleep error when retry.");
            System.exit(1);
          }
        boolean uprunning = reconnectOnce("Flights") && reconnectOnce("Rooms") && reconnectOnce("Cars");
        if(uprunning){
          System.out.print("^^Up running again!" + "\n");
          try{
            Thread.sleep(200);
            result = tm.Prepare(xid);
          }catch(Exception se_2){
            se_2.printStackTrace();
            System.out.print("Whoops error again.");
            result = Abort(xid);
            noforceAbort = false;
          }
        }else{
          System.out.print("^^No response again!" + "\n");
          result = Abort(xid);
          noforceAbort = false;
        }
      }
    //If successfully aborted or commited , release the locks
    if(result){
      lm.UnlockAll(xid);
      this.lm.saveFile();
    }
    return result && noforceAbort;
  }
  public boolean Commit(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
    if(!tm.checkAlive(xid)){
      throw new InvalidTransactionException(xid);
    }
    boolean result = Prepare(xid) && lm.UnlockAll(xid);
    this.lm.saveFile();
    return result;
    //this.saveLocks();
  }
  public boolean Recommit(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
    if(!tm.checkAlive(xid)){
      throw new InvalidTransactionException(xid);
    }
    boolean result = tm.Commit(xid) && lm.UnlockAll(xid);
    this.lm.saveFile();
    return result;
    //this.saveLocks();
  }

  public static void main(String args[]) throws RemoteException
  {
    String[] providedManagers = new String[managerSize];
    String[] providedAddresses = new String[managerSize];
    String server = "localhost";
    if (args.length == managerSize * 2)
    {
      providedManagers[0] = args[0];
      providedAddresses[0] = args[1];

      providedManagers[1] = args[2];
      providedAddresses[1] = args[3];

      providedManagers[2] = args[4];
      providedAddresses[2] = args[5];

    }else{
      System.out.println("Required usage: [firstResourceManager] [address1] [secondResourceManager] [address2] [thirdResourceManager] [address3] ");
      System.exit(1);
    }
    // Create the RMI middleware entry
    try {
      // Create a new middleware object
      RMIMiddleware mw = new RMIMiddleware();

      // Dynamically generate the stub (client proxy)
      IResourceManager mw_Proxy = (IResourceManager) UnicastRemoteObject.exportObject(mw, 0);
      // Bind the remote object's stub in the registry
      Registry l_registry;
      try {
        l_registry = LocateRegistry.createRegistry(1099);
      } catch (RemoteException e) {
        l_registry = LocateRegistry.getRegistry(1099);
      }
      final Registry registry = l_registry;
      registry.rebind(mw_name, mw_Proxy);

      System.out.println("'" + mw_name + "' middleware server ready and bound to '" + mw_name + "'");
      Runtime.getRuntime().addShutdownHook(new Thread() {
        public void run() {
          try {
            registry.unbind(mw_name);
            System.out.println("'" + mw_name + "' resource manager unbound");
          } catch (Exception e) {
            System.err.println((char) 27 + "[31;1mServer exception: " + (char) 27 + "[0mUncaught exception");
            e.printStackTrace();
          }
        }
      });
      mw.connectResourceManagers(providedManagers, providedAddresses);
      mw.EchoEM();
      mw.loadFile();
      //mw.printLocks();
      System.out.print("Middleware Ready!" + "\n");

      //Time-to-live functionality
      Thread timeThread = new Thread(new Runnable(){
        @Override
        public void run() {
          try {
            mw.countingTime();
          }catch (Exception e){
            System.out.print("Time to live error." + "\n");
            e.printStackTrace();
          }
        }
      });
      timeThread.start();

      //Hearbeat functionality
      //Time-to-live functionality
      Thread beatThread = new Thread(new Runnable(){
        @Override
        public void run() {
          try {
            Timer timer = new Timer(true);
            timer.schedule(new TimerTask(){
              public void run(){
                try{
                  mw.Heartbeat("Flights");
                  mw.Heartbeat("Cars");
                  mw.Heartbeat("Rooms");
                }catch (RemoteException e){
                  System.out.print("Lost Connection error." + "\n");
                  }
                }
              },2000,2000);
          }catch (Exception e){
            System.out.print("Heartbeat error." + "\n");
          }
        }
      });
      beatThread.start();



    }catch (Exception e) {
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
  //Check if any transactions should be timed out
  public void countingTime()throws RemoteException, TransactionAbortedException, InvalidTransactionException {
    while (true) {
      synchronized(tm.livingTime){
        if(tm.livingTime.isEmpty()){
          continue;
        }
        for (Integer id : tm.livingTime.keySet()) {
          TransactionManager.Transaction currentT = tm.activeTransactions.get(id);
          if(currentT.status == TransactionManager.Status.COMMITTED || currentT.status == TransactionManager.Status.ABORTED){
            continue;
          }
          if (System.currentTimeMillis() > tm.livingTime.get(id) + tm.TIMEOUT) {
            Abort(id);
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
  public void shutdown() throws RemoteException{
    try{
      this.getFlightManager().shutdown();
    }catch(Exception e) {
      System.out.print("Resource Manager - Flight shutted down." + "\n");
    }
    try{
      this.getRoomManager().shutdown();
    }catch(Exception e) {
      System.out.print("Resource Manager - Room shutted down." + "\n");
    }
    try{
        this.getCarManager().shutdown();
    }catch(Exception e) {
      System.out.print("Resource Manager - Car shutted down." + "\n");
    }
    System.out.print("All Resource Managers shutted down." + "\n");
    System.exit(1);
  }



}
