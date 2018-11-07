package Server.RMI;

import Server.Interface.*;
import Server.Common.*;
import Server.LockManager.*;
import Server.TransactionManager.*;

import java.rmi.NotBoundException;
import java.util.*;

import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class RMIMiddleware extends ResourceManager
{
  private RMHashMap m_itemList;
  private Map<String, IResourceManager> managers;
  public static int managerSize = 3;
  public static String mw_name = "group34_Middleware";
  private static String s_rmiPrefix = "group34_";
  private static String host_ = "localhost";
  private static int port_ = 1099;
  private static String[] types = new String[]{"Flights", "Cars", "Rooms"};
  private LockManager lm;
  private TransactionManager tm;

  //Constructor
  public RMIMiddleware()
  {
    super(mw_name);
    managers = new HashMap<String, IResourceManager>();
    lm = new LockManager();
    tm = new TransactionManager();
  }

  //Connect to the resource managers provided by the user
  public void connectResourceManagers (String[] resourceManagers) throws RemoteException{

    IResourceManager resourceManager_Proxy;

    for(int i=0; i<managerSize; i++){
      try {
        String hostName = resourceManagers[i];
        //Registry registry = LocateRegistry.getRegistry(hostName,port_);
        Registry registry = LocateRegistry.getRegistry(host_,port_);
        System.out.print("Get the registry successfully!----------" + "\n");
        resourceManager_Proxy = (IResourceManager)registry.lookup(s_rmiPrefix + types[i]);
        System.out.print("Get the proxy from " + hostName + " successfully!" + "\n");
        this.managers.put(types[i], resourceManager_Proxy);
      }
      catch (Exception e) {
        System.err.println("Middleware Manager exception: " + e.toString());
        e.printStackTrace();
        System.exit(1);
      }
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
      boolean lock_result = lm.Lock(xid, lockKey, type);
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
        System.out.print("ADDED FLIGHT MANAGER TO TM" + "\n");
        tm.addRM(xid, this.getFlightManager());
        break;
      }
      case "roo":{
        System.out.print("ADDED ROOM MANAGER TO TM" + "\n");
        tm.addRM(xid, this.getRoomManager());
        break;
      }
      case "car":{
        System.out.print("ADDED CAR MANAGER TO TM" + "\n");
        tm.addRM(xid, this.getCarManager());
        break;
      }
      case "cus":{
        System.out.print("ADDED ALL MANAGERS TO TM" + "\n");
        tm.addRM(xid, this.getFlightManager());
        tm.addRM(xid, this.getRoomManager());
        tm.addRM(xid, this.getCarManager());
        break;
      }
    }
  }
  //For all the following methods, request locks first then perform the operation
  public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice)throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid, Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getFlightManager().addFlight(xid, flightNum, flightSeats, flightPrice);
  }

  public boolean addCars(int xid, String location, int count, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid, Car.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getCarManager().addCars(xid, location, count, price);
  }

  public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getRoomManager().addRooms(xid, location, count, price);
  }

  // Deletes flight
  public boolean deleteFlight(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getFlightManager().deleteFlight(xid, flightNum);
  }

  // Delete cars at a location
  public boolean deleteCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getCarManager().deleteCars(xid, location);
  }

  // Delete rooms at a location
  public boolean deleteRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_WRITE);
    return this.getRoomManager().deleteRooms(xid, location);
  }

  // Returns the number of empty seats in this flight
  public int queryFlight(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_READ);
    return this.getFlightManager().queryFlight(xid, flightNum);
  }

  // Returns the number of cars available at a location
  public int queryCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_READ);
    return this.getCarManager().queryCars(xid, location);
  }

  // Returns the amount of rooms available at a location
  public int queryRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_READ);
    return this.getRoomManager().queryRooms(xid, location);
  }

  // Returns price of a seat in this flight
  public int queryFlightPrice(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Flight.getKey(flightNum), TransactionLockObject.LockType.LOCK_READ);
    return this.getFlightManager().queryFlightPrice(xid, flightNum);
  }

  // Returns price of cars at this location
  public int queryCarsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Car.getKey(location), TransactionLockObject.LockType.LOCK_READ);
    return this.getCarManager().queryCarsPrice(xid, location);
  }

  // Returns room price at this location
  public int queryRoomsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
  {
    Lock(xid,Room.getKey(location), TransactionLockObject.LockType.LOCK_READ);
    return this.getRoomManager().queryRoomsPrice(xid, location);
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
  public void Abort(int xid)throws RemoteException, InvalidTransactionException{
    if(!tm.checkAlive(xid)){
      throw new InvalidTransactionException(xid);
    }
    tm.Abort(xid);
    lm.UnlockAll(xid);
  }
  public boolean Commit(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
    if(!tm.checkAlive(xid)){
      throw new InvalidTransactionException(xid);
    }
    return tm.Commit(xid) && lm.UnlockAll(xid);
  }
  public static void main(String args[]) throws RemoteException
  {
    String[] providedManagers = new String[managerSize];
    String server = "localhost";
    if (args.length == managerSize)
    {
      providedManagers[0] = args[0];
      providedManagers[1] = args[1];
      providedManagers[2] = args[2];
    }else{
      System.out.println("Required usage: [firstResourceManager] [secondResourceManager] [thirdResourceManager] ");
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
      mw.connectResourceManagers(providedManagers);
      mw.EchoEM();
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
      if(tm.livingTime.isEmpty()){
        continue;
      }
      for (Integer id : tm.livingTime.keySet()) {
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
