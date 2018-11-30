// -------------------------------
// adapted from Kevin T. Manley
// CSE 593
// -------------------------------

package Server.Common;

import Server.Interface.*;
import Server.TransactionManager.*;
import java.util.*;
import java.rmi.RemoteException;
import java.io.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
public class ResourceManager implements IResourceManager
{
	//Main memory
	protected String m_name = "";
	protected RMHashMap m_data;
	protected Boolean mp;
	protected Map<Integer, P_Transaction> pre_images;
	public CrashManager cm;
	public Hashtable<Integer, Long> livingTime;
	public static long TIMEOUT = 100000; //In milliseconds

	//Files wrote into disk
	protected DiskFile<RMHashMap> dataT;
	protected DiskFile<RMHashMap> dataF;
	protected DiskFile<Boolean> master_file;
	protected LogFile<RMHashMap> log_data;
	protected LogFile<Map<Integer, P_Transaction>> log_images;


	public static enum P_Status{
		ACTIVE,
		VOTED_YES,
		VOTED_NO,
		REC_COMMIT,
		REC_ABORT,
		COMMITED,
		ABORTED
	}
	public void resetTime(int xid) {
			synchronized (livingTime) {
					livingTime.put(xid, System.currentTimeMillis());
			}
	}

	public static class P_Transaction implements Serializable{
		public P_Status status;
		public RMHashMap pre_image;
		P_Transaction(){
			this.status = P_Status.ACTIVE;
			this.pre_image = new RMHashMap();
		}
	}

	public ResourceManager(String p_name)
	{
		//Main memory
		m_data = new RMHashMap();
		pre_images = new HashMap<Integer, P_Transaction>();
		m_name = p_name;
		mp = true;
		this.cm = new CrashManager(p_name);
		livingTime = new Hashtable<Integer, Long>();

		//Files
		dataT = new DiskFile(m_name, "dataT");
		dataF = new DiskFile(m_name, "dataF");
		master_file = new DiskFile(m_name, "Master-Record");
		//Logs
		log_data = new LogFile(m_name, "Logged-Data");
		log_images = new LogFile(m_name, "Logged-Images");

		loadData();
		loadMaster();
		try{
			loadFile();
		}catch(RemoteException e){
			System.out.print("Recover RM error.");
		}
	}

	private void loadFile() throws RemoteException{
		System.out.print("Recover old transactions now. " + "\n");
		//crash during during_recovery
		this.cm.during_recovery();
		try{
			for(Entry<Integer, P_Transaction> ts: this.pre_images.entrySet()){
				int current_xid  = ts.getKey();
				P_Transaction current_PT = ts.getValue();
				switch(current_PT.status){
					case ACTIVE:{
						// System.out.print("Abort active transaction: " + current_xid + "\n");
						// this.Abort(current_xid);
						this.resetTime(current_xid);
						break;
					}
					case REC_COMMIT:{
						System.out.print("Continued to commit transaction: " + current_xid + "\n");
						this.Commit(current_xid);
						break;
					}
					case REC_ABORT:{
						System.out.print("Continued to abort transaction: " + current_xid + "\n");
						this.Abort(current_xid);
						break;
					}
					case VOTED_NO:{
						System.out.print("Continued to abort transaction: " + current_xid + "\n");
						this.Abort(current_xid);
						break;
					}
					default:
						break;
				}
			}
		}catch(InvalidTransactionException|TransactionAbortedException ive){
			System.out.print("Found invalid or aborted transaction!");
		}
		// catch(IOException | ClassNotFoundException e){
		// 	System.out.print("Transaction Manager create new disk file for saving transactions now." + "\n");
		// 	this.tm.save();
		// }
	}

	public void resetCrashes() throws RemoteException{
		this.cm.resetCrashes();
		this.cm.save();
		System.out.print("**DEBUG CRASH MODE: " + this.cm.mode + "\n");
	}
	public void crashResourceManager(String name, int mode) throws RemoteException{
		this.cm.mode = mode;
		this.cm.save();
		System.out.print("**DEBUG CRASH MODE: " + this.cm.mode + "\n");
	}
	public void crashMiddleware(int mode) throws RemoteException{
		//dummy;
	}

	public boolean loadData(){
		try{
			this.m_data = (RMHashMap) log_data.read();
			this.pre_images = (Map<Integer, P_Transaction>) log_images.read();
			return true;
		}catch(IOException | ClassNotFoundException e){
			System.out.print("---Create new log file: Data now.---" + "\n");
			return log();
		}
	}
	public boolean loadMaster(){
		try{
			this.mp = (Boolean) master_file.read();
			return true;
		}catch(IOException | ClassNotFoundException e){
			System.out.print("---Create new log file: Master now.---" + "\n");
			return log();
		}
	}

	public boolean logData(){
		try{
			//System.out.print("Logging data: " + log_data.filePath + "\n");
			System.out.print("Logging data." + "\n");
			log_data.save(m_data);
			return true;
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Logging data failed :(");
			return false;
		}
	}
	public boolean logImages(){
		try{
			//System.out.print("Logging pre-images: " + log_images.filePath + "\n");
			System.out.print("Logging pre-images." + "\n");
			log_images.save(pre_images);
			return true;
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Logging images failed :(");
			return false;
		}
	}
	public boolean log(){
		return logData() && logImages();
	}
	public boolean saveData(){
		try{
			DiskFile<RMHashMap> data;
			if(mp){
				data = dataF;
			}else{
				data = dataT;
			}
			System.out.print("Write working data to disk " + data.filePath + "\n");
			data.save(m_data);
			return true;
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Write working data failed :(");
			return false;
		}
	}
	public boolean saveMaster(){
		try{
			System.out.print("Write master pointer to disk " + master_file.filePath + "\n");
			master_file.save(mp);
			return true;
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Write master pointer failed :(");
			return false;
		}
	}
	// Reads a data item
	protected RMItem readData(int xid, String key)
	{
		this.resetTime(xid);
		synchronized(m_data) {
			RMItem item = m_data.get(key);
			if (item != null) {
				return (RMItem)item.clone();
			}
			return null;
		}
	}
	public void logStatus(int xid, P_Status new_status){
		if(this.pre_images.containsKey(xid)){
			P_Transaction pre_image = this.pre_images.get(xid);
			pre_image.status = new_status;
			this.pre_images.put(xid, pre_image);
			log();
			System.out.print("**Logged transaction: " + xid + " with status: " + new_status + "\n");
		}
	}

	public boolean Commit(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{

		System.out.print("Received COMMIT-REQ." + "\n");
		logStatus(xid, P_Status.REC_COMMIT);
		if(!pre_images.containsKey(xid)){
			throw new InvalidTransactionException(xid);
		}
		P_Transaction current = pre_images.get(xid);
		if(current.status == P_Status.COMMITED){
			return true;
		}
		if(current.status == P_Status.ABORTED ||
			current.status == P_Status.VOTED_NO ||
			current.status == P_Status.REC_ABORT){
			throw new InvalidTransactionException(xid);
		}

		//Crash after receiving decision but before committing/aborting
		cm.after_rec_before_operate();

		saveData();
		mp = !mp;
		saveMaster();
		logStatus(xid, P_Status.COMMITED);
		System.out.print("Commited Transaction with id: " + xid + "\n");
		return true;
	}

	public boolean Abort(int xid) throws RemoteException, InvalidTransactionException{

		System.out.print("Received ABORT-REQ." + "\n");
		if(!pre_images.containsKey(xid)){
			System.out.print("Invalid transaction found." +"\n");
			throw new InvalidTransactionException(xid);
		}
		P_Transaction current = pre_images.get(xid);
		if(current.status == P_Status.ABORTED){
			return true;
		}
		if(current.status == P_Status.COMMITED){
			throw new InvalidTransactionException(xid);
		}
		logStatus(xid, P_Status.REC_ABORT);

		//Crash after receiving decision but before committing/aborting
		cm.after_rec_before_operate();
		m_data = pre_images.get(xid).pre_image;
		logStatus(xid, P_Status.ABORTED);
		System.out.print("Aborted Transaction with id: " + xid + "\n");
		return true;
	}

	public boolean Prepare(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{

			//Crash after receive vote request but before sending answer
			System.out.print("Received VOTE-REQ." + "\n");
			this.cm.rec_req_before_send();

			boolean result;
			if(!pre_images.containsKey(xid)){
				result = false;
			}else{
				P_Transaction current = pre_images.get(xid);
				 switch (current.status){
					case VOTED_NO:
					case REC_ABORT:
					case ABORTED:{
						result = false;
						break;
					}
					default:
						result = true;
				}
			}
			//Crash after decide which answer to send (commit/abort)(YES/NO)
			cm.after_decision();

			if(result){
				logStatus(xid,P_Status.VOTED_YES);
			}else{
				logStatus(xid, P_Status.VOTED_NO);
			}
			//Crash after sending answer
			cm.after_sending();
			return result;
	}

	public int Start() throws RemoteException{return -1;};

	// Writes a data item
	protected void writeData(int xid, String key, RMItem value)
	{
		synchronized(m_data) {
			if(!pre_images.containsKey(xid)){
				P_Transaction transaction_data = new P_Transaction();
				resetTime(xid);
				pre_images.put(xid, transaction_data);
			}
			P_Transaction transaction_data = pre_images.get(xid);
			RMHashMap pre_image = transaction_data.pre_image;
			if(!pre_image.containsKey(key)){
				if(!m_data.containsKey(key)){
					pre_image.put(key, null);
				}else{
						RMItem prev = m_data.get(key);
						pre_image.put(key, prev);
				}
				transaction_data.pre_image = pre_image;
				pre_images.put(xid, transaction_data);
				m_data.put(key, value);
			}
		}
		this.resetTime(xid);
		log();
	}

	// Remove the item out of storage
	protected void removeData(int xid, String key)
	{
		synchronized(m_data) {
			if(!pre_images.containsKey(xid)){
				P_Transaction transaction_data = new P_Transaction();
				resetTime(xid);
				pre_images.put(xid, transaction_data);
			}
			P_Transaction transaction_data = pre_images.get(xid);
			RMHashMap pre_image = transaction_data.pre_image;
			if(!pre_image.containsKey(key)){
				if(!m_data.containsKey(key)){
					pre_image.put(key, null);
				}else{
						RMItem prev = m_data.get(key);
						pre_image.put(key, prev);
				}
				transaction_data.pre_image = pre_image;
				pre_images.put(xid, transaction_data);
				m_data.remove(key);
			}
		}
		this.resetTime(xid);
		log();
	}

	// Deletes the encar item
	protected boolean deleteItem(int xid, String key)
	{
		Trace.info("RM::deleteItem(" + xid + ", " + key + ") called");
		ReservableItem curObj = (ReservableItem)readData(xid, key);
		// Check if there is such an item in the storage
		if (curObj == null)
		{
			Trace.warn("RM::deleteItem(" + xid + ", " + key + ") failed--item doesn't exist");
			return false;
		}
		else
		{
			if (curObj.getReserved() == 0)
			{
				removeData(xid, curObj.getKey());
				Trace.info("RM::deleteItem(" + xid + ", " + key + ") item deleted");
				return true;
			}
			else
			{
				Trace.info("RM::deleteItem(" + xid + ", " + key + ") item can't be deleted because some customers have reserved it");
				return false;
			}
		}
	}

	// Query the number of available seats/rooms/cars
	protected int queryNum(int xid, String key)
	{
		Trace.info("RM::queryNum(" + xid + ", " + key + ") called");
		ReservableItem curObj = (ReservableItem)readData(xid, key);
		int value = 0;
		if (curObj != null)
		{
			value = curObj.getCount();
		}
		Trace.info("RM::queryNum(" + xid + ", " + key + ") returns count=" + value);
		return value;
	}

	// Query the price of an item
	protected int queryPrice(int xid, String key)
	{
		Trace.info("RM::queryPrice(" + xid + ", " + key + ") called");
		ReservableItem curObj = (ReservableItem)readData(xid, key);
		int value = 0;
		if (curObj != null)
		{
			value = curObj.getPrice();
		}
		Trace.info("RM::queryPrice(" + xid + ", " + key + ") returns cost=$" + value);
		return value;
	}

	// Reserve an item
	protected boolean reserveItem(int xid, int customerID, String key, String location)
	{
		Trace.info("RM::reserveItem(" + xid + ", customer=" + customerID + ", " + key + ", " + location + ") called" );
		// Read customer object if it exists (and read lock it)
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ")  failed--customer doesn't exist");
			return false;
		}

		// Check if the item is available
		ReservableItem item = (ReservableItem)readData(xid, key);
		if (item == null)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") failed--item doesn't exist");
			return false;
		}
		else if (item.getCount() == 0)
		{
			Trace.warn("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") failed--No more items");
			return false;
		}
		else
		{
			customer.reserve(key, location, item.getPrice());
			writeData(xid, customer.getKey(), customer);

			// Decrease the number of available items in the storage
			item.setCount(item.getCount() - 1);
			item.setReserved(item.getReserved() + 1);
			writeData(xid, item.getKey(), item);

			Trace.info("RM::reserveItem(" + xid + ", " + customerID + ", " + key + ", " + location + ") succeeded");
			return true;
		}
	}

	// Create a new flight, or add seats to existing flight
	// NOTE: if flightPrice <= 0 and the flight already exists, it maintains its current price
	public boolean addFlight(int xid, int flightNum, int flightSeats, int flightPrice) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::addFlight(" + xid + ", " + flightNum + ", " + flightSeats + ", $" + flightPrice + ") called");
		Flight curObj = (Flight)readData(xid, Flight.getKey(flightNum));
		if (curObj == null)
		{
			// Doesn't exist yet, add it
			Flight newObj = new Flight(flightNum, flightSeats, flightPrice);
			writeData(xid, newObj.getKey(), newObj);
			Trace.info("RM::addFlight(" + xid + ") created new flight " + flightNum + ", seats=" + flightSeats + ", price=$" + flightPrice);
		}
		else
		{
			// Add seats to existing flight and update the price if greater than zero
			curObj.setCount(curObj.getCount() + flightSeats);
			if (flightPrice > 0)
			{
				curObj.setPrice(flightPrice);
			}
			writeData(xid, curObj.getKey(), curObj);
			Trace.info("RM::addFlight(" + xid + ") modified existing flight " + flightNum + ", seats=" + curObj.getCount() + ", price=$" + flightPrice);
		}
		return true;
	}

	// Create a new car location or add cars to an existing location
	// NOTE: if price <= 0 and the location already exists, it maintains its current price
	public boolean addCars(int xid, String location, int count, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::addCars(" + xid + ", " + location + ", " + count + ", $" + price + ") called");
		Car curObj = (Car)readData(xid, Car.getKey(location));
		if (curObj == null)
		{
			// Car location doesn't exist yet, add it
			Car newObj = new Car(location, count, price);
			writeData(xid, newObj.getKey(), newObj);
			Trace.info("RM::addCars(" + xid + ") created new location " + location + ", count=" + count + ", price=$" + price);
		}
		else
		{
			// Add count to existing car location and update price if greater than zero
			curObj.setCount(curObj.getCount() + count);
			if (price > 0)
			{
				curObj.setPrice(price);
			}
			writeData(xid, curObj.getKey(), curObj);
			Trace.info("RM::addCars(" + xid + ") modified existing location " + location + ", count=" + curObj.getCount() + ", price=$" + price);
		}
		return true;
	}

	// Create a new room location or add rooms to an existing location
	// NOTE: if price <= 0 and the room location already exists, it maintains its current price
	public boolean addRooms(int xid, String location, int count, int price) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::addRooms(" + xid + ", " + location + ", " + count + ", $" + price + ") called");
		Room curObj = (Room)readData(xid, Room.getKey(location));
		if (curObj == null)
		{
			// Room location doesn't exist yet, add it
			Room newObj = new Room(location, count, price);
			writeData(xid, newObj.getKey(), newObj);
			Trace.info("RM::addRooms(" + xid + ") created new room location " + location + ", count=" + count + ", price=$" + price);
		} else {
			// Add count to existing object and update price if greater than zero
			curObj.setCount(curObj.getCount() + count);
			if (price > 0)
			{
				curObj.setPrice(price);
			}
			writeData(xid, curObj.getKey(), curObj);
			Trace.info("RM::addRooms(" + xid + ") modified existing location " + location + ", count=" + curObj.getCount() + ", price=$" + price);
		}
		return true;
	}

	// Deletes flight
	public boolean deleteFlight(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return deleteItem(xid, Flight.getKey(flightNum));
	}

	// Delete cars at a location
	public boolean deleteCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return deleteItem(xid, Car.getKey(location));
	}

	// Delete rooms at a location
	public boolean deleteRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return deleteItem(xid, Room.getKey(location));
	}

	// Returns the number of empty seats in this flight
	public int queryFlight(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return queryNum(xid, Flight.getKey(flightNum));
	}

	// Returns the number of cars available at a location
	public int queryCars(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return queryNum(xid, Car.getKey(location));
	}

	// Returns the amount of rooms available at a location
	public int queryRooms(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return queryNum(xid, Room.getKey(location));
	}

	// Returns price of a seat in this flight
	public int queryFlightPrice(int xid, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return queryPrice(xid, Flight.getKey(flightNum));
	}

	// Returns price of cars at this location
	public int queryCarsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return queryPrice(xid, Car.getKey(location));
	}

	// Returns room price at this location
	public int queryRoomsPrice(int xid, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return queryPrice(xid, Room.getKey(location));
	}

	public String queryCustomerInfo(int xid, int customerID) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::queryCustomerInfo(" + xid + ", " + customerID + ") called");
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::queryCustomerInfo(" + xid + ", " + customerID + ") failed--customer doesn't exist");
			// NOTE: don't change this--WC counts on this value indicating a customer does not exist...
			return "";
		}
		else
		{
			Trace.info("RM::queryCustomerInfo(" + xid + ", " + customerID + ")");
			System.out.println(customer.getBill());
			return customer.getBill();
		}
	}

	public int newCustomer(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::newCustomer(" + xid + ") called");
		// Generate a globally unique ID for the new customer
		int cid = Integer.parseInt(String.valueOf(xid) +
		String.valueOf(Calendar.getInstance().get(Calendar.MILLISECOND)) +
		String.valueOf(Math.round(Math.random() * 100 + 1)));
		Customer customer = new Customer(cid);
		writeData(xid, customer.getKey(), customer);
		Trace.info("RM::newCustomer(" + cid + ") returns ID=" + cid);
		return cid;
	}

	public boolean newCustomer(int xid, int customerID) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::newCustomer(" + xid + ", " + customerID + ") called");
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			customer = new Customer(customerID);
			writeData(xid, customer.getKey(), customer);
			Trace.info("RM::newCustomer(" + xid + ", " + customerID + ") created a new customer");
			return true;
		}
		else
		{
			Trace.info("INFO: RM::newCustomer(" + xid + ", " + customerID + ") failed--customer already exists");
			return false;
		}
	}

	public boolean deleteCustomer(int xid, int customerID) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") called");
		Customer customer = (Customer)readData(xid, Customer.getKey(customerID));
		if (customer == null)
		{
			Trace.warn("RM::deleteCustomer(" + xid + ", " + customerID + ") failed--customer doesn't exist");
			return false;
		}
		else
		{
			// Increase the reserved numbers of all reservable items which the customer reserved.
 			RMHashTable reservations = customer.getReservations();
			for (String reservedKey : reservations.keySet())
			{
				ReservedItem reserveditem = customer.getReservedItem(reservedKey);
				Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") has reserved " + reserveditem.getKey() + " " +  reserveditem.getCount() +  " times");
				ReservableItem item  = (ReservableItem)readData(xid, reserveditem.getKey());
				Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") has reserved " + reserveditem.getKey() + " which is reserved " +  item.getReserved() +  " times and is still available " + item.getCount() + " times");
				item.setReserved(item.getReserved() - reserveditem.getCount());
				item.setCount(item.getCount() + reserveditem.getCount());
				writeData(xid, item.getKey(), item);
			}

			// Remove the customer from the storage
			removeData(xid, customer.getKey());
			Trace.info("RM::deleteCustomer(" + xid + ", " + customerID + ") succeeded");
			return true;
		}
	}

	// Adds flight reservation to this customer
	public boolean reserveFlight(int xid, int customerID, int flightNum) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return reserveItem(xid, customerID, Flight.getKey(flightNum), String.valueOf(flightNum));
	}

	// Adds car reservation to this customer
	public boolean reserveCar(int xid, int customerID, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return reserveItem(xid, customerID, Car.getKey(location), location);
	}

	// Adds room reservation to this customer
	public boolean reserveRoom(int xid, int customerID, String location) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		return reserveItem(xid, customerID, Room.getKey(location), location);
	}

	// Reserve bundle
	public boolean bundle(int xid, int customerId, Vector<String> flightNumbers, String location, boolean car, boolean room) throws RemoteException, TransactionAbortedException, InvalidTransactionException
	{
		boolean fb = true;
		boolean cb = true;
		boolean rb = true;
		if(car){
             cb = reserveCar(xid, customerId, location);
        }
        if(room) {
             rb = reserveRoom(xid, customerId, location);
        }
		for(int i=0; i<flightNumbers.size(); i++){
		    String flightNum = flightNumbers.get(i);
			fb = fb && reserveFlight(xid, customerId, Integer.parseInt(flightNum));
		}
		return fb && cb && rb;
	}

	public String getName() throws RemoteException
	{
		return m_name;
	}

	public void shutdown() throws RemoteException{
			System.out.print("Bye~");
			System.exit(1);
	}
}
