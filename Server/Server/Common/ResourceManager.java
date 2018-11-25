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

@SuppressWarnings("unchecked")
public class ResourceManager implements IResourceManager
{
	protected String m_name = "";
	protected RMHashMap m_data = new RMHashMap();
	protected Map<Integer, RMHashMap> pre_images = new HashMap<Integer, RMHashMap>();
	protected DiskFile<RMHashMap> dataFile;
	protected DiskFile<Map<Integer, RMHashMap>> imagesFile;
	public CrashManager cm;

	public ResourceManager(String p_name)
	{
		m_name = p_name;
		dataFile = new DiskFile(m_name, "data");
		imagesFile = new DiskFile(m_name, "pre-images");
		cm = new CrashManager();
		loadFile();
	}
	public void resetCrashes() throws RemoteException{
		this.cm.resetCrashes();
		System.out.print("****DEBUG CRASH MODE: " + this.cm.mode + "\n");
	}
	public void crashResourceManager(String name, int mode) throws RemoteException{
		this.cm.mode = mode;
		System.out.print("****DEBUG CRASH MODE: " + this.cm.mode + "\n");
	}
	public void crashMiddleware(int mode) throws RemoteException{
		//dummy;
	}

	public boolean loadFile(){
		try{
			this.m_data = dataFile.read();
			this.pre_images = imagesFile.read();
			return true;
		}catch(IOException | ClassNotFoundException e){
			System.out.print("------Create new disk file: Data and Images now.-------" + "\n");
			return saveData() && saveImages();
		}
	}
	public boolean saveData(){
		try{
			System.out.print("Writing data to disk file: " + dataFile.filePath + "\n");
			dataFile.save(m_data);
			return true;
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Saving data failed :(");
			return false;
		}
	}

	public boolean saveImages(){
		try{
			System.out.print("Writing pre-images to disk file: " + imagesFile.filePath + "\n");
			imagesFile.save(pre_images);
			return true;
		}catch(IOException e){
			e.printStackTrace();
			System.out.print("Saving images failed :(");
			return false;
		}
	}
	// Reads a data item
	protected RMItem readData(int xid, String key)
	{
		synchronized(m_data) {
			RMItem item = m_data.get(key);
			if (item != null) {
				return (RMItem)item.clone();
			}
			return null;
		}
	}
	public boolean Commit(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
		System.out.print("Received COMMIT-REQ." + "\n");
		//!!!!!!!!!!!!!!!!Crash after receiving decision but before committing/aborting
		cm.after_rec_before_operate();
		if(pre_images.containsKey(xid)){
			pre_images.remove(xid);
			Save();
		}
		//dummy
		System.out.print("Commited Transaction with id: " + xid + "\n");
		return true;
	}
	public boolean Abort(int xid) throws RemoteException, InvalidTransactionException{
		System.out.print("Received ABORT-REQ." + "\n");
		//!!!!!!!!!!!!!!!!Crash after receiving decision but before committing/aborting
		cm.after_rec_before_operate();
		if(pre_images.containsKey(xid)) {
			m_data = pre_images.remove(xid);
			Save();
		}
		System.out.print("Aborted Transaction with id: " + xid + "\n");
		return true;
	}
	public boolean Prepare(int xid)throws RemoteException, TransactionAbortedException, InvalidTransactionException{
			//!!!!!!!!!!!!!!!Crash after receive vote request but before sending answer
			System.out.print("Received VOTE-REQ." + "\n");
			cm.rec_req_before_send();
			//!!!!!!!!!!!!!!Crash after decigin which answer to send (commit/abort)(YES/NO)
			boolean result = pre_images.containsKey(xid);
			cm.after_decision();
			//!!!!!!!!!!!!!!Crash after sending answer
			new Timer().schedule(new TimerTask() {
					@Override
					public void run() {
						cm.after_sending();
					}
				}, 100);
			return result;
	}
	public boolean Save(){
		return saveData() && saveImages();
	}

	public int Start() throws RemoteException{return -1;};

	// Writes a data item
	protected void writeData(int xid, String key, RMItem value)
	{
		synchronized(m_data) {
			if(!pre_images.containsKey(xid)){
				RMHashMap transaction_data = new RMHashMap();
				pre_images.put(xid, transaction_data);
			}
			RMHashMap td = pre_images.get(xid);
			if(!td.containsKey(key)){
				if(!m_data.containsKey(key)){
					td.put(key, null);
				}else{
						RMItem prev = m_data.get(key);
						td.put(key, prev);
				}
				pre_images.put(xid, td);
				m_data.put(key, value);
			}
		}
		Save();
	}

	// Remove the item out of storage
	protected void removeData(int xid, String key)
	{
		synchronized(m_data) {
			if(!pre_images.containsKey(xid)){
				RMHashMap transaction_data = new RMHashMap();
				pre_images.put(xid, transaction_data);
			}
			RMHashMap td = pre_images.get(xid);
			if(!td.containsKey(key)){
				if(!m_data.containsKey(key)){
					td.put(key, null);
				}else{
					RMItem prev = m_data.get(key);
					td.put(key, prev);
			}
				pre_images.put(xid, td);
				m_data.remove(key);
			}
		}
		Save();
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
