import java.sql.*;
import java.util.*;

class MyLinkedList<E> {
    private class Node {
        E data;
        Node next;

        Node(E data) {
            this.data = data;
            this.next = null;
        }
    }

    private Node head;
    private Node tail;
    private int size;

    public MyLinkedList() {
        head = null;
        tail = null;
        size = 0;
    }

    public void add(E data) {
        Node newNode = new Node(data);
        if (tail == null) {
            head = newNode;
            tail = newNode;
        } else {
            tail.next = newNode;
            tail = newNode;
        }
        size++;
    }

    public E get(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        Node current = head;
        for (int i = 0; i < index; i++) {
            current = current.next;
        }
        return current.data;
    }

    public E remove(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException();
        }
        if (index == 0) {
            E deleted = head.data;
            head = head.next;
            if (head == null) {
                tail = null;
            }
            size--;
            return deleted;
        } else {
            Node prev = head;
            for (int i = 0; i < index - 1; i++) {
                prev = prev.next;
            }
            E deleted = prev.next.data;
            prev.next = prev.next.next;
            if (prev.next == null) {
                tail = prev;
            }
            size--;
            return deleted;
        }
        
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}

class HashMap1<K, V> {
    private class Node {
        K key;
        V value;

        Node(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }

    private MyLinkedList<Node>[] buckets;
    private int size;
    private int N;

    public HashMap1() {
        this.N = 4;
        this.buckets = new MyLinkedList[N];
        this.size = 0;
        for (int i = 0; i < buckets.length; i++) {
            buckets[i] = new MyLinkedList<>();
        }
    }

    private void reHash()
    {
        MyLinkedList<Node>[] oldBucket = buckets;
        buckets = new MyLinkedList[2 * N];
        for(int i = 0; i<buckets.length; i++)
            buckets[i] = new MyLinkedList<>();

        for(int i = 0; i<oldBucket.length; i++)
        {
            MyLinkedList<Node> ll = oldBucket[i];
            for(int j = 0; j<ll.size(); j++)
            {
                Node node = ll.get(j);
                put(node.key , node.value);
            }
        }
    }
    private int hashFunction(K key) {
        int bi = key.hashCode();
        return Math.abs(bi) % N;
    }

    public int searchInll(K key , int bi)
    {
        MyLinkedList<Node> ll = buckets[bi];
        for(int i = 0; i<ll.size(); i++)
        {
            Node node = ll.get(i);
            if(node.key.equals(key))
                return i; // di
        }
        return -1;
    }
    public void put(K key, V value) {
        int bi = hashFunction(key);
        int di = searchInll(key, bi);

        if(di == -1)  // key doesn't exists
        {
            buckets[bi].add(new Node(key , value));
            size++;
        }
        else // key exists
        {
            Node node = buckets[bi].get(di);
            node.value = value;
        }

        double lambda = (double)size / N;
        if(lambda > 2.0)
            reHash();

    }

    public boolean containsKey(K key)
    {
        int bi = hashFunction(key);
        int di = searchInll(key, bi);
        if(di == -1)
            return false;
        else return true;
    }
    
    public V get(K key)
    {
        int bi = hashFunction(key);
        int di = searchInll(key, bi);
        if(di == -1)
            return null;
        else
        {
            Node node = buckets[bi].get(di);
            return node.value;
        }
    }

    public V remove(K key) {
        int bi = hashFunction(key);
        int di = searchInll(key, bi);

        if(di == -1)
            return null;
        else
        {
            size--;
            Node deleted = buckets[bi].remove(di);
            return deleted.value;
        }
    }


    public List<V> values() {
        List<V> values = new ArrayList<>();
        for(int i = 0; i < buckets.length; i++)
        {
            MyLinkedList<Node> ll = buckets[i];
            for(int j = 0; j < ll.size(); j++)
            {
                Node node = ll.get(j);
                values.add(node.value);
            }
        }
        return values;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }
}



class DataBaseConnection
{
    public static Connection getConnection() throws SQLException
    {
        try
        {
            Class.forName("com.mysql.jdbc.Driver");
        }catch(ClassNotFoundException ce)
        {
            System.out.println(ce.getMessage());
        }
        String url = "jdbc:mysql://localhost:3306/brts";
        String user = "root";
        String pass = "";
        Connection con = DriverManager.getConnection(url, user, pass);
        return con;
    }
}
class BRTSPortal{
    private MyLinkedList<Bus> buses;
    private MyLinkedList<Ticket> tickets;
    private HashMap1<String, User> registeredUsers;
    private HashMap1<String, Manager> registeredManagers;
    private int nextBusId;
    private int nextUserId;
    private int nextManagerId;
    private User loggedInUser;
    private Manager loggedInManager;
    Connection con;

    public BRTSPortal() {
        buses = new MyLinkedList<>();
        tickets = new MyLinkedList<>();
        registeredUsers = new HashMap1<>();
        registeredManagers = new HashMap1<>();
        nextBusId = 1;
        nextUserId = 1;
        nextManagerId = 1;
        loggedInUser = null;
        loggedInManager = null;
        try{
            con = DataBaseConnection.getConnection();
        }
        catch(Exception e)
        {
            System.out.println(e.getMessage());
        }
    }
    Scanner sc = new Scanner(System.in);
    
    public void addBus(String bus_no ,String routeName) {
        List<String> stops = new ArrayList<>();
        System.out.println("Enter stops for the bus. Type 'done' when finished:");
        while (true) {
            System.out.print("Enter stop: ");
            String stop = sc.nextLine();
            if (stop.equalsIgnoreCase("done")) {
                break;
            }
            stops.add(stop);
        }

        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO buses VALUES (? , ? , ?, ?)")) {
            pstmt.setInt(1, nextBusId);
            pstmt.setString(2, bus_no);
            pstmt.setString(3, routeName);
            pstmt.setString(4 , String.join(",", stops));
            pstmt.executeUpdate();
            Bus newBus = new Bus(nextBusId++, bus_no , routeName, stops);
            buses.add(newBus);
            System.out.println("Bus added successfully: " + newBus);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void removeBus(int busId) {
        String checkQuery = "select * from tickets where bus_id = ?";
        try {
            PreparedStatement pstmt = con.prepareStatement(checkQuery);
            pstmt.setInt(1, busId);
            ResultSet rs = pstmt.executeQuery();
            if(rs.next())
            {
                for (int i = 0; i < buses.size(); i++) {
                    Bus bus = buses.get(i);
                    if (bus.getBusId() == busId) 
                            buses.remove(i);
                }
                for(int j = 0; j < tickets.size(); j++)
                {
                        Ticket ticket = tickets.get(j);
                        if(ticket.getBus().getBusId() == busId)
                                tickets.remove(j);
                }
                PreparedStatement pstmt1 = con.prepareStatement("DELETE FROM tickets WHERE bus_id = ?");
                pstmt1.setInt(1, busId);
                pstmt1.executeUpdate();
                PreparedStatement pstmt2 = con.prepareStatement("DELETE FROM buses WHERE bus_id = ?");    
                pstmt2.setInt(1, busId);           
                pstmt2.executeUpdate();
                System.out.println("Bus removed successfully.");
                return;
                    
                
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
        System.out.println("Bus with ID " + busId + " is not available.");
    }

    public void viewBuses() {
        if (buses.isEmpty()) {
            System.out.println("No buses available.");
        } else {
            for (int i = 0; i < buses.size(); i++) {
                System.out.println(buses.get(i));
            }
        }
    }

    public void viewBusesForRoute(String from, String to) {
        System.out.println("Available buses from " + from + " to " + to + ":");
        for (int i = 0; i < buses.size(); i++) {
            Bus bus = buses.get(i);
            if (bus.canTravel(from, to)) {
                System.out.println(bus);
            }
        }
    }

    public void viewUserInformation() {
        if (registeredUsers.isEmpty()) {
            System.out.println("No users registered.");
        } else {
            System.out.println("Registered Users:");
            for (User user : registeredUsers.values()) { 
                System.out.println(user);
            }
        }
    }

    
    public void viewTicketBookingInfo() {
        if (tickets.isEmpty()) {
            System.out.println("No tickets booked.");
        } else {
            System.out.println("Ticket Booking Information:");
            for (int i = 0; i < tickets.size(); i++) {
                Ticket ticket = tickets.get(i);
                System.out.println(ticket);
            }
        }
    }
        
        
    public void buyTicket(String buyFrom, String buyTo) {
        Connection balanceConnection = null;
        Connection ticketConnection = null;
        if (loggedInUser == null) {
            System.out.println("Please log in first.");
            return;
        }
    
        MyLinkedList<Bus> availableBuses = new MyLinkedList<>();
        for (int i = 0; i < buses.size(); i++) {
            Bus bus = buses.get(i);
            if (bus.canTravel(buyFrom, buyTo)) {
                availableBuses.add(bus);
            }
        }
    
        if (availableBuses.isEmpty()) {
            System.out.println("No buses available for the route from " + buyFrom + " to " + buyTo);
            for (int i = 0; i < buses.size(); i++) {
                Bus bus = buses.get(i);
                if (!bus.canTravelFromLocation(buyFrom)) {
                    System.out.println("Bus " + bus.getBusId() + " is currently at stop: " + bus.getCurrentStop());
                }
            }
            return;
        }
    
        System.out.println("Available Buses:");
        for (int i = 0; i < availableBuses.size(); i++) {
            Bus bus = availableBuses.get(i);
            System.out.println(bus + ", Ticket Price: " + bus.countTicketPrice(buyFrom, buyTo));
        }
    
        int busId = -1;
        while (true) {
            try {
                System.out.print("Enter Bus ID to book ticket: ");
                busId = sc.nextInt();
                break; // break out of the loop if the input is valid
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a valid Bus ID.");
                sc.nextLine(); // clear the invalid input
            }
        }
    
        Bus selectedBus = null;
        for (int i = 0; i < availableBuses.size(); i++) {
            Bus bus = availableBuses.get(i);
            if (bus.getBusId() == busId) {
                selectedBus = bus;
                break;
            }
        }
    
        if (selectedBus == null) {
            System.out.println("Invalid Bus ID.");
            return;
        }
    
        if (!selectedBus.canTravelFromLocation(buyFrom)) {
            System.out.println("Bus " + selectedBus.getBusId() + " has already passed your current location. Cannot book ticket.");
            System.out.println("Bus " + selectedBus.getBusId() + " is currently at stop: " + selectedBus.getCurrentStop());
            return;
        }
    
        try {
            balanceConnection = DataBaseConnection.getConnection();
            balanceConnection.setAutoCommit(false);
            double ticketPrice = selectedBus.countTicketPrice(buyFrom, buyTo);
            if (loggedInUser.getBalance() < ticketPrice) {
                System.out.println("Insufficient balance.");
                return;
            }
    
            // Deduct balance
            PreparedStatement pt1 = balanceConnection.prepareStatement("update users set balance = ? where name = ?");
            pt1.setDouble(1, loggedInUser.getBalance() - ticketPrice);
            pt1.setString(2, loggedInUser.getName());
            pt1.executeUpdate();
            loggedInUser.setBalance(loggedInUser.getBalance() - ticketPrice);
            balanceConnection.commit();
    
            ticketConnection = DataBaseConnection.getConnection();
            ticketConnection.setAutoCommit(false);
            PreparedStatement pstmt = ticketConnection.prepareStatement("INSERT INTO tickets (user_name, bus_id, bus_no, route_name, from_location, to_location , ticket_price) VALUES (?, ?, ?, ?, ?, ?, ?)");
            pstmt.setString(1, loggedInUser.getName());
            pstmt.setInt(2, selectedBus.getBusId());
            pstmt.setString(3, selectedBus.getBusNo());
            pstmt.setString(4, selectedBus.getRouteName());
            pstmt.setString(5, buyFrom);
            pstmt.setString(6, buyTo);
            pstmt.setDouble(7, ticketPrice);
            pstmt.executeUpdate();
            ticketConnection.commit();
    
            Ticket ticket = new Ticket(selectedBus, loggedInUser, buyFrom, buyTo, ticketPrice);
            tickets.add(ticket);
            System.out.println("Ticket booked successfully: " + ticket);
        } catch (SQLException e) {
            System.out.println("Transaction failed. Error: " + e.getMessage());
            try {
                if (balanceConnection != null) balanceConnection.rollback(); // Rollback balance update
                System.out.println("Balance transaction rolled back.");
    
                if (ticketConnection != null) ticketConnection.rollback(); // Rollback ticket update
                System.out.println("Ticket transaction rolled back.");
            } catch (SQLException rollbackException) {
                System.out.println("Failed to rollback transaction: " + rollbackException.getMessage());
            }
        } finally {
            try {
                if (balanceConnection != null) balanceConnection.close();
                if (ticketConnection != null) ticketConnection.close();
            } catch (SQLException closeException) {
                System.out.println("Failed to close connection: " + closeException.getMessage());
            }
        }
    }
    
    
    public void registerUser(String name, String password , double balance) {
        try  {
            PreparedStatement pstmt = con.prepareStatement("INSERT INTO users (name, password , balance) VALUES (?, ? , ?)");
            pstmt.setString(1, name);
            pstmt.setString(2, password);
            pstmt.setDouble(3, balance);
            pstmt.executeUpdate();
            User user = new User(nextUserId++, name, password , balance);
            registeredUsers.put(name, user);
            System.out.println("User registered successfully: " + name);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean loginUser(String name, String password) {
        User user = registeredUsers.get(name);
        if (user != null && user.getPassword().equals(password)) {
            loggedInUser = user;
            System.out.println("Logged in successfully as " + name);
            return true;
        } else {
            System.out.println("Please Register First");
            return false;
        }
    }

    public void logoutUser() {
        if (loggedInUser != null) {
            System.out.println("Logged out successfully: " + loggedInUser.getName());
            loggedInUser = null;
        } else {
            System.out.println("No user is currently logged in.");
        }
    }

    
    public void registerManager(String name, String password) {
        try (PreparedStatement pstmt = con.prepareStatement("INSERT INTO managers (name, password) VALUES (?, ?)")) {
            pstmt.setString(1, name);
            pstmt.setString(2, password);
            pstmt.executeUpdate();
            Manager manager = new Manager(nextManagerId++, name, password);
            registeredManagers.put(name, manager);
            System.out.println("Manager registered successfully: " + name);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean loginManager(String name, String password) {
        Manager manager = registeredManagers.get(name);
        if (manager != null && manager.getPassword().equals(password)) {
            loggedInManager = manager;
            System.out.println("Logged in successfully as " + name);
            return true;
        } else {
            System.out.println("Please Register First");
            return false;
        }
    }

    public void logoutManager() {
        if (loggedInManager != null) {
            System.out.println("Logged out successfully: " + loggedInManager.getName());
            loggedInManager = null;
        } else {
            System.out.println("No manager is currently logged in.");
        }
    }

    public static void main(String[] args) {
        BRTSPortal portal = new BRTSPortal();
        Scanner sc = new Scanner(System.in);
    
        while (true) {
            try {
                System.out.println("\nBRTS Portal Main Menu:");
                System.out.println("1. Register User");
                System.out.println("2. Login User");
                System.out.println("3. Register Manager");
                System.out.println("4. Login Manager");
                System.out.println("5. Exit");
                System.out.print("Choose an option: ");
                int choice = sc.nextInt();
                sc.nextLine(); // clear the buffer
    
                switch (choice) {
                    case 1:
                        System.out.print("Enter your name: ");
                        String userName = sc.nextLine();
                        System.out.print("Enter your password: ");
                        String userPassword = sc.nextLine();
    
                        double initialBalance = 0;
                        while (true) {
                            try {
                                System.out.print("Enter initial balance: ");
                                initialBalance = sc.nextDouble();
                                sc.nextLine(); // clear the buffer
                                if (initialBalance < 0) {
                                    System.out.println("Balance cannot be negative. Please enter a valid amount.");
                                } else {
                                    break;
                                }
                            } catch (InputMismatchException e) {
                                System.out.println("Invalid input. Please enter a numeric value for the balance.");
                                sc.nextLine(); // clear the invalid input from the scanner
                            }
                        }
    
                        portal.registerUser(userName, userPassword, initialBalance);
                        break;
    
                    case 2:
                        System.out.print("Enter your name: ");
                        String loginUser = sc.nextLine();
                        System.out.print("Enter your password: ");
                        String loginUserPassword = sc.nextLine();
                        if (portal.loginUser(loginUser, loginUserPassword)) {
                            userMenu(portal, sc);
                        }
                        break;
    
                    case 3:
                        System.out.print("Enter your name: ");
                        String managerName = sc.nextLine();
                        System.out.print("Enter your password: ");
                        String managerPassword = sc.nextLine();
                        portal.registerManager(managerName, managerPassword);
                        break;
    
                    case 4:
                        System.out.print("Enter your name: ");
                        String loginManager = sc.nextLine();
                        System.out.print("Enter your password: ");
                        String loginManagerPassword = sc.nextLine();
                        if (portal.loginManager(loginManager, loginManagerPassword)) {
                            managerMenu(portal, sc);
                        }
                        break;
    
                    case 5:
                        System.out.println("Exiting BRTS Portal.");
                        System.exit(0);
    
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a number.");
                sc.nextLine(); // clear the invalid input from the scanner
            }
        }
    }
    
    
    private static void managerMenu(BRTSPortal portal, Scanner sc) {
        while (true) {
            try {
                System.out.println("\nManager Menu:");
                System.out.println("1. Add Bus");
                System.out.println("2. Remove Bus");
                System.out.println("3. View Buses");
                System.out.println("4. View User Information");
                System.out.println("5. View Ticket Booking Information");
                System.out.println("6. Logout");
                System.out.print("Choose an option: ");
                int choice = sc.nextInt();
                sc.nextLine(); // clear the buffer
    
                switch (choice) {
                    case 1:
                        System.out.print("Enter Bus No: ");
                        String bus_no = sc.nextLine();
                        System.out.print("Enter route name: ");
                        String routeName = sc.nextLine();
                        portal.addBus(bus_no, routeName);
                        break;
                    case 2:
                        System.out.print("Enter Bus ID to remove: ");
                        int busIdToRemove = sc.nextInt();
                        sc.nextLine(); // clear the buffer
                        portal.removeBus(busIdToRemove);
                        break;
                    case 3:
                        portal.viewBuses();
                        break;
                    case 4:
                        portal.viewUserInformation();
                        break;
                    case 5:
                        portal.viewTicketBookingInfo();
                        break;
                    case 6:
                        portal.logoutManager();
                        return;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a number.");
                sc.nextLine(); // clear the invalid input from the scanner
            }
        }
    }
    

    private static void userMenu(BRTSPortal portal, Scanner sc) {
        while (true) {
            try {
                System.out.println("\nUser Menu:");
                System.out.println("1. Search Buses");
                System.out.println("2. Buy Ticket");
                System.out.println("3. Logout");
                System.out.print("Choose an option: ");
                int choice = sc.nextInt();
                sc.nextLine(); // clear the buffer
    
                switch (choice) {
                    case 1:
                        System.out.print("Enter from location: ");
                        String fromLocation = sc.nextLine();
                        System.out.print("Enter to location: ");
                        String toLocation = sc.nextLine();
                        portal.viewBusesForRoute(fromLocation, toLocation);
                        break;
                    case 2:
                        System.out.print("Enter from location: ");
                        String buyFrom = sc.nextLine();
                        System.out.print("Enter to location: ");
                        String buyTo = sc.nextLine();
                        portal.buyTicket(buyFrom, buyTo);
                        break;
                    case 3:
                        portal.logoutUser();
                        return;
                    default:
                        System.out.println("Invalid choice. Please try again.");
                }
            } catch (InputMismatchException e) {
                System.out.println("Invalid input. Please enter a number.");
                sc.nextLine(); // clear the invalid input from the scanner
            }
        }
    }
    
}

class User {
    private int userId;
    private String name;
    private String password;
    private double balance;

    public User(int userId, String name, String password , double initialBalance) {
        this.userId = userId;
        this.name = name;
        this.password = password;
        this.balance = initialBalance;
    }

    public int getUserId() {
        return userId;
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

    public double getBalance()
    {
        return balance;
    }
    public void setBalance(double bal)
    {
        this.balance = bal;
    }
    @Override
    public String toString() {
        return "User ID: " + userId + ", Name: " + name;
    }
}

class Manager {
    private int managerId;
    private String name;
    private String password;

    public Manager(int managerId, String name, String password) {
        this.managerId = managerId;
        this.name = name;
        this.password = password;
    }

    public int getManagerId() {
        return managerId;
    }

    public String getName() {
        return name;
    }

    public String getPassword() {
        return password;
    }

}

class Bus {
    private int busId;
    private String bus_no;
    private String routeName;
    private List<String> stops;
    int currentStopIndex;
    MoveToNextStop m1;
    public Bus(int busId, String bus_no, String routeName, List<String> stops) {
        this.busId = busId;
        this.bus_no = bus_no;
        this.routeName = routeName;
        this.stops = stops;
        this.currentStopIndex = 0;
        m1 = new MoveToNextStop(this);
        m1.start();
    }

    public String getBusNo()
    {
        return bus_no;
    }
    public int getBusId() {
        return busId;
    }

    public String getRouteName() {
        return routeName;
    }

    public String getCurrentStop() {
        return stops.get(currentStopIndex);
    }

    public boolean canTravel(String from, String to) {
        int fromIndex = stops.indexOf(from);
        int toIndex = stops.indexOf(to);

        return fromIndex != -1 && toIndex != -1 && fromIndex < toIndex;
    }

    public int countTicketPrice(String from , String to)
    {
        int fromIndex = stops.indexOf(from);
        int toIndex = stops.indexOf(to);
        return (toIndex - fromIndex) * 5;
    }

    public boolean canTravelFromLocation(String from) {
        int fromIndex = stops.indexOf(from);
        return fromIndex >= currentStopIndex;
    }

    public List<String> getStops()
    {
        return stops;
    }
    @Override
    public String toString() {
        return "Bus ID: " + busId + ", Bus No: " + bus_no + ", Route: " + routeName + ", Current Stop: " + getCurrentStop();
    }
}

class Ticket {
    private Bus bus;
    private User user;
    private String from;
    private String to;
    private double ticket_price;

    public Ticket(Bus bus, User user, String from, String to , double ticket_price) {
        this.bus = bus;
        this.user = user;
        this.from = from;
        this.to = to;
        this.ticket_price = ticket_price;
    }

    public User getUser() {
        return user;
    }

    public Bus getBus() {
        return bus;
    }

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public double getTicketPrice()
    {
        return ticket_price;
    }
    @Override
    public String toString() {
        return "User: " + user.getName() + ", Bus ID: " + bus.getBusId() +", Bus No: " + bus.getBusNo() +", Route: " + bus.getRouteName()
                + ", From: " + from + ", To:"+to + " , Ticket Price: " + ticket_price;
    }
}


class MoveToNextStop extends Thread
{
    Bus bus;
    public MoveToNextStop(Bus bus)
    {
        this.bus = bus;
    }
    public void run()
    {
        while(true)
        {
            bus.currentStopIndex = (bus.currentStopIndex + 1) % bus.getStops().size();
            try
            {
                Thread.sleep(5000);
            }
            catch(InterruptedException ie)
            {
                System.out.println(ie.getMessage());
            }
        }
    }
}