import java.sql.*;

public class CarRentalAssignment {

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		
		// TODO Auto-generated method stub
		final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
		final String DB_URL = "jdbc:mysql://localhost:3306/mycarrentaldb";
		final String USER_NAME = "root";
		final String PASSWORD = "Welcome@01";
		
		Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
     
        try {
        	Class.forName(JDBC_DRIVER);
            connection = DriverManager.getConnection(DB_URL,USER_NAME,PASSWORD);
 
            System.out.println(" ** Advanced Database Management Homework: JDBC and SQL Integration **");
            statement = connection.createStatement();
            
            // New Rental Record Insertion:
            System.out.println(" ************ New Rental Record Insertion Starts ************");
            String newRentalRecordInsertionQuery = "INSERT INTO rentaldata (rentalrecordid,customerid, vehicleid, startdate, enddate) " +
                    "VALUES (" +
            		" 01, "+
                    "(SELECT customerid FROM customers WHERE customername = 'Alex Johnson'), " +
                    "(SELECT vehicleid FROM vehicles WHERE registrationnumber = 'SGX1234B'), " +
                    "CURDATE(), " +
                    "DATE_ADD(CURDATE(), INTERVAL 15 DAY))";
            statement.executeUpdate(newRentalRecordInsertionQuery);
            System.out.println(" ************ New Rental Record Insertion Ends ************");
            
            // Extended Rental Duration
            System.out.println(" ************ Extended Rental Duration Starts ************");
            String extendedRentalDurationQuery = "UPDATE rentaldata r " +
                    "INNER JOIN customers c ON r.customerid = c.customerid " +
                    "INNER JOIN vehicles v ON r.vehicleid = v.vehicleid " +
                    "SET r.enddate = DATE_ADD(CURDATE(), INTERVAL 2 MONTH) " +
                    "WHERE c.customername = 'Maria Lee' AND v.registrationnumber = 'SGP6789C'";
            statement.executeUpdate(extendedRentalDurationQuery);
            System.out.println(" ************ Extended Rental Duration Ends ************");
            
            // Rental Records Overview:
            System.out.println(" ************ Rental Records Overview Starts ************");
            String rentalRecordsOverviewQuery = "SELECT r.startdate, r.enddate, v.registrationnumber, v.brand, c.customername" +
            									 " FROM rentaldata r" +
            									 " JOIN vehicles v ON r.vehicleid = v.vehicleid" +
            									 " JOIN customers c ON r.customerid = c.customerid" +
            									 " ORDER BY v.category, r.startdate ";
            resultSet = statement.executeQuery(rentalRecordsOverviewQuery);
            printRentalRecordsReport(resultSet);
            System.out.println(" ************ Rental Records Overview Ends ************");
            
            // Expired Rentals Report:
            System.out.println(" ************ Expired Rentals Report Starts ************");
            String expriedRentalRecordsQuery = "SELECT * FROM rentaldata WHERE enddate < CURDATE()";
            resultSet = statement.executeQuery(expriedRentalRecordsQuery);
            displayExpiredRentalRecords(resultSet);
            System.out.println(" ************ Expired Rentals Report Ends ************");
            
            // Specific Date Query:
            System.out.println(" ************ Specific Date Query Starts ************");
            String specificDateQuery = "SELECT v.registrationnumber, c.customername, r.startdate, r.enddate" +
					 " FROM rentaldata r" +
					 " INNER JOIN vehicles v ON r.vehicleid = v.vehicleid" +
					 " INNER JOIN customers c ON r.customerid = c.customerid" +
					 " WHERE r.startdate <= '2023-04-05' AND r.enddate >= '2023-04-05'; ";
            resultSet = statement.executeQuery(specificDateQuery);
            displaySpecificDateRecords(resultSet);
            System.out.println(" ************ Specific Date Query Ends ************");
            
            // Today's Rentals:
            System.out.println(" ************ Today's Rental Starts ************");
            String todaysRentalsQuery = "SELECT v.registrationnumber, c.customername, r.startdate, r.enddate" +
					 " FROM rentaldata r" +
					 " JOIN vehicles v ON r.vehicleid = v.vehicleid" +
					 " JOIN customers c ON r.customerid = c.customerid" +
					 " WHERE r.startdate = CURDATE(); ";
            resultSet = statement.executeQuery(todaysRentalsQuery);
            displayTodaysRentalsRecords(resultSet);
            System.out.println(" ************ Today's Rental Ends ************");
            
            // Rental Period Analysis:
            System.out.println(" ************ Rental Period Analysis Starts ************");
            String rentalPeriodAnalysisQuery = "SELECT v.registrationnumber, c.customername, r.startdate, r.enddate" +
					 " FROM rentaldata r" +
					 " JOIN vehicles v ON r.vehicleid = v.vehicleid" +
					 " JOIN customers c ON r.customerid = c.customerid" +
					 " WHERE (r.startdate BETWEEN '2023-04-01' AND '2023-04-15') OR (r.enddate BETWEEN '2023-04-01' AND '2023-04-15'); ";
            resultSet = statement.executeQuery(rentalPeriodAnalysisQuery);
            displayRentalPeriodAnalysisRecords(resultSet);
            System.out.println(" ************ Rental Period Analysis Ends ************");
            
            // Vehicle Availability Check:
            System.out.println(" ************ Vehicle Availability Check Starts ************");
            String vehicleAvailabilityCheckQuery = "SELECT v.registrationnumber" +
					 " FROM vehicles v" +
					 " WHERE v.vehicleid NOT IN (" +
					 " SELECT r.vehicleid" +
					 " FROM rentaldata r" +
					 " WHERE r.startdate <= '2023-04-05' AND r.enddate >= '2023-04-05' ); ";
            resultSet = statement.executeQuery(vehicleAvailabilityCheckQuery);
            displayVehicleAvailabilityCheckRecords(resultSet);
            System.out.println(" ************ Vehicle Availability Check Ends ************");
            
            // Extended Availability Analysis:
            System.out.println(" ************ Extended Availability Analysis Starts ************");
            String extendedAvailabilityAnalysisQuery = "SELECT v.registrationnumber" +
					 " FROM vehicles v" +
					 " JOIN rentaldata r ON r.vehicleid = v.vehicleid" +
					 " WHERE (r.startdate >= '2023-04-01' AND r.enddate <= '2023-04-15')"; 
            resultSet = statement.executeQuery(extendedAvailabilityAnalysisQuery);
            displayExtendedAvailabilityAnalysisRecords(resultSet);
            System.out.println(" ************ Extended Availability Analysis Ends ************");
            
            // Upcoming Rental Availability:
            System.out.println(" ************ Upcoming Rental Availability Starts ************");
            String upcomingRentalAvailabilityQuery = "SELECT v.registrationnumber" +
					 " FROM vehicles v" +
					 " WHERE v.vehicleid NOT IN (" +
					 " SELECT r.vehicleid" +
					 " FROM rentaldata r" +
					 " WHERE r.startdate BETWEEN CURDATE() AND DATE_ADD(CURDATE(), INTERVAL 15 DAY) ); ";
            resultSet = statement.executeQuery(upcomingRentalAvailabilityQuery);
            displayUpcomingRentalAvailabilityRecords(resultSet);
            System.out.println(" ************ Upcoming Rental Availability Ends ************");
            
        }
        catch (Exception exception) {
            System.out.println(exception);
        }
        finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
	}
	
	public static void printRentalRecordsReport(ResultSet resultSet) throws SQLException {
	  // Print report header
	  System.out.println("Rental Records Overview Report");
	  System.out.println("-------------------------------------------------------------------------------------------");
	  System.out.printf("%-15s %-15s %-20s %-20s %-20s\n",
	          "Start Date", "End Date", "Registration Number", "Brand", "Customer Name");
	  System.out.println("--------------------------------------------------------------------------------------------");

	  // Print Data Rows
	  while (resultSet.next()) {
	    String startDate = resultSet.getString("startdate");
	    String endDate = resultSet.getString("enddate");
	    String registrationNumber = resultSet.getString("registrationnumber");
	    String brand = resultSet.getString("brand");
	    String customerName = resultSet.getString("customername");

	    System.out.printf("%-15s %-15s %-20s %-20s %-20s\n",
		    startDate, endDate, registrationNumber, brand, customerName);
	  }
	  
	}
	
	public static void displayExpiredRentalRecords(ResultSet resultSet) throws SQLException {
		System.out.println("Display Expired Rental Records Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-15s %-15s %-20s %-20s %-20s\n",
		          "Rental Record Id", "Customer Id", "Vehicle Id", "Start Date", "End Date");
		  System.out.println("--------------------------------------------------------------------------------------------");
		  
		  if (resultSet != null) {
			  while (resultSet.next()) {
			    String rentalRecordId = resultSet.getString("rentalrecordid");
			    String customerId = resultSet.getString("customerid");
			    String vehicleId = resultSet.getString("vehicleid");
			    String startDate = resultSet.getString("startdate");
			    String endDate = resultSet.getString("enddate");

			    System.out.printf("%-15s %-15s %-20s %-20s %-20s\n",
			    		rentalRecordId, customerId, vehicleId, startDate, endDate);
			  }
		  }
		  else {
			  System.out.println("No Expired Rental Records Found.");
		  }
	}
	
	public static void displaySpecificDateRecords(ResultSet resultSet) throws SQLException {
		  // Print report header
		  System.out.println("Display Specific Records Overview Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-20s %-20s %-20s %-20s\n",
		          "Registration Number", "Customer Name","Start Date", "End Date");
		  System.out.println("--------------------------------------------------------------------------------------------");

		  while (resultSet.next()) {
			String registrationNumber = resultSet.getString("registrationnumber");
			String customerName = resultSet.getString("customername");
		    String startDate = resultSet.getString("startdate");
		    String endDate = resultSet.getString("enddate");
		    System.out.printf("%-20s %-20s %-20s %-20s\n",
		    		registrationNumber, customerName, startDate, endDate);
		  }  
	}
	
	public static void displayTodaysRentalsRecords(ResultSet resultSet) throws SQLException {
		  // Print report header
		  System.out.println("Display Todays Rentals Records Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-20s %-20s %-20s %-20s\n",
		          "Registration Number", "Customer Name","Start Date", "End Date");
		  System.out.println("--------------------------------------------------------------------------------------------");

		  while (resultSet.next()) {
			String registrationNumber = resultSet.getString("registrationnumber");
			String customerName = resultSet.getString("customername");
		    String startDate = resultSet.getString("startdate");
		    String endDate = resultSet.getString("enddate");
		    System.out.printf("%-20s %-20s %-20s %-20s\n",
		    		registrationNumber, customerName, startDate, endDate);
		  }  
	}
	
	public static void displayRentalPeriodAnalysisRecords(ResultSet resultSet) throws SQLException {
		  // Print report header
		  System.out.println("Display Rental Period Analysis Records Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-20s %-20s %-20s %-20s\n",
		          "Registration Number", "Customer Name","Start Date", "End Date");
		  System.out.println("--------------------------------------------------------------------------------------------");

		  while (resultSet.next()) {
			String registrationNumber = resultSet.getString("registrationnumber");
			String customerName = resultSet.getString("customername");
		    String startDate = resultSet.getString("startdate");
		    String endDate = resultSet.getString("enddate");
		    System.out.printf("%-20s %-20s %-20s %-20s\n",
		    		registrationNumber, customerName, startDate, endDate);
		  }  
	}
	
	public static void displayVehicleAvailabilityCheckRecords(ResultSet resultSet) throws SQLException {
		  // Print report header
		  System.out.println("Display Vehicle Availability Check Records Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-30s\n","Registration Number");
		  System.out.println("--------------------------------------------------------------------------------------------");
		  
		  while (resultSet.next()) {
			String registrationNumber = resultSet.getString("registrationnumber");
		    System.out.printf("%-30s\n",registrationNumber);
		  }
	}
	
	public static void displayExtendedAvailabilityAnalysisRecords(ResultSet resultSet) throws SQLException {
		  // Print report header
		  System.out.println("Display Extended Availability Analysis Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-30s\n","Registration Number");
		  System.out.println("--------------------------------------------------------------------------------------------");
		  
		  while (resultSet.next()) {
			String registrationNumber = resultSet.getString("registrationnumber");
		    System.out.printf("%-30s\n",registrationNumber);
		  }
	}
	
	public static void displayUpcomingRentalAvailabilityRecords(ResultSet resultSet) throws SQLException {
		  // Print report header
		  System.out.println("Display Upcoming Rental Availability Records Report");
		  System.out.println("-------------------------------------------------------------------------------------------");
		  System.out.printf("%-30s\n","Registration Number");
		  System.out.println("--------------------------------------------------------------------------------------------");
		  
		  if (!resultSet.next()) {
		    System.out.println("No Upcoming Rental Availability.");
		  }
		  else {
			  while (resultSet.next()) {
				String registrationNumber = resultSet.getString("registrationnumber");
			    System.out.printf("%-30s\n",registrationNumber);
			  }
		  }
	}
}
