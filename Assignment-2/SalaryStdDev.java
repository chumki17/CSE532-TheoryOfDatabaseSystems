import java.sql.*;
import java.util.ArrayList;
	
public class SalaryStdDev {
	
	   static final String DB_URL = "jdbc:db2://localhost:50000/";
	
	   private static double getStdDev(ArrayList<Double> a){	
		   double sum =0 , sum1 =0;
		   for(int i = 0;i < a.size();i++){
			  sum = sum +a.get(i);
			  sum1 =sum1 + Math.pow(a.get(i), 2);
		   }
		   double mean_of_sq = sum1/ a.size();
		   double mean_sq = Math.pow(sum/ a.size(),2);
		   return Math.sqrt(mean_of_sq - mean_sq);
 	  }
	   
	   
	   public static void main(String[] args) {
	   Connection conn = null;
	   Statement stmt = null;
	   try{
	      Class.forName("com.ibm.db2.jcc.DB2Driver");
	      System.out.println("Connecting to database running at --- localhost:50000 ...\n");
	      
	      String db_name = args[0];
	      String tableName = args[1];
	      String username = args[2];
	      String password = args[3];
	      
	      String db_url = DB_URL+db_name+"";
	      conn = DriverManager.getConnection(db_url, username, password);
	      stmt = conn.createStatement();
	      
	      String query = "SELECT SALARY FROM " + tableName;
	      ResultSet rs = stmt.executeQuery(query);
	      ArrayList<Double> data = new ArrayList<Double>();
	      while(rs.next()){
	    	  		data.add(rs.getDouble("SALARY"));
					}
	      double stdDev= getStdDev(data);
	      System.out.println("Standard Deviation: "+stdDev+"\n" );
		  
	      rs.close();
	      stmt.close();
	      conn.close();
	      
	     
	   }catch(SQLException sqle){
	      
		   sqle.printStackTrace();
	   }catch(Exception e){
	     
	      e.printStackTrace();
	   }finally{
	     
	      try{
	         if(stmt!=null)
	            stmt.close();
	         if(conn!=null)
		        conn.close();
	      }catch(SQLException sqle2){
	    	  sqle2.printStackTrace();
	      }
	      
	   }
	}
}

