package com.anagarle.connector.splunk;

import com.splunk.HttpService;
import com.splunk.Job;
import com.splunk.SSLSecurityProtocol;
import com.splunk.SavedSearch;
import com.splunk.SavedSearchCollection;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

public class SplunkSearch
{
  static Service service = null;
    
  private static void CreasteService(String hostname, String username, String password, int port, String Schema) 
  {
	  try
	    {
		  if (service == null)
		  {
	  
			  Map<String, Object> args = new HashMap<String, Object>();
			  args.put("host", hostname);
			  args.put("port", Integer.valueOf(port));
			  args.put("schema", Schema);
      
			  HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
			  service = new Service(args);
			  service.login(username, password);
			  System.out.println("Authentication Successful.\n");
		  }
		  else 
		  {
			  System.out.println("Service Already created");
		  }
	    } catch (Exception ex) 
	  {
	    	System.out.println("Authentication Failed. Please check Splunk Settings.");
	    	System.out.println("Stopping Execution...");
	    	ex.printStackTrace();
	    	System.exit(0);	
	  }
  }
  
  private static Job getSplunkService(String savedSearchValue, String appname, String FromTime, String ToTime, String username)
  {
    try
    {
      ServiceArgs namespace = new ServiceArgs();
      namespace.setApp(appname);
      namespace.setOwner(username);

      SavedSearchCollection savedSearches = service.getSavedSearches(namespace);
      
      SavedSearch savedSearch = (SavedSearch)savedSearches.get(savedSearchValue);
      
      if (savedSearch == null) 
      {
        System.out.println("Saved search " + savedSearchValue + " not present in splunk");
    	System.out.println("Stopping Execution...");
    	System.exit(0);	
        
      }
      savedSearch.setDispatchEarliestTime(FromTime);
      savedSearch.setDispatchLatestTime(ToTime);
      savedSearch.update();
      savedSearch.refresh();

      SavedSearch tmpsavedSearch = (SavedSearch)savedSearches.get(savedSearchValue);

      DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
      dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
  	  Date From = new Date(Long.parseLong(tmpsavedSearch.getDispatchEarliestTime()) * 1000);
  	  Date To = new Date(Long.parseLong(tmpsavedSearch.getDispatchLatestTime()) * 1000);
      System.out.println("------------------------------------------------------------------------------------------------");
      System.out.println("Starting Splunk Search between " + dateFormat.format(From) + " and " + dateFormat.format(To));

      Job job = savedSearch.dispatch();
      
      //long startTime = System.currentTimeMillis();
      
      while (!job.isDone()) {
        //System.out.println("Waiting for job to complete. Time Completed:" + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - startTime) + " Minutes.");
        Thread.sleep(2000L);
        job.refresh();
      }
      return job;
    }
    catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }
  
  public static void main(String[] args)
  {
    long startTime = System.currentTimeMillis();
    
    String line = "";
    StringBuilder sb = new StringBuilder();
    
    int COUNT = 10000;
    
    InputStream inputProperties = null;
    Properties prop = new Properties();
    try {
      inputProperties = new FileInputStream("SplunkReader.properties");
      prop.load(inputProperties);
      System.out.println("Properties File Loaded.\n");
    } catch (IOException e) {
    	System.out.println("Properties File Not Found. Stopping Execution.");
    	System.exit(0);
    }
    
    String hostname = prop.getProperty("hostname");
    int port = Integer.parseInt(prop.getProperty("port"));
    String schema = prop.getProperty("schema");
    String username = prop.getProperty("username");
    System.out.println("Splunk Settings");
    System.out.println("-----------------------------");
    System.out.println("Hostname : " + hostname);
    System.out.println("Port : " + port);
    System.out.println("Schema : " + schema);
    System.out.println("Username : " + username);
    System.out.println("-----------------------------\n");

    String appname = prop.getProperty("appname");
    String searchname = prop.getProperty("searchname");
    String FromTime = prop.getProperty("from");
    String ToTime = prop.getProperty("to");
    String Interval = prop.getProperty("interval");
    System.out.println("Search Information");
    System.out.println("-----------------------------");
    System.out.println("Appname : " + appname);
    System.out.println("Searchname : " + searchname);
    System.out.println("From : " + FromTime);
    System.out.println("To : " + ToTime);
    System.out.println("interval : " + Interval);
    System.out.println("-----------------------------\n");
    
    String outfile = prop.getProperty("outputfilepath");    
    String outfileprefix = prop.getProperty("outputfileprefix");
    System.out.println("Output Information");
    System.out.println("-----------------------------");
    System.out.println("Output Directory : " + outfile ) ;
    System.out.println("Output File Prefix : " + outfileprefix ) ;
    System.out.println("-----------------------------\n");
    
    String password = prop.getProperty("password");
    int  sleeptime = Integer.parseInt(prop.getProperty("sleeptime"));

    CreasteService(hostname, username, password, port, schema);
    
    for (long i = Long.parseLong(FromTime); i < Long.parseLong(ToTime); i += Integer.parseInt(Interval))
    {
      long tempStartTime = System.currentTimeMillis();
      String tempFrom = Long.toString(i);
      String tempTo = Long.toString(i + Integer.parseInt(Interval));
      
      Job job = getSplunkService(searchname, appname, tempFrom, tempTo, username);
      
      try
      {
        Integer offset = Integer.valueOf(0);
        boolean recordsAvailable = true;
        int totalResoutCount = job.getResultCount();
        
        System.out.println("Total Events received: " + totalResoutCount);
        
        int countLines = 0;
        while (recordsAvailable)
        {

          Map<String,Object> jobMap = new HashMap<String,Object>();
          jobMap.put("count", Integer.valueOf(COUNT));
          jobMap.put("output_mode", "csv");
          jobMap.put("offset", offset);
          InputStream stream = job.getResults(jobMap);
          BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
          
          while ((line = reader.readLine()) != null) {
            countLines++;
            if (!line.contains("_raw"))
            {
              sb.append(line.replace("\"", ""));
              sb.append("\n");
            }
          }
          
          if (countLines < totalResoutCount) {
            recordsAvailable = true;
          } else {
            recordsAvailable = false;
          }
          offset = Integer.valueOf(offset.intValue() + COUNT);
        }
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    	Date From = new Date(Long.parseLong(tempFrom) * 1000);
    	Date To = new Date(Long.parseLong(tempTo) * 1000);
        
        System.out.println("Writing Lines to file : " + outfile + outfileprefix + "-" +  dateFormat.format(From).toString().trim().replaceAll("\\s","_").replaceAll("\\/","") + "--" +  dateFormat.format(To).toString().trim().replaceAll("\\s","_").replaceAll("\\/",""));
        PrintWriter Writer = new PrintWriter(outfile + outfileprefix + "-" +  dateFormat.format(From).toString().trim().replaceAll("\\s","_").replaceAll("\\/","") + "--" +  dateFormat.format(To).toString().trim().replaceAll("\\s","_").replaceAll("\\/",""));
        Writer.println(sb.toString().trim());
        sb.setLength(0);
        Writer.close();
        long tempStopTime = System.currentTimeMillis();
        long tempelapsedTime = TimeUnit.MILLISECONDS.toSeconds(tempStopTime - tempStartTime);
        System.out.println("Job Completed in " + tempelapsedTime + " seconds");
        System.out.println("------------------------------------------------------------------------------------------------\n");
        System.out.println("Sleeping for " + sleeptime + " Minutes.\n");
        TimeUnit.MINUTES.sleep(sleeptime);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
    service.logout();
    
    long stopTime = System.currentTimeMillis();
    long elapsedTime = TimeUnit.MILLISECONDS.toMinutes(stopTime - startTime);
    
    System.out.println("Total Time of Execution : " + elapsedTime + "Min");
  }

}
