package com.anagarle.connector.splunk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.splunk.HttpService;
import com.splunk.Job;
import com.splunk.SSLSecurityProtocol;
import com.splunk.SavedSearch;
import com.splunk.SavedSearchCollection;
import com.splunk.Service;
import com.splunk.ServiceArgs;

public class SplunkSearchOTP
{

	private static Job getSplunkService(String hostname,String username,String savedSearchValue,String appname,String password,int port) {

		try {
			
			Map<String, Object> args4 = new HashMap<String, Object>();
			args4.put("host", hostname);
			args4.put("port", port);
			args4.put("schema", "https");

			HttpService.setSslSecurityProtocol(SSLSecurityProtocol.TLSv1_2);
			Service service = new Service(args4);
			if(password.length()>0)
				service.login(username, password);
			else 
				service.login(username,"");

			ServiceArgs namespace = new ServiceArgs();
			namespace.setApp(appname);
			namespace.setOwner(username);
			SavedSearchCollection savedSearches = service.getSavedSearches(namespace);

			SavedSearch savedSearch = savedSearches.get(savedSearchValue);
			
			if (savedSearch == null) {
				System.out.println("Saved search " + savedSearchValue + "not present in splunk");
				return null;
			}

			Job job;
			job = savedSearch.dispatch();
			
			long startTime = System.currentTimeMillis();
			
			while (!job.isDone()) {
            	System.out.println("Waiting for job to complete. Time Completed:" + TimeUnit.MILLISECONDS.toMinutes(System.currentTimeMillis() - startTime));
				Thread.sleep(2000);
				job.refresh();
			}
			return job;

		} catch (Exception ex) {
			ex.printStackTrace();
		} 
		return null;

	}

	public static void main(String[] args)
	{
		
		long startTime = System.currentTimeMillis();
		
		String line="";
	    StringBuilder sb = new StringBuilder();
	    
	    int COUNT = 10000;

		InputStream inputProperties = null;
		Properties prop = new Properties();
		try {
				inputProperties = new FileInputStream("splunkReader.properties");
				prop.load(inputProperties);
			} catch (IOException e) {
				e.printStackTrace();
			} 
		
		String hostname = prop.getProperty("hostname");
		String username = prop.getProperty("username");
		String searchname = prop.getProperty("searchname");
		String outfile = prop.getProperty("outputfilepath");
		String appname = prop.getProperty("appname");
		String password = prop.getProperty("password");
		String outputfilename = prop.getProperty("outputfilename");
		int port = Integer.parseInt(prop.getProperty("port"));
		
		System.out.println("Hostname : " + hostname );
		System.out.println("username : " + username );
		System.out.println("searchname : " + searchname );
		System.out.println("appname : " + appname );
		System.out.println("port : " + port );

		Job job = getSplunkService(hostname,username,searchname,appname,password,port);
		
		try{
			
			Integer offset = 0;
			boolean recordsAvailable = true;
			int totalResoutCount = job.getResultCount();
			
			System.out.println("------Total Results for Search " + totalResoutCount);
			
			int countLines = 0;
            System.out.println("Writing Lines to file");
            while (recordsAvailable) {

                //InputStream stream = service.export(search, args);

                Map<String,Object> jobMap = new HashMap<String,Object>();
                jobMap.put("count", COUNT);
                jobMap.put("output_mode", "csv");
                jobMap.put("offset", offset);
                InputStream stream = job.getResults(jobMap);
                BufferedReader reader = new BufferedReader(new InputStreamReader(stream));

                while ((line = reader.readLine()) != null) {
                    countLines++;
                    if(!line.contains("_raw"))
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
                offset += COUNT;
            }
            
            String fileName = new SimpleDateFormat("yyyyMMddHHmm'.txt'").format(new Date());
			
            System.out.println("Writing Complete. The File is located at " + outfile + outputfilename + fileName);
            PrintWriter Writer = new PrintWriter(outfile + outputfilename + fileName);
			Writer.println(sb.toString().trim());
			sb.setLength(0);
			Writer.close();
			
			print(outfile + outputfilename + "-UTF8-" + fileName, sb.toString().trim());
			
			long stopTime = System.currentTimeMillis();
		    long elapsedTime = stopTime - startTime;
		      
		    System.out.println("Time of Execution : " + elapsedTime);
		    System.out.println("----------------------------------");

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
	
	public static void print(String filename, String sb) {
		Writer out;
		try {
				out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), "UTF-8"));
				out.append(sb);
				out.close();
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
