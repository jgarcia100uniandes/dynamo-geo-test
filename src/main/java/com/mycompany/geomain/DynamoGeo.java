/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.geomain;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.geo.GeoDataManager;
import com.amazonaws.geo.GeoDataManagerConfiguration;
import com.amazonaws.geo.model.GeoPoint;
import com.amazonaws.geo.model.PutPointRequest;
import com.amazonaws.geo.model.QueryRadiusRequest;
import com.amazonaws.geo.model.QueryRadiusResult;
import com.amazonaws.geo.util.GeoTableUtil;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jair
 */
public class DynamoGeo {

    public static void main(String[] args) {
        //crearTabla();
        //insertarDatosEjemplo();
        //buscarPunto();

    }
    
    public static void crearTabla(){
        DynamoGeo s=new DynamoGeo();
        s.setupGeoDataManager();
        s.createTable();
    }
    
    public static void insertarDatosEjemplo(){
        DynamoGeo s = new DynamoGeo();
        s.setupGeoDataManager();
        try {
            s.insertData();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DynamoGeo.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public static void buscarPunto(){
        DynamoGeo s = new DynamoGeo();
        s.setupGeoDataManager();
        
        DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
	Date date = new Date();
	System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
        //Search 48.65487	-119.64118 1 km a la redonda
        GeoPoint centerPoint = new GeoPoint(48.65487, -119.64118);
        QueryRadiusRequest queryRadiusRequest = new QueryRadiusRequest(centerPoint, 1000);
        QueryRadiusResult result = s.geoDataManager.queryRadius(queryRadiusRequest);
        GeoDataManagerConfiguration config = s.geoDataManager.getGeoDataManagerConfiguration();

        for (Map<String, AttributeValue> item : result.getItem()) {
            System.out.println(item);
            
            String geoJsonString = item.get(config.getGeoJsonAttributeName()).getS();
            System.out.println(geoJsonString);
        }
        date = new Date();
	System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
        
    }

    private GeoDataManager geoDataManager;

    public GeoDataManager getGeoDataManager() {
        return this.geoDataManager;
    }

    /**/
    public void setupTable() {
        setupGeoDataManager();

        GeoDataManagerConfiguration config = geoDataManager.getGeoDataManagerConfiguration();
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(config.getTableName());

    }

    /*
    
     */
    public void insertData() throws FileNotFoundException {
        File initialFile = new File("/home/jair/Downloads/park.tsv");
        InputStream fis = new FileInputStream(initialFile);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
        String line;
        int i=0;

        try {
            while ((line = br.readLine()) != null) {
                System.out.println(i+" "+line);
                String[] columns = line.split("\t");
                String parkId = columns[0];
                String parkName = columns[1];
                double latitude = Double.parseDouble(columns[2]);
                double longitude = Double.parseDouble(columns[3]);

                GeoPoint geoPoint = new GeoPoint(latitude, longitude);
                AttributeValue rangeKeyValue = new AttributeValue().withS(parkId);
                AttributeValue parkNameValue = new AttributeValue().withS(parkName);

                PutPointRequest putPointRequest = new PutPointRequest(geoPoint, rangeKeyValue);
                putPointRequest.getPutItemRequest().getItem().put("parkName", parkNameValue);

                geoDataManager.putPoint(putPointRequest);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                br.close();
                fis.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }


    public void createTable() {
        GeoDataManagerConfiguration config = geoDataManager.getGeoDataManagerConfiguration();

        CreateTableRequest createTableRequest = GeoTableUtil.getCreateTableRequest(config);
        config.getDynamoDBClient().createTable(createTableRequest);
    }

    public synchronized void setupGeoDataManager() {
        String tableName = "uniandes-geotest2";
        System.out.println("Tabla: "+tableName);

        Region region = Region.getRegion(Regions.US_EAST_1);
        ClientConfiguration clientConfiguration = new ClientConfiguration().withMaxErrorRetry(20);

        AmazonDynamoDBClient ddb = new AmazonDynamoDBClient(clientConfiguration);
        ddb.setRegion(region);

        GeoDataManagerConfiguration config = new GeoDataManagerConfiguration(ddb, tableName);
        geoDataManager = new GeoDataManager(config);

    }
}
