package util;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by root on 30/05/17.
 */
public class HBaseClient {
    /* Configuration Parameters */
    private static final String ZOOKEEPER_HOST = "localhost";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String HBASE_MASTER  = "localhost:60000";

    private static final boolean DEBUG = true;

    public HBaseClient(){}


    private enum ALTER_COLUMN_FAMILY {
        ADD, DELETE
    }

    private Connection connection = null;

    private static byte[] b(String s){
        return Bytes.toBytes(s);
    }

    /**
     * Create a connection with HBase
     * @return
     * @throws IOException
     * @throws ServiceException
     */
    public Connection getConnection() throws IOException, ServiceException {

        if (!(connection == null || connection.isClosed() || connection.isAborted()))
            return connection;

        if (!DEBUG || true)
            Logger.getRootLogger().setLevel(Level.ERROR);

        Configuration conf  = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ZOOKEEPER_HOST);
        conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        conf.set("hbase.master", HBASE_MASTER);

        /* Check configuration */
        HBaseAdmin.checkHBaseAvailable(conf);

        if (DEBUG)
            System.out.println("HBase is running!");

        this.connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    /* *******************************************************************************
    *  Cane administration
    * ******************************************************************************* */

    /**
     * Create a new table named tableName, with the specified columnFamilies
     *
     * @param tableName
     * @param columnFamilies
     * @return
     */
    public boolean createTable(String tableName, String... columnFamilies) {

        try {
            System.out.println("Dentro createTable");

            Admin admin = getConnection().getAdmin();
            HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

            for (String columnFamily : columnFamilies) {
                tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }

            admin.createTable(tableDescriptor);
            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;

    }

    /**
     * Retrieve the list of tables on the HBase data store
     *
     * @return
     */
    public List<String> listTables(){

        List<String> tables = new ArrayList<>();

        try {
            Admin admin = getConnection().getAdmin();
            HTableDescriptor[] tableDescriptor = admin.listTables();

            for (int i=0; i<tableDescriptor.length;i++ ){
                String tableName = tableDescriptor[i].getNameAsString();

                tables.add(tableName);
            }

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return tables;

    }

    /**
     * Describe the table.
     * This function returns a serialization of the columnFamilies of the table.
     *
     * @param table name of the table to describe
     * @return comma separated list of columns
     */
    public String describeTable(String table){

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(tableName);

            String columnFamilies = "";
            HColumnDescriptor[] columnDescriptors = tableDescriptor.getColumnFamilies();

            for (int j = 0; j < columnDescriptors.length; j++){
                columnFamilies += columnDescriptors[j].getNameAsString();

                if (j < columnDescriptors.length - 1)
                    columnFamilies += ", ";
            }

            return tableName + ": " + columnFamilies;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return null;

    }

    /**
     * Add or remove a column family from a table.
     *
     * @param operation add or remove
     * @param table     tablename
     * @param columnFamily  name of the column family to add or remove
     * @return true if the operation is completed successfully
     */
    public boolean alterColumnFamily(ALTER_COLUMN_FAMILY operation, String table, String columnFamily){

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);


            switch (operation){
                case ADD:
                    HColumnDescriptor columnDescriptor = new HColumnDescriptor(columnFamily);
                    admin.addColumn(tableName, columnDescriptor);
                    break;
                case DELETE:
                    admin.deleteColumn(tableName, Bytes.toBytes(columnFamily));
                    break;
                default:
                    return false;
            }

            return true;

        } catch (IOException | ServiceException e) {
//            e.printStackTrace();
        }

        return false;

    }

    /**
     * Check if a table exists
     * @param table table name
     * @return  true if a table exists
     */
    public boolean exists(String table){

        try {

            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);
            return admin.tableExists(tableName);

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;

    }

    /**
     * Drop a table and all its content
     *
     * @param table table to be deleted
     * @return true if a table has been deleted
     */
    public boolean dropTable(String table) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);

            // To delete a table or change its settings, you need to first disable the table
            admin.disableTable(tableName);

            // Delete the table
            admin.deleteTable(tableName);

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * Erase the content of the table.
     *
     * @param table     table name
     * @param preserveSplits    remove the split from region servers
     * @return          true if the table has been erased
     */
    public boolean truncateTable(String table, boolean preserveSplits) {

        try {
            Admin admin = getConnection().getAdmin();
            TableName tableName = TableName.valueOf(table);

            // To delete a table or change its settings, you need to first disable the table
            admin.disableTable(tableName);

            // Truncate the table
            admin.truncateTable(tableName, preserveSplits);

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /* *******************************************************************************
    *  CRUD operations
    * ******************************************************************************* */

    /**
     * Insert a new row in a table.
     *
     * To put a record, this method should be executed by providing triples of
     *  columnFamily, column, value
     *
     * If a columnFamily:column already exists, the value is updated.
     * Values are stacked according to their timestamp.
     *
     * @param table     table name
     * @param rowKey    row name
     * @param columns   columnFamily, column, value
     * @return          true if the record is inserted
     */
    public boolean put(String table, String rowKey, String... columns){
            System.out.println("table"+table);
        System.out.println("rowkey"+rowKey);
        System.out.println("colums"+columns.length);
        if (columns == null || (columns.length % 3 != 0)) {
            System.out.println("Dentro put CHE RITORNA FALSE");
            // Invalid usage of the function; columns should contain 3-ple in the
            // following format:
            // - columnFamily
            // - column
            // - value
            return false;
        }

        try {
                System.out.println("Dentro put"+columns[2]);
            Table hTable = getConnection().getTable(TableName.valueOf(table));

            Put p = new Put(b(rowKey));

            for (int i = 0; i < (columns.length / 3); i++){

                String columnFamily = columns[i * 3];
                String column       = columns[i * 3 + 1];
                String value        = columns[i * 3 + 2];

                p.addColumn(b(columnFamily), b(column), b(value));

            }

            // Saving the put Instance to the HTable.
            hTable.put(p);

            // closing HTable
            hTable.close();

            return true;
        } catch (IOException | ServiceException e) {
         e.printStackTrace();
        }

        return false;
    }


    /**
     *
     * To get everything for a row, instantiate a Get object with the row to get.
     *
     * To further narrow the scope of what to Get, use the methods below.
     * - To get all columns from specific families, execute addFamily for each family to retrieve.
     * - To get specific columns, execute addColumn for each column to retrieve.
     * - To only retrieve columns within a specific range of version timestamps, execute setTimeRange.
     * - To only retrieve columns with a specific timestamp, execute setTimestamp.
     * - To limit the number of versions of each column to be returned, execute setMaxVersions.
     *
     * @param table     table to query
     * @param rowKey    row key to retrieve
     * @param columnFamily  columnFamily to retrieve
     * @param column        column to retrieve
     * @return          the value stored in the rowKey:columnFamily:column
     */
    public String get(String table, String rowKey, String columnFamily, String column){

        try {

            // Instantiating HTable class
            Table hTable = getConnection().getTable(TableName.valueOf(table));

            Get g = new Get(b(rowKey));

            // Narrowing the scope
            // g.addFamily(b(columnFamily));
            // g.addColumn(b(columnFamily), b(column));

            // Reading the data
            Result result = hTable.get(g);

            byte [] value = result.getValue(b(columnFamily), b(column));
            return Bytes.toString(value);

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     * Delete a row (or columnFamily, or column) from a table.
     *
     * To delete all column of a columnFamily, set column to null
     * To delete all columnFamilies a rowKey,  set columnFamily to null
     *
     * @param table         table of interest
     * @param rowKey        row key to alter
     * @param columnFamily  columnFamily to alter or delete
     * @param column        column to alter or delete
     * @return              true if the value has been deleted
     */
    public boolean delete(String table, String rowKey, String columnFamily, String column){

        try {

            // Instantiating HTable class
            Table hTable = getConnection().getTable(TableName.valueOf(table));

            // Instantiating Delete class
            Delete delete = new Delete(b(rowKey));

            if (columnFamily != null && column != null)
                delete.addColumn(b(columnFamily), b(column));

            else if (columnFamily != null)
                delete.addFamily(b(columnFamily));

            // deleting the data
            hTable.delete(delete);

            // closing the HTable object
            hTable.close();

            return true;

        } catch (IOException | ServiceException e) {
            e.printStackTrace();
        }

        return false;
    }


    /**
     * Scan the content of the table.
     *
     * @param table            table to scan
     * @param columnFamily     columnFamily to scan
     * @param column           column to scan
     * @throws IOException
     * @throws ServiceException
     */
    public void scanTable(String table, String columnFamily, String column) throws IOException, ServiceException {

        Table products = getConnection().getTable(TableName.valueOf(table));

        Scan scan = new Scan();

        if (columnFamily != null && column != null)
            scan.addColumn(b(columnFamily), b(column));

        else if (columnFamily != null)
            scan.addFamily(b(columnFamily));

        ResultScanner scanner = products.getScanner(scan);

        // Reading values from scan result
        for (Result result = scanner.next(); result != null; result = scanner.next()){
            System.out.println("Found row : " + result);
        }

        scanner.close();

    }

    public static void tableManagementOperations(HBaseClient hbc) throws IOException, ServiceException {

          /***********************************************************
         *  Table Management: Create, Alter, Describe, Delete
         * ********************************************************** */
        //  Create
        if (!hbc.exists("products")){
            System.out.println("Creating table...");
            hbc.createTable("products", "fam1", "fam2", "fam3");
        }

        // List tables
        System.out.println("Listing table...");
        System.out.println(hbc.listTables());

        //  Alter
        System.out.println("Altering tables...");
        System.out.println(hbc.describeTable("products"));
        hbc.alterColumnFamily(ALTER_COLUMN_FAMILY.DELETE, "products", "fam3");
        System.out.println(hbc.describeTable("products"));
        hbc.alterColumnFamily(ALTER_COLUMN_FAMILY.ADD, "products", "fam4");
        System.out.println(hbc.describeTable("products"));

        //  Scan table
        System.out.println("Scanning table...");
        hbc.scanTable("products", null, null);

        //  Drop table
        System.out.println("Deleting table...");
        hbc.dropTable("products");

    }
   /* public static void simpleDataManipulationOperations(HBaseClient hbc) throws IOException, ServiceException {

        *//* **********************************************************
         *  Data Management: Put, Get, Delete, Scan, Truncate
         * ********************************************************** *//*

        *//* Create *//*
        System.out.println("\n******************************************************** \n");
        if (!hbc.exists("products")){
            System.out.println("Creating table...");
            hbc.createTable("products", "fam1", "fam2", "fam3");
        }

        *//* Put *//*
        System.out.println("\n ------------------\n");
        System.out.println("Adding value: row1:fam1:col1:val1");
        hbc.put("products", "row1", "fam1", "col1", "val1");
        System.out.println("Adding value: row1:fam1:col2:val2");
        hbc.put("products", "row1", "fam1", "col2", "val2");

        *//* Get *//*
        String v1 = hbc.get("products", "row1", "fam1", "col1");
        String v2 = hbc.get("products", "row1", "fam1", "col2");
        System.out.println("Retrieving values : " + v1 + "; " + v2);
        System.out.println("\n ------------------\n");


        *//* Update a row (the row key is unique) *//*
        System.out.println("Updating value: row1:fam1:col1:val1 to row1:fam1:col1:val3");
        hbc.put("products", "row1", "fam1", "col1", "val3");

        v1 = hbc.get("products", "row1", "fam1", "col1");
        v2 = hbc.get("products", "row1", "fam1", "col2");
        System.out.println("Retrieving values : " + v1 + "; " + v2);
        System.out.println("\n ------------------\n");


        *//* Delete a column (with a previous value stored within) *//*
        System.out.println("Deleting value: row1:fam1:col1");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        System.out.println("Retrieving value row1:fam1:col1 (pre-delete): " + v1);
        hbc.delete("products", "row1", "fam1", "col1");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        System.out.println("Retrieving value row1:fam1:col1  (post-1st-delete): " + v1);
        hbc.delete("products", "row1", "fam1", "col1");
        v1 = hbc.get("products", "row1", "fam1", "col1");
        System.out.println("Retrieving value row1:fam1:col1  (post-2nd-delete): " + v1);
        System.out.println("\n ------------------\n");

        *//* Scanning table *//*
        System.out.println("Scanning table...");
        hbc.scanTable("products", null, null);

        *//* Truncate *//*
        System.out.println("Truncating data... ");
        hbc.truncateTable("products", true);
        System.out.println("\n ------------------\n");

    }

    public static void otherDataManipulationOperations(HBaseClient hbc) throws IOException, ServiceException {

        *//* **********************************************************
         *  Data Management: Special Cases of Put, Delete
         * ********************************************************** *//*

        *//* Data manipulation, special cases *//*

        *//* try to insert a row for a not existing column family *//*
        System.out.println("Insert a key with a not existing column family");
        boolean res = hbc.put("products", "row2", "fam100", "col1", "val1");
        System.out.println(" result: " + res);
        System.out.println("\n ------------------\n");


        *//* Delete: different columns, same column family *//*
        System.out.println(" # Inserting row2:fam1:col1 and row2:fam1:col2 ");
        hbc.put("products", "row2", "fam1", "col1", "val1");
        hbc.put("products", "row2", "fam1", "col2", "val2");
        System.out.println("Deleting data of different columns, but same column family... ");
        hbc.delete("products", "row2", "fam1", "col1");
        String v1 = hbc.get("products", "row2", "fam1", "col1");
        String v2 = hbc.get("products", "row2", "fam1", "col2");
        System.out.println("Retrieving values (post-delete of col1): " + v1 + "; " + v2);

        System.out.println("\n ------------------\n");

        *//* Delete: column family *//*
        System.out.println(" # Inserting row2:fam1:col2 (same family of existing row)");
        hbc.put("products", "row2", "fam1", "col2", "val2");
        System.out.println(" # Inserting row2:fam2:col3 (same family of existing row)");
        hbc.put("products", "row2", "fam2", "col3", "val3");
        System.out.println("Deleting a column family for a data... ");
        hbc.delete("products", "fam1", "fam1", null);
        v1 = hbc.get("products", "row2", "fam1", "col1");
        v2 = hbc.get("products", "row2", "fam1", "col2");
        String v3 = hbc.get("products", "row2", "fam2", "col3");
        System.out.println("Retrieving values : " + v1 + "; " + v2 + "; " + v3);

        System.out.println("\n ------------------\n");


        *//* Delete: entire row key *//*
        System.out.println("Deleting the whole row (row2)... ");
        hbc.delete("products", "row2", null, null);
        v1 = hbc.get("products", "row2", "fam1", "col1");
        v2 = hbc.get("products", "row2", "fam1", "col2");
        v3 = hbc.get("products", "row2", "fam2", "col3");
        System.out.println("Retrieving values : " + v1 + "; " + v2 + "; " + v3);

    }
*/
    public static void main(String[] args) throws IOException, ServiceException {

        HBaseClient hbc = new HBaseClient();

        /* **********************************************************
         *  Table Management: Create, Alter, Describe, Delete
         * ********************************************************** */
        tableManagementOperations(hbc);

        /* **********************************************************
         *  Data Management: Put, Get, Delete, Scan, Truncate
         * ********************************************************** */
      //  simpleDataManipulationOperations(hbc);

        /* Data manipulation, special cases */
     //   otherDataManipulationOperations(hbc);

    }
}
