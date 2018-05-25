
# Export and Import LOB values

## Oracle PL/SQL solution to move LOB data with text files

## Why?

Because I did not find any useful and easy solution for exporting and importing LOB data.
 
The **EXP_IMP_LOB** package can export and import **CLOB, NCLOB, BLOB** type column data using simple SQL (text) files.

## How?

First of all install the package onto both source and target schemas.
To export run this select

    select * from table( EXP_IMP_LOB.EXPORT('table_name','lob_column_name','condition') );

where the *Table_Name* and *LOB_Column_Name* define the data column and the optional Condition defines the row or rows. 
If there is no condition, then every row data will be exported row by row.
    
Example:

    select * from table( EXP_IMP_LOB.EXPORT('person','image','id=103' ) );

Result:

    / ******************************************************
        TABLE  :PERSON
        COLUMN :IMAGE
        ROW    :103
    ****************************************************** /
    BEGIN
        EXP_IMP_LOB.IMPORT_NEW;
        EXP_IMP_LOB.IMPORT_APPEND ( 'FFD8FFE000104A464....23232323232');
        EXP_IMP_LOB.IMPORT_APPEND ( '32323232323232323....798999AA2A3');
        .........
        EXP_IMP_LOB.IMPORT_APPEND ( 'B2316524267279AA9....51401FFFD9');
        EXP_IMP_LOB.IMPORT_UPDATE ( 'PERSON','IMAGE','103' ); 
        COMMIT;
    END;
    /   
 
So, the export converts the binary data to 400 char length hexa strings and creates a script from it.
I used ..... to symbolize many chars, because that is only a sample above.
DO NOT ORDER THE RESULT!
To import, you only have to install the package onto the target schema too and run this script above in the target schema.
That's all.

...more:

* The source and target *table name, column name* must be the same!
* The Table (both source and target) must have Primary key and they must be identical.
* The EXPORT function can detect the primary key automatically. Theoretically it can manage composed keys too...
* The size of a hexa string is defined in G_LENGTH global variable. 200 chars means 400 hexa chars.
* The additional procedures:
* **IMPORT_NEW**: resets the package variables to prepare it to accept a new LOB
* **IMPORT_APPEND** : converts the hexa string to a binary data and append it the package variable
* **IMPORT_UPDATE** : updates the given table, row, column with the package variable
* **DIRECT_SQL** : executes the given SQL using the global LOB variable as a parameter. eg:     

    EXP_IMP_LOB.DIRECT_SQL( 'insert into ANY_TABLE ( ID, IMAGE ) values ( 123, :1 )' ); 
                    
