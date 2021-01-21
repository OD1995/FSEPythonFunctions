import logging
import pyodbc
import os

def run_sql_commmand(query,server,database):

    ## Get information used to create connection string
    if server == "nonDashboard":
        username = 'matt.shepherd'
        password = os.getenv("msPassword")
        driver = '{ODBC Driver 17 for SQL Server}'
        serverLong = os.getenv(server)
    else:
        raise ValueError(f"This function is not yet developed for server = `{server}`")
    ## Create connection string
    connectionString = f'DRIVER={driver};SERVER={serverLong};PORT=1433;DATABASE={database};UID={username};PWD={password}'
    ## Execute query
    with pyodbc.connect(connectionString) as conn:
        with conn.cursor() as cursor:
            logging.info("About to execute query below")
            logging.info(query)
            cursor.execute(query)
            logging.info("Query executed")

def time2secs(x):
    """
    Input: datetime.time object
    Output: number of seconds in that object - int
        e.g datetime.time(0, 30) -> 1800
    NB - Microseconds (etc) are ignored
    """
    hour_seconds = x.hour * 3600
    minute_seconds = x.minute * 60
    second_seconds = x.second * 1
    
    return hour_seconds + minute_seconds + second_seconds

def create_insert_query(df,columnDict,sqlTableName):
    """
    Inputs: - df - pandas.DataFrame
            - columnDict - dict - keys - column names
                                - vals - column (rough) SQL data types (as strings)
            - sqlTableName - str
    Output: - SQL insert query - str
    """
    ## Create column list string
    columnsListStr = "[" + "],[".join(columnDict.keys()) + "]"
    ## Convert df into a string of rows to upload
    stringRows = rows_to_strings(df,columnDict)
    
    
    Q = f"""
INSERT INTO {sqlTableName}
({columnsListStr})
VALUES {','.join(stringRows)}
    """
    return Q

def rows_to_strings(df,columnDict):
    
    listToReturn = []
    
    ## Loop throw the rows (as dicts)
    for row_dict in df.to_dict(orient="records"):
        ## Create list of strings formatted in the way SQL expects them
        ##    based on their SQL data type
        rowList = [sqlise(
                    _val_=row_dict[colName],
                    _format_=colType
                            )
                    for colName,colType in columnDict.items()]
        ## Create SQL ready string out of the list
        stringRow = "\n(" + ",".join(rowList) + ")"
        ## Add string to list
        listToReturn.append(stringRow)
        
    return listToReturn

            
def sqlise(_val_,_format_):
    if _val_ is None:
        return "NULL"
    elif _format_ == "str":
        return "'" + _val_.replace("'","''") + "'"
    elif _format_ == "DateTime":
        ## datetime gives 6 microsecond DPs, SQL only takes 3
        return "'" + _val_.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "'"
    else:
        raise ValueError(f"Data type `{_format_}` not expected")
        
        
def create_removeDuplicates_query(columnDict,sqlTableName,primaryKeyColName):
    
    ## Create column list string
    columnsListStr = "[" + "],[".join(columnDict.keys()) + "]"
    Q = f"""
    WITH ToDelete AS (
       SELECT ROW_NUMBER() OVER
           (PARTITION BY {columnsListStr} ORDER BY {primaryKeyColName}) AS rn
       FROM {sqlTableName}
    )
    DELETE FROM ToDelete
    WHERE rn > 1
    """
    
    return Q