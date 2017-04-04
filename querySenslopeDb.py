
#import MySQLdb
import ConfigParser
import pandas.io.sql as psql
import platform

curOS = platform.system()

if curOS == "Windows":
    import MySQLdb as mysqlDriver
elif curOS == "Linux":
    import pymysql as mysqlDriver

def PrintOut(line):
    if printtostdout:
        print line

def SenslopeDBConnect(nameDB):
    while True:
        try:
            db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb, db=nameDB)
            cur = db.cursor()
            return db, cur
        except mysqlDriver.OperationalError:
            print '.',

#Check if table exists
#   Returns true if table exists
def DoesTableExist(table_name):
    db, cur = SenslopeDBConnect(Namedb)
    cur.execute("use "+ Namedb)
    cur.execute("SHOW TABLES LIKE '%s'" %table_name)

    if cur.rowcount > 0:
        db.close()
        return True
    else:
        db.close()
        return False

def GetLatestTimestamp(nameDb, table):
    db = mysqlDriver.connect(host = Hostdb, user = Userdb, passwd = Passdb)
    cur = db.cursor()
    #cur.execute("CREATE DATABASE IF NOT EXISTS %s" %nameDB)
    try:
        cur.execute("select max(timestamp) from %s.%s" %(nameDb,table))
    except:
        print "Error in getting maximum timestamp"

    a = cur.fetchall()
    if a:
        return a[0][0]
    else: 
        return ''
		
#GetDBResultset(query): executes a mysql like code "query"
#    Parameters:
#        query: str
#             mysql like query code
#    Returns:
#        resultset: str
#             result value of the query made
def GetDBResultset(query):
    a = ''
    try:
        db, cur = SenslopeDBConnect(Namedb)

        a = cur.execute(query)

        db.close()
    except:
        PrintOut("Exception detected")

    if a:
        return cur.fetchall()
    else:
        return ""
        
#execute query without expecting a return
#used different name
def ExecuteQuery(query):
    GetDBResultset(query)
        
#GetDBDataFrame(query): queries a specific sensor data table and returns it as
#    a python dataframe format
#    Parameters:
#        query: str
#            mysql like query code
#    Returns:
#        df: dataframe object
#            dataframe object of the result set
def GetDBDataFrame(query):
    try:
        db, cur = SenslopeDBConnect(Namedb)
        df = psql.read_sql(query, db)
        # df.columns = ['ts','id','x','y','z','m']
        # change ts column to datetime
        # df.ts = pd.to_datetime(df.ts)

        db.close()
        return df
    except KeyboardInterrupt:
        PrintOut("Exception detected in accessing database")
        
#Push a dataframe object into a table
def PushDBDataFrame(df,table_name):     
    db, cur = SenslopeDBConnect(Namedb)

    df.to_sql(con=db, name=table_name, if_exists='append', flavor='mysql')
    db.commit()
    db.close()


# import values from config file
configFile = "server-config.txt"
cfg = ConfigParser.ConfigParser()

try:
    cfg.read(configFile)
    
    DBIOSect = "DB I/O"
    Hostdb = cfg.get(DBIOSect,'Hostdb')
    Userdb = cfg.get(DBIOSect,'Userdb')
    Passdb = cfg.get(DBIOSect,'Passdb')
    Namedb = cfg.get(DBIOSect,'Namedb')
    NamedbPurged = cfg.get(DBIOSect,'NamedbPurged')
    printtostdout = cfg.getboolean(DBIOSect,'Printtostdout')
    
    valueSect = 'Value Limits'
    xlim = cfg.get(valueSect,'xlim')
    ylim = cfg.get(valueSect,'ylim')
    zlim = cfg.get(valueSect,'zlim')
    xmax = cfg.get(valueSect,'xmax')
    mlowlim = cfg.get(valueSect,'mlowlim')
    muplim = cfg.get(valueSect,'muplim')
    islimval = cfg.getboolean(valueSect,'LimitValues')
    
except:
    #default values are used for missing configuration files or for cases when
    #sensitive info like db access credentials must not be viewed using a browser
    #print "No file named: %s. Trying Default Configuration" % (configFile)
    Hostdb = "127.0.0.1"    
    Hostdb = "192.168.1.102"
    Userdb = "root"
    Passdb = "senslope"
    Namedb = "senslopedb"
    NamedbPurged = "senslopedb_purged"
    printtostdout = False
    
    xlim = 100
    ylim = 1126
    zlim = 1126
    xmax = 1200
    mlowlim = 2000
    muplim = 4000
    islimval = True   






