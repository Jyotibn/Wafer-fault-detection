import csv
import os

from Application_logging.logger import App_Logger
import mysql.connector as conn

class DbOperation:

    def __init__(self):
        #give the database path in the Path variable
        #self.Path="training database path"
        self.GoodFilePath="C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Good_Raw"
        self.BadFilePath = "C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw"
        self.logger=App_Logger()

    def CreateDatabaseConection(self,DatabaseName):

        '''
                              Method Name:CreateDatabaseConection
                              Description:It creates a Database of given Database name ,if already exists then opens the existing database
                             return:connection string,cursor
                             on Failure:raise Exception
                             Written by:Jyoti Malik
                             version:1.0
                             Revision:None

                                     '''

        try:
            # building the connection
            mydb=conn.connect(host='localhost',user="root",passwd="qweasdzxc@12")
            cursor=mydb.cursor()                                                     # building the cursor
            query="show databases"                                                   # query for showing all databases name
            cursor.execute(query)                                                    # executing the query
            dbNameList=cursor.fetchall()                                             # dfNameList will contain a list of name of databases
            # open the Database if the DatabaseName is present in the list
            if (DatabaseName,) in dbNameList:
                query="Use "+DatabaseName
                cursor.execute(query)
                file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DatabaseConnectionLog.txt",'a+')
                self.logger.log(file,"successfully opened the {} database".format(DatabaseName))
                file.close()
            else:
                query="Create Database "+DatabaseName
                cursor.execute(query)
                mydb.commit()
                file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DatabaseConnectionLog.txt", 'a+')
                self.logger.log(file, "successfully created the {} database".format(DatabaseName))
                file.close()

        except ConnectionError:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DatabaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Error occurred while connecting the database :%s"%ConnectionError)
            file.close()
            raise ConnectionError
        return mydb,cursor







    def CreateTable(self,DatabaseName,column_names):
        '''
                                      Method Name:CreateTable
                                      Description:It creates a Table in  given Database name ,if already exists then Alter the existing Table
                                     return:None
                                     on Failure:raise Exception
                                     Written by:Jyoti Malik
                                     version:1.0
                                     Revision:None

                                             '''

        try:
            connection,cursor=self.CreateDatabaseConection(DatabaseName)    # this function will return the connection string and the  cursor
            for key in column_names.keys:                 # looping over all the names of columns from schema
                type=column_names[key]                    # type=datatype of column name --value of keys from schema
                # if table already exixts then alter it
                try:
                    query="ALTER TABLE Good_Raw_Data ADD COLUMN '{column_name}' {datatype}".format(column_name=key,datatype=type)
                    cursor.execute(query)
                # Create Table Good_Raw_Data if does not exist with key as col name and type as datatype
                except:
                    query = "CREATE TABLE Good_Raw_Data ('{column_name}'{datatype})".format(column_name=key,datatype=type)
                    cursor.execute(query)
            # closing the database connection
            connection.close()

            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DbTableCreatingLog.txt.txt", 'a+')
            self.logger.log(file, "Table created successfully")
            file.close()

            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DatabaseConnectionLog.txt", 'a+')
            self.logger.log(file, "{} Database closed successfully".format(DatabaseName))
            file.close()

        except Exception as e:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DbTableCreatingLog.txt.txt", 'a+')
            self.logger.log(file, "Error occurred while creating the table :%s"%e)
            file.close()
            raise Exception

    def InsertIntoTableGoodData(self,DatabaseName):
        '''
                                  Method Name:InsertIntoTableGoodData
                                  Description:It inserts the data into the Good_Raw_Data table ,if the file is not appropriate then
                                             move it to the Bad_Raw folder
                                 return:None
                                 on Failure:raise Exception
                                 Written by:Jyoti Malik
                                 version:1.0
                                 Revision:None

                                                     '''

        connection,cursor=self.CreateDatabaseConection(DatabaseName)       # this fun will return connection string and the cursor
        # storing the Good_Raw folder and Bad_Raw folder path
        GoodDataPAth=self.GoodFilePath
        BadDataPath=self.BadFilePath
        #onlyfiles will contain a list of directories stored in the Good_Raw folder
        onlyfiles=[file for file in os.listdir(GoodDataPAth)]
        log_file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/DbInsertingLog.txt",'a+')
        for file in onlyfiles:                 # looping over all the files in the onlyfiles list
            try:
                with open(GoodDataPAth+'/'+file,"r") as f:      # opening the file
                    next(f)
                    reader=csv.reader(f,delimiter='\n')          # reading each line seperated by '\n' i.e new line charracter
                for lines in enumerate(reader):                  # giving index to each line as 0,1,2,etc
                    for list_ in lines[1]:                       # lines[0]=indexs,lines[1]=content
                        try:
                            # inserting the data into the table Good_Raw_Data
                            cursor.execute("INSERT INTO  Good_Raw_Data VALUES({values})".format(values=list_))
                            self.logger.log(log_file,"%s File loaded successfully into the database"%file)
                            connection.commit()
                        except Exception as e:
                            raise e
            except Exception as e:
                connection.rollback()
                self.logger.log(log_file,"Error while creating table :%s"%e)
                shutil.move(GoodDataPAth+'/'+file,BadDataPath)
                self.logger.log(log_file, "File moved to Bad Raw folder:%s"%file)
                log_file.close()
                connection.close()
        connection.close()
        log_file.close()

    def SelectDataFromTable(self,DatabaseName):
        '''
                                          Method Name:SelectDataFromTable
                                          Description:It will extract the data from the table and convert it into the
                                                    csv file and store it in Input.csv file inside TrainingFileFromDb folder
                                         return:None
                                         on Failure:raise Exception
                                         Written by:Jyoti Malik
                                         version:1.0
                                         Revision:None

                                                             '''
        #path for csv file to be stored
        self.fileFromDb="C:/Users/malik/PycharmProjects/wafer_project/TrainingFileFromDb/"
        self.Inputfile="C:/Users/malik/PycharmProjects/wafer_project/TrainingFileFromDb/InputFile.csv"
        log_file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/ExportToCsv.txt", 'a+')
        try:
            # extracting the data from Good_Raw_Data table
            connection, cursor = self.CreateDatabaseConection(DatabaseName)
            query="SELECT * FROM Good_Raw_Data"
            cursor.execute(query)
            result=cursor.fetchall()                               # result=rows
            header=[i[0] for i in cursor.description]              # header=column names

            # make the directory for storing the file if not made
            if os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)
            # creating the csv file with  name stored in InputFile variable
            csvFile=csv.writer(open(self.fileFromDb+self.Inputfile,'w',newline=' '),delimiter=',',lineterminator='\r\n',quoting=csv.QUOTE_ALL,escapechar='\\')

            csvFile.writerow(header)            # column names
            csvFile.writerows(result)           # rows

            self.logger.log(log_file,"File exported successfully!!!")
            log_file.close()
        except Exception as e:

            self.logger.log(log_file,"Error occurred while exporting the file:%s"%e)
            log_file.close()
            raise e























