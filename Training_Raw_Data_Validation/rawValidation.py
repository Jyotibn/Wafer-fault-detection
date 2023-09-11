import os
import re
import shutil
import pandas as pd
import json
from datetime import datetime


from Application_logging.logger import App_Logger

class Raw_Data_Validation:
    def __init__(self,path):
        self.Batch_Directory=path
        self.schema_path= "C:/Users/malik/PycharmProjects/wafer_project/Schema_Training.json"
        self.logger=App_Logger()

    def Values_from_schema(self):
        '''
            Method Name:Values_from_schema
            Description:This method will extract all the information from the Schema_Training.json file
            on Failure:raise  ValueError,KeyError,Exception
            return: LengthOfDateStampFile,LengthOfTimeStampFile,NumberOfColumns,column_names

            Written By:Jyoti Malik
            Version:1.0
            Revision=None
        '''


        try:
            with open(self.schema_path,'r') as f:    # opening the Schema_Training.json file
                dict=json.load(f)                    # loading the entire json data into dictionary dict
                f.close()                            # close the Schema_Training.json file
            pattern=dict['SampleFileName']       # storing the value of SampleFileName of dict in pattern variable
            LengthOfDateStampFile=dict['LengthOfDateStampFile']        # storing the value of LengthOfDateStampFile of dict in LengthOfDateStampFile variable
            LengthOfTimeStampFile=dict['LengthOfTimeStampFile']        # storing the value of LengthOfTimeStampFile of dict in LengthOfTimeStampFile variable
            NumberOfColumns=dict['NumberOfColumns']                    # storing the value of NumberOfColumns of dict in NumberOfColumns variable
            column_names=dict['ColName']                               # storing the value of ColName of dict in column_names variable


                #writing the log into ValuesFromSchemaValidationLog.txt
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/ValuesFromSchemaValidationLog.txt",'a+')
            message="LengthOfDateStampFile=%s"%LengthOfDateStampFile,'\t\t' + "LengthOfTimeStampFile=%s"%LengthOfTimeStampFile,'\t\t'+"NumberOfColumns=%s"%NumberOfColumns+'\n'
            self.logger.log(file,message)
            file.close()


        except ValueError:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/ValuesFromSchemaValidationLog.txt",'a+')
            self.logger.log(file,"ValueError:Value Not found inside Schema_Training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/ValuesFromSchemaValidationLog.txt",'a+')
            self.logger.log(file, "KeyError:KeyValueError--Incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/ValuesFromSchemaValidationLog.txt",'a+')
            self.logger.log(file,str(e))
            file.close()
            raise e


        return LengthOfDateStampFile,LengthOfTimeStampFile,NumberOfColumns,column_names


    def ManualRegexCreation(self):


        regex="['Wafer']+['\_']+[\d_]+[\d]+\.csv"

        return regex

    def createDirectoryForGoodBadRawData(self):
        '''
              Method Name:createDirectoryForGoodBadRawData
              Description:It creates the directory to store good data and the bad data after validating the training data

              return: None
              written by:jyoti malik
              version:1.0
              revision:None
        '''



        try:
            path = os.path.join("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated","/Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated", "/Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file=open('C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt','a+')
            self.logger.log(file,"error while creating the directory %s:"%ex)
            file.close()
            raise OSError


    def deleteExistingBadDataTrainingFolder(self):
        '''
                     Method Name:deleteExistingBadDataTrainingFolder
                     Description:It deletes the existing Bad Raw directory

                     return: None
                     written by:jyoti malik
                     version:1.0
                     revision:None
               '''



        try:
            path="C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/"
            if os.path.isdir(path+'Bad_Raw/'):
                shutil.rmtree(path+'Bad_Raw/')
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt",'a+')
            self.logger.log(file,"Bad Raw directory deleted before starting the Validation")
            file.close()

        except OSError as s:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt",'a+')
            self.logger.log(file,"Error Ocured while deleting the directory: %s"%s)
            file.close()
            raise OSError






    def deleteExistingGoodDataTrainingFolder(self):
        '''
                             Method Name:deleteExistingGoodDataTrainingFolder
                             Description:It deletes the existing Good Raw directory

                             return: None
                             written by:jyoti malik
                             version:1.0
                             revision:None
                       '''


        try:
            path = "C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/"
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt", 'a+')
            self.logger.log(file, "Good Raw directory deleted before starting the Validation")
            file.close()

        except OSError as s:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error Occured while deleting the directory: %s" % s)
            file.close()
            raise OSError



    def MovingBadFilesToArchive(self):
        '''
                                     Method Name:MovingBadFilesToArchive
                                     Description:It moves the Bad files to the Archive folder and then deletes the
                                                 Bad_Raw folder
                                     On Failure:raise exception

                                     return: None
                                     written by:jyoti malik
                                     version:1.0
                                     revision:None
                               '''
        # storing the date,time in the date and time variable
        now=datetime.now()
        date=now.date()
        time=now.strftime("%H:%M:%S")
        try:
            # storing the path of Bad_Raw folder in the source folder
            source="C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw"
            if os.path.isdir(source):                 # if source is an existing directory then
                path="C:/Users/malik/PycharmProjects/wafer_project/TrainingArchiveBadData"    # stores the path of Archive folder into the Path variable
                if not os.path.isdir(path):           # if path is not an existing directory then
                    os.makedirs(path)                 # make the directory for path
                dest="C:/Users/malik/PycharmProjects/wafer_project/TrainingArchiveBadData/BadData_"+str(date)+"_"+str(time)  # dest =BadData_date_time folder path
                if not os.path.isdir(dest):           #if dest is not an existing directory then
                    os.makedirs(dest)                 # make the directory for dest
                files=os.listdir(source)              #files=list of all directories inside the source
                for f in files:                       # looping over all the files
                    if f not in os.listdir(dest):      # if file is not in the list of directory files of dest then
                        shutil.move(source+f,dest)     # move the file to dest directory
                log_file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt",'a+')
                self.logger.log(log_file,"Bad files moved to Archive")
                #removing the Bad_Raw folder
                path="C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw/"
                if os.path.isdir(path):
                    shutil.rmtree(path)
                self.logger.log(log_file, "Bad_Raw folder deleted successfully!!!")
                log_file.close()
        except Exception as e:
            log_file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/GeneralLog.txt", 'a+')
            self.logger.log(log_file, "Error occurred while moving  Bad files to Archive :%s"%e)
            log_file.close()
            raise e














    def ValidationFileNameRaw(self,regex,LengthOfDateStampFile,LengthOfTimeStampFile):
        '''
                   Method Name:ValidationFileNameRaw
                   Description:It validates the name of training file given on the basis of schema given in schemafile
                   regex id used for validation of name if name is found to be same then the file is moved to good data else bad data

                  return:
                  Written by:Jyoti Malik
                  version:1.0
                  Revision:None

              '''


        #deleting the existing folder if present
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()

        #create new directories
        self.createDirectoryForGoodBadRawData()   # it will create two directories one for good data and another for bad data
        onlyfiles=[f for f in os.listdir(self.Batch_Directory)]  # creating a list of all the files from Training_Batch_files

        try:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/NameValidationLog.txt",'a+') # opening a log file--NameValidationLog.txt
            for filename in onlyfiles:     # looping over each file of onlyfile list
                if re.match(regex,filename):    # if regex and filename matches then...
                    splitAtDot=re.split('.csv',filename)    # split the filename from .csv
                    splitAt_=re.split('_',splitAtDot[0])    # split the filename into wafer name,datestamp,timestamp on bases of _
                    if len(splitAt_[1])==LengthOfDateStampFile:     # if length of LengthOfDateStampFile matches
                        if len(splitAt_[2])==LengthOfTimeStampFile:  # if length of LengthOfTimeStampFile matches
                            #copying the filename to the good raw folder
                            shutil.copy("C:/Users/malik/PycharmProjects/wafer_project/Training_Batch_Files/"+filename,"C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Good_Raw")
                            self.logger.log(file,"Valid File name file moved to Good Raw folder:%s"%filename)

                        else:
                            # copying the filename to bad raw folder as length of LengthOfTimeStampFile does not matches

                            shutil.copy("C:/Users/malik/PycharmProjects/wafer_project/Training_Batch_Files/" + filename,"C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw")
                            self.logger.log(file, "Invalid File name ,File moved to Bad Raw Folder:%s" %filename)
                    else:
                        # copying the filename to bad raw folder as length of LengthOfDateStampFile does not matches
                        shutil.copy("C:/Users/malik/PycharmProjects/wafer_project/Training_Batch_Files/" + filename,"C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw")
                        self.logger.log(file, "Invalid File name ,File moved to Bad Raw Folder:%s" %filename)




                else:
                    #copying the filename to bad raw folder as regex and filename did not matches
                    shutil.copy("C:/Users/malik/PycharmProjects/wafer_project/Training_Batch_Files/"+filename,"C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(file,"Invalid File name ,File moved to Bad Raw Folder:%s"%filename)

            file.close()

        except Exception as e:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/NameValidationLog.txt",'a+')
            self.logger.log(file,"Error occured while Validating the filename :%s"%e)
            file.close()
            raise e


    def ValidateColumnLength(self,NumberOfColumns):
        '''
                          Method Name:ValidateColumnLength
                          Description:It validates the length of columns i.e no of columns in the files of Good Raw folder , if the no of columns
                                    are same then do nothing else move the file to Bad Raw folder

                         return:None
                         Written by:Jyoti Malik
                         version:1.0
                         Revision:None

                     '''
        try:
            # writting the validation start log
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/columnValidatingLog.txt",'a+')
            self.logger.log(file,"Validation for number of columns started!!!")
            for file in os.listdir("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/"+"Good_Raw/"):    # looping over the files stored in Good Raw folder
                csv=pd.read_csv("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/"+"Good_Raw/"+file)   # reading the path for .csv file
                if csv.shape[1]==NumberOfColumns:                                 # if the no of columns i.e csv.shape[1] matches with NumberOfColumns then do nothing
                    pass
                else:
                    # if the no of columns i.e csv.shape[1] does not matches with NumberOfColumns then move the file From Good Raw folder to Bad Raw folder
                    shutil.move("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/"+"Good_Raw/"+file,"C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(file,"Invalid column length in file ,file moved to Bad Raw folder :%s"%file)
            # writting the validation completed log
            self.logger.log(file,"Validation for number of columns completed!!")
            file.close()
        except OSError:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/columnValidatingLog.txt",'a+')
            self.logger.log(file,"Error occured while moving the file :%s "%OSError)
            file.close()
            raise OSError

        except Exception as e:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/columnValidatingLog.txt", 'a+')
            self.logger.log(file, "Error occured  :%s " % e)
            file.close()
            raise Exception


    def ValidatingMiisingValuesInWholeColumn(self):
        '''
                                  Method Name:ValidatingMiisingValuesInWholeColumn
                                  Description:It validates wheather a file contains any column which is having all values as Null, If found so then
                                              move it to the Bad Raw folder

                                 return:None
                                 Written by:Jyoti Malik
                                 version:1.0
                                 Revision:None

                             '''


        try:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/MissingValueInColumn.txt",'a+')
            self.logger.log(file,"Validation for missing values in column started!!!")
            for file in os.listdir("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Good_Raw/"):
                csv=pd.read_csv("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Good_Raw/"+file)
                count=0
                for feat in csv.columns:
                    if csv[feat].isnull().sum()==csv.shape[0]:
                        count+=1
                        shutil.copy("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Good_Raw/"+file,"C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Bad_Raw/")
                        self.logger.log(file,"Missing values column found in file,file moved to Bad Raw folder ")
                        break
                    else:
                        pass
                if count==0:
                    csv.rename(columns={"Unnamed: 0":"Wafer"},inplace=True)
                    csv.to_csv("C:/Users/malik/PycharmProjects/wafer_project/Training_Raw_files_validated/Good_Raw/"+file,index=None,header=True)

            self.logger.log(file, "Validation for missing values in column completed!!!")
            file.close()

        except OSError:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/MissingValueInColumn.txt",'a+')
            self.logger.log(file,"Error occured while moving the file :%s "%OSError)
            file.close()
            raise OSError

        except Exception as e:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/MissingValueInColumn.txt", 'a+')
            self.logger.log(file, "Error occured  :%s " % e)
            file.close()
            raise Exception






