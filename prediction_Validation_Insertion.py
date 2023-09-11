from Application_logging import logger
from Prediction_Raw_Data_Validation.predictionRawValidation import Prediction_Data_Validation
from DataTransformation_Prediction.data_transformation_prediction import DataTransformPredict
from DataType_Validation_Prediction.datatype_Validation_prediction import dBoperation

class pred_Validation:
    def __init__(self,path):
        self.raw_data=Prediction_Data_Validation(path)
        self.Data_transform_obj=DataTransformPredict()
        self.DbOperation_obj=dBoperation()
        self.file_object=open("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Logs/Prediction_log.txt",'a+')
        self.log_writer=logger.App_Logger()


    def prediction_Validation(self):
        try:
            self.log_writer.log(self.file_object,"Starting the Validation of files for prediction!!!")  # calling the log() using log_writer obect
            # extracting values from schema
            LengthOfDateStampFile,LengthOfTimeStampFile,NumberOfColumns,column_names=self.raw_data.Values_from_schema()

            #getting the regex defined for vlaidation of filename
            regex=self.raw_data.ManualRegexCreation()

            #validating the filename
            self.raw_data.ValidationFileNameRaw(regex,LengthOfDateStampFile,LengthOfTimeStampFile)

            # validating the column length in file means the no of columns
            self.raw_data.ValidateColumnLength(NumberOfColumns)

            #validating if there is any column which is having the values as Null values
            self.raw_data.ValidatingMiisingValuesInWholeColumn()

            self.log_writer.log(self.file_object, " Validation of Prediction_Raw_data_files Completed!!!")

            #Data Transformation
            self.log_writer.log(self.file_object, "Starting the Data Transformation!!!")

            # replacing all the missing values with NULL
            self.Data_transform_obj.ReplaceMissingValuesWithNull()
            self.log_writer.log(self.file_object, " Data Transformation completed!!!")

            # inserting the transformed data into the database
                # creating a table for insertion of data into database
            self.log_writer.log(self.file_object,"creating a prediction table for the transformed data to be inserted into the database")

            self.DbOperation_obj.createTable('Training',column_names)   # Training=database name

            self.log_writer.log(self.file_object,"Table created successfully!!!")
            #insertion of good data into the table
            self.log_writer.log(self.file_object, "Insertion of data into the table started!!!")

            self.DbOperation_obj.insertIntoTableGoodData('Training')
            self.log_writer.log(self.file_object, "Insertion of data into the table completed!!!")

            #now we don't need the Good_Raw folder as all the good data is inserted into the database successfully so we can delete this folder
            self.raw_data.deleteExistingGoodDataPredictionFolder()
            self.log_writer.log(self.file_object, "Good_Raw folder deleted successfully!!!")
            self.log_writer.log(self.file_object, "Moving the Bad_Raw folder to archive and then deleting the Bad_Raw folder")

            # moving the Bad_Raw folder to the Archive Folder and then delete the Bad_Raw folder
            self.raw_data.MovingBadFilesToArchive()
            self.log_writer.log(self.file_object,"Bad files moved to Archive Folder and the Bad_Raw folder is deleted successfully!!!")

            # Extracting the data  from table and exporting to csv file
            self.log_writer.log(self.file_object,"Extracting the csv file from table")
            self.DbOperation_obj.selectDataFromTable('Training')
            self.log_writer.log(self.file_object,"Exported the file to csv successfully!!!")

        except Exception as e:
            raise e