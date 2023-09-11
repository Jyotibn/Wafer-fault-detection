import os
from datetime import datetime
import pandas as pd
from Application_logging.logger import App_Logger

class DataTransformPredict:
    def __init__(self):
        self.GoodDataPath="C:/Users/malik/PycharmProjects/wafer_project/Prediction_Raw_files_validated/Good_Raw"
        self.logger=App_Logger()

    def ReplaceMissingValuesWithNull(self):
        '''
                              Method Name:ReplaceMissingValuesWithNull
                              Description:It replace all the missing values with Null and we are only keeping the integer
                                         portion of filename in the Wafer column of the files

                             return:None
                             Written by:Jyoti Malik
                             version:1.0
                             Revision:None

                                     '''
        try:
            file=open("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Logs/DataTransforming.txt",'a+')
            self.logger.log(file,"File transformation started!!!")
            for file in os.listdir("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Raw_files_validated/Good_Raw/"):
                csv=pd.read_csv("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Raw_files_validated/Good_Raw/"+file)
                csv.fillna("NULL",inplace=True)
                csv['Wafer']=csv['Wafer'].str[6:]
                csv.to_csv("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Raw_files_validated/Good_Raw/"+file,index=None,header=True)
                self.logger.log(file,"%s file transformed successfully"%file)
            self.logger.log(file,"File transformation completed!!!")
            file.close()

        except Exception as e:
            file = open("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Logs/DataTransforming.txt", 'a+')
            self.logger.log(file, "Error occurred while replacing the missing values: %s"%e)
            file.close()
            raise Exception



