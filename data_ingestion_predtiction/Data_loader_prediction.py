import pandas as pd

class Data_Getter_Prediction:

    def __init__(self,file_object,logger_object):
        self.prediction_input_file="C:/Users/malik/PycharmProjects/wafer_project/PredictionFileFromDb/InputFile.csv"
        self.file_obj=file_object
        self.logger_object=logger_object


    def get_data(self):
        '''
                                  Method Name:get_data
                                  Description:It will read the csv file --InputFile.csv
                                 return:DataFrame df
                                 on Failure:raise Exception
                                 Written by:Jyoti Malik
                                 version:1.0
                                 Revision:None

                                                                     '''
        self.logger_object.log(self.file_obj,"entering the get_data() method of Data_Getter class to read the csv file")
        try:
            # reading the csv file
            self.df=pd.read_csv(self.prediction_input_file)
            self.logger_object.log(self.file_obj,"Data loaded successfully!!!,exiting from the get_Data() method")
            return self.df    # returning the dataframe df
        except Exception as e:
            self.logger_object.log(self.file_obj,"Error occurred while reading the csv file:%s"%e)
            self.logger_object.log(self.file_obj,"data loading unsuccessful")
            raise e
