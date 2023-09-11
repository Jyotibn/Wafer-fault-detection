import pandas
from Prediction_Raw_Data_Validation.predictionRawValidation import Prediction_Data_Validation
from Application_logging.logger import App_Logger
from data_ingestion_predtiction import Data_loader_prediction
from Data_Preprocessing import Pre_Processing
from File_operations import file_methods


class Prediction:
    def __init__(self,path):
        if path is not None:
            self.pred_data_val=Prediction_Data_Validation(path)
            self.file_obj=open("C:/Users/malik/PycharmProjects/wafer_project/Prediction_Logs/Prediction_log.txt",'a+')
            self.log_writter=App_Logger()

    def Predict_from_model(self):
        try:
            self.pred_data_val.delete_prediction_file()     # delete the existing prediction file from the last run
            self.log_writter.log(self.file_obj,"Start of Prediction")

            # DATA INGESTION
            data_getter=Data_loader_prediction.Data_Getter_Prediction(self.file_obj,self.log_writter)
            df=data_getter.get_data()

            # DATA PREPROCESSING
            preprocessor=Pre_Processing.PreProcessor(self.file_obj,self.log_writter)
            null_present=preprocessor.check_missing_values(df)         # check if there is any missing values
            if null_present:                                           # if found then impute the missing values
                df=preprocessor.impute_missing_values(df)

            # checking for the columns having 0 Std deviation
            col_to_drop=preprocessor.check_column_with_constant_values(df)
            df=preprocessor.remove_columns(df,col_to_drop)

            # MODEL LOADING-------for clustering---KMeans
            file_loader=file_methods.file_operation(self.file_obj,self.log_writter)
            KMeans=file_loader.load_model('KMeans')

            # getting the clusters for each record
            Clusters=KMeans.predict(df.drop['Wafer'],axis=1)    # we don't need the Wafer col so drop it
            df['Cluster']=Clusters                              # adding the cluster col to the df

            # getting unique clusters
            unique_clusters=df['Cluster'].unique()

            for i in unique_clusters:
                cluster_data=df[df['Cluster']==i]
                wafer_names=list(cluster_data['Wafer'])
                #cluster_data=cluster_data.drop(labels=['Wafer'],axis=1)
                cluster_data = df.drop(labels=['Wafer'], axis=1)
                cluster_data=cluster_data.drop(columns=['Cluster'],axis=1)
                model_name=file_loader.find_correct_model_file(i)    # we got the best model
                model=file_loader.load_model(model_name)       # loading the model
                #result=list(model.predict(cluster_data))

                result=list(model.predict(cluster_data))
                # now creating a dataframe for wafer and the corresponding cluster stored in the result
                result=pandas.DataFrame(list(zip(wafer_names,result)),columns=['Wafer','Prediction'])
                path="C:/Users/malik/PycharmProjects/wafer_project/PredictionOutputFile/Predictions.csv"
                result.to_csv("C:/Users/malik/PycharmProjects/wafer_project/PredictionOutputFile/Predictions.csv",header=True,mode='a+')
            self.log_writter.log(self.file_obj,"End of Prediction")

        except Exception as e:
            self.log_writter.log(self.file_obj,"Error occur while running the prediction :%s"%e)
            raise e

        return path,result.head().to_json(orient="records")

















