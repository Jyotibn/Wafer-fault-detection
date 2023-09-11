from Application_logging.logger import App_Logger
from Data_Ingestion import Data_loader
from Data_Preprocessing import Pre_Processing
from Data_Preprocessing import clustring
from sklearn.model_selection import train_test_split
from best_model_finder import tuner
from File_operations import file_methods


class TrainingModel():
    def __init__(self):
        self.file_object=open("C:/Users/malik/PycharmProjects/wafer_project/Training_log/ModelTrainingLog.txt",'a+')
        self.logger_object=App_Logger()


    def ModelTraining(self):
        # logging the start of training
        self.logger_object.log(self.file_object,"Start of Training")
        try:
            data_getter=Data_loader.Data_Getter(self.file_object,self.logger_object)
            df=data_getter.get_data()

            # data preprocessing steps
            self.logger_object.log(self.file_object,"Data Preprocessing Started!!!")
            PreProcessing_obj=Pre_Processing.PreProcessor(self.file_object,self.logger_object)           # creating the object for PreProcesssor class
            df=PreProcessing_obj.remove_columns(df,['Wafer'])                        # calling the remove_columns() method with the PreProcessing_obj

            # seperate the independent and dependent features X and Y
            X,Y=PreProcessing_obj.seperate_independent_dependent_feature(df,target_column='Output')   # X=independent features ,Y=dependent feature

            # checking whether the missing values are present or not
            is_null_present=PreProcessing_obj.check_missing_values(X)

            # imputing the missing values
            if is_null_present:
                X=PreProcessing_obj.impute_missing_values(X)   # this X will have the new dataframe with no missing values
                self.logger_object.log(self.file_object,"all missing values are handled successfully!!!")

            # check for the columns which are not contributing in  model building
            # checking for the columns which are containing constant values and make  a list of those column to drop
            col_to_drop=PreProcessing_obj.check_column_with_constant_values(X)
            # dropping the columns
            PreProcessing_obj.remove_columns(X,col_to_drop)
            self.logger_object.log(self.file_object,"Unneccessary columns are dropped successfully!!!")

            # Data clustring

            kmean_ob=clustring.KMeansClustering(self.file_object,self.logger_object)
            no_of_clusters=Kmean_obj.elbow_plot(X)

            # creating the clusters
            X=Kmean_obj.create_cluster(X,no_of_clusters)

            # adding the labels
            X['Labels']=Y

            # getting the unique clusters from the dataset
            unique_clusters=X['Cluster'].unique()

            #finding the best model for each cluster
            for i in unique_clusters:
                cluster_data=X[X['Cluster']==i]

                # prepare the label and feature columns
                cluster_featuers=cluster_data.drop(Columns=['Labels'])
                cluster_label=cluster_data['Labels']

                # train test split
                x_train,x_test,y_train,y_test=train_test_split(cluster_featuers,cluster_label,test_size=1/3,random_state=355)

                model_finder=tuner.Model_finder(self.file_object,self.logger_object)
                Best_model_name,Best_model=model_finder.best_model(x_train,x_test,y_train,y_test)

                # saving the best model
                file_op_obj=file_methods.file_operation(self.file_object,self.logger_object)
                save_file=file_op_obj.save_model(Best_model,Best_model_name+str(i))

            # logging the successful training
            self.logger_object.log(self.file_object,"Successful end of training!!!")
            self.file_object.close()

        except Exception as e:
            self.logger_object.log(self.file_object,"Unsuccessful end of training:"+str(e))
            self.file_object.close()
            raise e














