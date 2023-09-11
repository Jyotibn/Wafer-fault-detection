import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer

class PreProcessor:
    def __init__(self,file_object,log_object):
        self.file_obj=file_object
        self.log_obj=log_object

    def remove_columns(self,df,column):
        '''
                                    Method Name:remove_columns
                                    Description:It will remove the given column from the dataframe
                                   return:useful dataframe df
                                   on Failure:raise Exception
                                   Written by:Jyoti Malik
                                   version:1.0
                                   Revision:None

                                                    '''
        # dataframe and the column to be deleted are stored in df and column variable
        self.df=df
        self.column=column
        self.log_obj.log(self.file_obj,"removal of column started!!!")
        try:
            # dropping the column
            self.df.drop(columns=self.column,axis=1,inplace=True)
            self.log_obj.log(self.file_obj,"%s Column removed successfully!!!"%self.column)
            return self.df
        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred while removing the given column %s"%e)
            self.log_obj.log(self.file_obj,"Removal of column unsuccessful")
            raise e

    def seperate_independent_dependent_feature(self,df,target_column):
        '''
                                          Method Name:seperate_independent_dependent_feature
                                          Description:It will seperate the data into independent and dependent features
                                         return:independent features in X and dependent feature in Y
                                         on Failure:raise Exception
                                         Written by:Jyoti Malik
                                         version:1.0
                                         Revision:None

                                                          '''
        self.df=df
        self.target=target_column
        self.log_obj.log(self.file_obj,"seperation of independent and dependent feature started!!!")
        try:
            # Y=dependent feature and X=independent features
            Y=self.df[self.target]
            X=self.df.drop(columns=self.target,axis=1,inplace=True)
            self.log_obj.log(self.file_obj,"seperation of independent and dependent feature completed successfuly!!!")
            return X,Y
        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred while seperation of independent and dependent features:%s"%e)
            raise e

    def check_missing_values(self,X):
        '''
                              Method Name:check_missing_values
                              Description:It will check if missing values ar there in the X dataframe,if found then creates
                               a new dataframe which will contain the info of count of null values of each column and then convert it into a csv file
                             on Failure:raise Exception
                             return:True if missing  values are present otherwise False
                             Written by:Jyoti Malik
                             version:1.0
                             Revision:None

                                                                  '''
        self.df=X
        self.null_present=False
        try:
            self.log_obj.log(self.file_obj,"checking missing values in the dataframe started!!!")
            null_counts_list= self.df.isnull().sum()
            for i in null_counts_list:
                if i>0:
                    self.null_present=True
                    self.log_obj.log(self.file_obj,"missing values found in the dataframe")
                    break
            # creating a new csv file null_values.csv which will contain the info of columns having null values
            if self.null_present:
                df_null_values=pd.DataFrame()    # creating a empty dataframe
                df_null_values.columns=self.df.columns     # giving the same column names as that of df dataframe
                df_null_values['missing_values_count']=np.asarray(self.df.isnull().sum())     # count of null values will be stored
                df_null_values.to_csv("C:/Users/malik/PycharmProjects/wafer_project/Preprocessing_data/null_values.csv")    # converting it to a csv file
            return self.null_present
        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred while checking the missing values in the dataframe")
            raise e

    def impute_missing_values(self,X):
        '''
                                     Method Name:impute_missing_values
                                     Description:It will impute the missing values with KNNImputer
                                    on Failure:raise Exception
                                    return:new dataframe with handled missing values
                                    Written by:Jyoti Malik
                                    version:1.0
                                    Revision:None

                                                                         '''
        self.log_obj.log(self.file_obj, "Imputation of missing values started!!!")
        # storing the independent feature dataframe in df
        self.df=X
        try:
            # imputing the missing values with KNNImputer
            imputer=KNNImputer(missing_values=np.nan,n_neighbors=5,weights='uniform')
            self.new_array=imputer.fit_transform(self.df)
            self.new_df=pd.DataFrame(data=self.new_array,columns=self.df.columns)
            self.log_obj.log(self.file_obj, "Imputation of missing values completed!!!")
            return self.new_df        # return the new dataframe with no missing values

        except Exception as e:
            self.log_obj.log(self.file_obj, "Error occurred while Imputing the missing values :%s"%e)
            raise e

    def check_column_with_constant_values(self,dataframe):
        '''
                                             Method Name:check_column_with_constant_values
                                             Description:It will check for the columns having std deviation 0 so that they can be dropped
                                            on Failure:raise Exception
                                            return:list of columns to drop
                                            Written by:Jyoti Malik
                                            version:1.0
                                            Revision:None

                                                                                 '''
        self.log_obj.log(self.file_obj, "checking for 0 std columns started!!!")
        # storing the dataframe in df
        self.df=dataframe
        try:
            # checking the for the columns having 0 std
            self.col_to_drop = [feat for feat in self.df.columns if self.df[feat].std() == 0]
            self.log_obj.log(self.file_obj, "checking for 0 std columns completed!!!")
            return self.col_to_drop   # returning the list of columns to be dropped
        except Exception as e:
            self.log_obj.log(self.file_obj, "Error occured while checking for 0 std columns :%s"%e)
            raise e













