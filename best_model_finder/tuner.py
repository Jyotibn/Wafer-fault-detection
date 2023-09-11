import numpy as np
from sklearn.model_selection import GridSearchCV
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.metrics import roc_auc_score,accuracy_score

class Model_finder:
    def __init__(self,file_obj,log_obj):
        self.file_obj=file_obj
        self.log_obj=log_obj
        self.rfc=RandomForestClassifier()
        self.xgbc=XGBClassifier(Objective='binary:logistic')
        self.log_reg=LogisticRegression()
        self.nb=GaussianNB()
        self.knn=KNeighborsClassifier()
        self.svc=SVC()


    def get_best_param_for_random_forest(self,x_train,y_train):
        '''
                                           Method Name:get_best_param_for_random_forest
                                           Description:It will find the best parameters for RandomForestClassifiers after performing hyperparmeter tuning using GridSearchCv
                                          on Failure:raise Exception
                                          return:model with best parameters
                                          Written by:Jyoti Malik
                                          version:1.0
                                          Revision:None

                                                                               '''
        self.log_obj.log(self.file_obj,"Entered the get_best_param_for_random_forest method of  Model_finder class")
        try:
            # trying for different paramters
            self.param_grid = {
                'n_estimators': [200, 500],
                'max_features': ['auto', 'sqrt', 'log2'],
                'max_depth': [4, 5, 6, 7, 8],
                'criterion': ['gini', 'entropy']
            }
            # performing the GridSearchCV
            self.grid=GridSearchCV(estimator=self.rfc,param_grid=self.param_grid,cv=5,verbose=3)
            self.grid.fit(x_train,y_train)

            # extracting the best parameters
            self.n_estimators=self.grid.best_params_['n_estimators']
            self.max_features=self.grid.best_params_['max_features']
            self.max_depth=self.grid.best_params_['max_depth']
            self.criterion=self.grid.best_params_['criterion']

            # calling the RandomForestClassifier with the best parameters
            self.clf=RandomForestClassifier(n_estimators=self.n_estimators,max_features=self.max_features,max_depth=self.max_depth,criterion=self.criterion)
            self.clf.fit(x_train,y_train)

            self.log_obj.log(self.file_obj,"Best parameters for RandomForestClassifier are "+str(self.grid.best_params_))

            return self.clf     # returning the model with best paramters

        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred in get_best_param_for_random_forest method of Model_finder class"+str(e))
            self.log_obj.log(self.file_obj,"failed to give best parameters for RandomForestClassifier")
            raise e





    def get_best_param_for_XGboost(self,x_train,y_train):
        '''
                                                   Method Name:get_best_param_for_XGboost
                                                   Description:It will find the best parameters for XGBClassifiers after performing hyperparmeter tuning using GridSearchCv
                                                  on Failure:raise Exception
                                                  return:model with best parameters
                                                  Written by:Jyoti Malik
                                                  version:1.0
                                                  Revision:None

                                                                                       '''
        self.log_obj.log(self.file_obj, "Entered the get_best_param_for_XGboost method of  Model_finder class")
        try:
            self.param_grid = {
                'max_depth': range(2, 10, 1),
                'n_estimators': range(60, 220, 40),
                'learning_rate': [0.1, 0.01, 0.05]
            }
            # performing the GridSearchCV
            self.grid = GridSearchCV(estimator=self.xgbc,param_grid=self.param_grid, cv=5, verbose=3)
            self.grid.fit(x_train, y_train)

            # extracting the best parameters
            self.n_estimators = self.grid.best_params_['n_estimators']
            self.max_depth = self.grid.best_params_['max_depth']
            self.learning_rate= self.grid.best_params_['learning_rate']

            # calling the RandomForestClassifier with the best parameters
            self.xgbc = XGBClassifier(n_estimators=self.n_estimators,
                                              max_depth=self.max_depth, learning_rate=self.learning_rate)
            self.xgbc.fit(x_train, y_train)

            self.log_obj.log(self.file_obj,
                             "Best parameters for RandomForestClassifier are " + str(self.grid.best_params_))

            return self.clf  # returning the model with best paramters

        except Exception as e:
            self.log_obj.log(self.file_obj,
                             "Error occurred in get_best_param_for_random_forest method of Model_finder class" + str(e))
            self.log_obj.log(self.file_obj, "failed to give best parameters for RandomForestClassifier")
            raise e

    def get_best_param_for_logistic_regression(self,x_train,y_train):
        '''
                                                   Method Name:get_best_param_for_logistic_regression
                                                   Description:It will find the best parameters for LogisticRegression after performing hyperparmeter tuning using GridSearchCv
                                                  on Failure:raise Exception
                                                  return:model with best parameters
                                                  Written by:Jyoti Malik
                                                  version:1.0
                                                  Revision:None

                                                                                       '''
        self.log_obj.log(self.file_obj, "Entered the get_best_param_for_logistic_regression method of  Model_finder class")
        try:
            self.param_grid = {"C": np.logspace(-3, 3, 7), "penalty": ["l1", "l2"]}
            # performing the GridSearchCV
            self.grid = GridSearchCV(estimator=self.log_reg, param_grid=self.param_grid, cv=5, verbose=3)
            self.grid.fit(x_train, y_train)

            # extracting the best parameters
            self.C=self.grid.best_params_['C']
            self.penalty=self.grid.best_params_['penalty']

            # calling the RandomForestClassifier with the best parameters
            self.log_reg = LogisticRegression(C=self.C,penalty=self.penalty)
            self.log_reg.fit(x_train, y_train)

            self.log_obj.log(self.file_obj,
                             "Best parameters for RandomForestClassifier are " + str(self.grid.best_params_))

            return self.log_reg  # returning the model with best paramters

        except Exception as e:
            self.log_obj.log(self.file_obj,
                             "Error occurred in get_best_param_for_random_forest method of Model_finder class" + str(e))
            self.log_obj.log(self.file_obj, "failed to give best parameters for RandomForestClassifier")
            raise e

    def get_best_param_for_naive_bayes(self,x_train,y_train):
        '''
                                                   Method Name:get_best_param_for_naive_bayes
                                                   Description:It will find the best parameters for NaiveBayes after performing hyperparmeter tuning using GridSearchCv
                                                  on Failure:raise Exception
                                                  return:model with best parameters
                                                  Written by:Jyoti Malik
                                                  version:1.0
                                                  Revision:None

                                                                                       '''
        self.log_obj.log(self.file_obj, "Entered the get_best_param_for_naive_bayes method of  Model_finder class")
        try:

            self.nb.fit(x_train,y_train)
            self.log_obj.log(self.file_obj, "Best parameters for NaiveBayes are="+str(self.nb.get_params()))
            return self.nb
        except Exception as e:
            self.log_obj.log(self.file_obj,
                             "Error occurred in get_best_param_for_naive_bayes method of Model_finder class" + str(e))
            self.log_obj.log(self.file_obj, "failed to give best parameters for NaiveBayes")
            raise e

    def get_best_param_for_KNN(self,x_train,y_train):
        '''
                                                   Method Name:get_best_param_for_KNN
                                                   Description:It will find the best parameters for KNN after performing hyperparmeter tuning using GridSearchCv
                                                  on Failure:raise Exception
                                                  return:model with best parameters
                                                  Written by:Jyoti Malik
                                                  version:1.0
                                                  Revision:None

                                                                                       '''
        self.log_obj.log(self.file_obj, "Entered the get_best_param_for_KNN method of  Model_finder class")
        try:
            self.param_grid = {'n_neighbors':np.range(1,31)}
            # performing the GridSearchCV
            self.grid = GridSearchCV(estimator=self.knn, param_grid=self.param_grid, cv=5, verbose=3)
            self.grid.fit(x_train, y_train)

            # extracting the best parameters
            self.n_neighbors = self.grid.best_params_['n_neighbors']


            # calling the RandomForestClassifier with the best parameters
            self.knn = KNeighborsClassifier(n_neighbors=self.n_neighbors)
            self.knn.fit(x_train, y_train)

            self.log_obj.log(self.file_obj,
                             "Best parameters for RandomForestClassifier are " + str(self.grid.best_params_))

            return self.knn  # returning the model with best paramters

        except Exception as e:
            self.log_obj.log(self.file_obj,
                             "Error occurred in get_best_param_for_random_forest method of Model_finder class" + str(e))
            self.log_obj.log(self.file_obj, "failed to give best parameters for RandomForestClassifier")
            raise e

    def get_best_param_for_SVC(self,x_train,y_train):
        '''
                                                   Method Name:get_best_param_for_SVC
                                                   Description:It will find the best parameters for SVC after performing hyperparmeter tuning using GridSearchCv
                                                  on Failure:raise Exception
                                                  return:model with best parameters
                                                  Written by:Jyoti Malik
                                                  version:1.0
                                                  Revision:None

                                                                                       '''
        self.log_obj.log(self.file_obj, "Entered the get_best_param_for_SVC method of  Model_finder class")
        try:
            self.param_grid = {'C': [0.1, 1, 10, 100], 'gamma': [1, 0.1, 0.01, 0.001], 'kernel': ['rbf', 'poly', 'sigmoid']}
            # performing the GridSearchCV
            self.grid = GridSearchCV(estimator=self.svc, param_grid=self.param_grid, cv=5, verbose=3)
            self.grid.fit(x_train, y_train)

            # extracting the best parameters
            self.C = self.grid.best_params_['C']
            self.gamma = self.grid.best_params_['gamma']
            self.kernel = self.grid.best_params_['kernel']


            # calling the RandomForestClassifier with the best parameters
            self.svc = SVC(C=self.C, gamma=self.gamma,
                                              kernel=self.kernel,)
            self.svc.fit(x_train, y_train)

            self.log_obj.log(self.file_obj,
                             "Best parameters for RandomForestClassifier are " + str(self.grid.best_params_))

            return self.svc  # returning the model with best paramters

        except Exception as e:
            self.log_obj.log(self.file_obj,
                             "Error occurred in get_best_param_for_random_forest method of Model_finder class" + str(e))
            self.log_obj.log(self.file_obj, "failed to give best parameters for RandomForestClassifier")
            raise e

    def accuracy(self,y_test,prediction_variable,name_of_model):
        try:
            if len(y_test.unique())==1:
               self.score=accuracy_score(y_test,prediction_variable)
               self.log_obj.log(self.file_obj,"accuracy score for "+name_of_model+"="+ str(self.score))
            else:
                self.score=roc_auc_score(y_test,prediction_variable)
                self.log_obj.log(self.file_obj,"auc for "_name_of_model+"="+str(self.score))
            return self.score
        except Exception as e:
            raise e









    def best_model(self,x_train,x_test,y_train,y_test):
        try:
            # RandomForest predictions
            self.randomForest=self.get_best_param_for_random_forest(x_train,y_train)
            self.randomForest_Prediction=self.randomForest.predict(x_test)
            self.Random_Forest_accuracy_score=self.accuracy(y_test,self.randomForest_Prediction,name_of_model='RandomForest')


            # XGboost predictions
            self.XGboost = self.get_best_param_for_XGboost(x_train, y_train)
            self.XGboost_predictions = self.XGboost.predict(x_test)
            self.XGboost_accuracy_score = self.accuracy(y_test, self.XGboost_predictions,name_of_model='XGBoost')

            # LogisticRegression prediction
            self.LogisticRegress = self.get_best_param_for_logistic_regression(x_train, y_train)
            self.LogisticRegression_Prediction = self.LogisticRegress.predict(x_test)
            self.Logistic_Regression_accuracy_score = self.accuracy(y_test, self.LogisticRegression_Prediction, name_of_model='LogisticRegression')

            # Naive Bayes prediction
            self.NaiveBayes = self.get_best_param_for_naive_bayes(x_train, y_train)
            self.Naive_Bayes_prediction = self.NaiveBayes.predict(x_test)
            self.NaiveBayes_accuracy_score = self.accuracy(y_test, self.Naive_Bayes_prediction, name_of_model='NaiveBayes')



            # KNN  prediction
            self.KNN = self.get_best_param_for_KNN(x_train, y_train)
            self.KNN_prediction = self.KNN.predict(x_test)
            self.KNN_accuracy_score = self.accuracy(y_test, self.KNN_prediction, name_of_model='KNN')

            # SVC  prediction
            self.SVc = self.get_best_param_for_SVC(x_train, y_train)
            self.SVC_prediction = self.SVc.predict(x_test)
            self.SVC_accuracy_score = self.accuracy(y_test, self.SVC_prediction, name_of_model='SVC')

            # compairing the accuracy of all models
            self.largest_Accuracy=0
            dict_of_acuuracy_scores={'RandomForest':[self.Random_Forest_accuracy_score,self.randomForest],
                                     'XGboost':[self.XGboost_accuracy_score,self.XGboost],
                                     'Logistic_Regression':[self.Logistic_Regression_accuracy_score,self.LogisticRegress],
                                     'NaiveBayes':[self.NaiveBayes_accuracy_score,self.NaiveBayes],'KNN':[self.KNN_accuracy_score,self.KNN],
                                     'SVC':[self.SVC_accuracy_score,self.SVc]}
            for i in dict_of_acuuracy_scores:
                if dict_of_acuuracy_scores.get(i)[0]>self.largest_Accuracy:
                    self.largest_Accuracy=dict_of_acuuracy_scores.get(i)[0]
                else:
                    pass
            for model_name,accuracy_score_model in dict_of_acuuracy_scores.items():
                if accuracy_score_model[0]==self.largest_Accuracy:
                    self.model_name=model_name
                    self.model=accuracy_score_model[1]
            return self.model_name,self.model

        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred in best_model method of Model finder class :"+str(e))
            self.log_obj.log(self.file_obj, "Failed to find the best model ")
            raise e
