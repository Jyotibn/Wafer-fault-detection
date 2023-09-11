import matplotlib.pyplot as plt
#from Kneed import KneeLocator
from kneed import KneeLocator
from sklearn.cluster import KMeans
from File_operations import file_methods


class KMeansClustering:
    def __init__(self,file_obj,log_obj):
        self.file_obj=file_obj
        self.log_obj=log_obj


    def elbow_plot(self,df):
        '''
                             Method Name:elbow_plot
                             Description:It will check for clusters(K) from 1 to 10 and make a list of sum of squares as
                             wcss and then plot the elbow graph between K and wcss
                            on Failure:raise Exception
                            return:no of clusters
                            Written by:Jyoti Malik
                            version:1.0
                            Revision:None

                                                                 '''
        # storing the dataframe in df
        self.log_obj.log(self.file_obj,"entered the elbow_plot method of KMeansClustering class")
        self.df=df
        try:
            wcss=[]                        # wcss= list of sum of squares within cluster

            for K in range(1,11):
                Km=KMeans(n_clusters=K)    # calling the KMeans class
                Km.fit(df)                 # fitting the data
                wcss.append(Km.inertia_)   # appending the sum of square given by Km.interia_ in wcss list
            # plotting the graph for wcss and K(no of clusters)
            plt.plot(range(1,11),wcss)
            plt.title('Elbow method')
            plt.xlabel('Number of Clusters')
            plt.ylabel('WCSS')
            plt.show()
            #saving the plot
            plt.savefig('C:/Users/malik/PycharmProjects/wafer_project/Preprocessing_data/KMeans_Elbow.PNG')
            # checking for optimum no of clusters using KneeLocator
            self.Kn=KneeLocator(range(1,11),wcss,curve='convex',direction='decreasing')
            self.log_obj.log(self.file_obj, "The optimum no of clusters are "+str(self.Kn.Knee))
            return self.Kn.Knee   # this will give the no of clusters

        except Exception as e:
            self.log_obj.log(self.file_obj, "Error occurred in the elbow_plot method of KMeansClustering class :%s"%e)
            self.log_obj.log(self.file_obj, "finding the no of clusters failed!!!")
            raise e

    def create_cluster(self,df,no_of_clusters):
        '''
                                    Method Name:create_cluster
                                    Description:It will find the clusters for each row and then add the clusters in a new column
                                    named Cluster in the given database
                                   on Failure:raise Exception
                                   return:dataframe with the added cluster column
                                   Written by:Jyoti Malik
                                   version:1.0
                                   Revision:None

                                                                        '''
        # store the dataframe in df and no of clusters in no_of_clusters
        self.df=df
        self.no_of_clusters=no_of_clusters
        try:
            # calling the KMeans class
            self.log_obj.log(self.file_obj,"entered the create cluster method of KmeansClustring class")
            self.Km=KMeans(n_clusters=no_of_clusters)
            # getting the clusters
            self.clusters=self.Km.fit_predict(self.df)

            # saving the model

            self.file_op=file_methods.file_operation()
            self.save_model=self.file_op.save_model(self.Km,'KMeans')
            self.log_obj.log(self.file_obj, "saved the model ")

            # creating new column for Clusters in given dataframe
            self.df['Cluster']=self.clusters
            self.log_obj.log(self.file_obj, "successfully created the "+str(self.Kn.Knee)+'clusters ,exiting the create cluster method')
            return self.df           # returning the df with cluster column added

        except Exception as e:
            self.log_obj.log(self.file_obj, "error occurred in the create cluster method of KmeansClustring class :%s"%e)
            self.log_obj.log(self.file_obj, "failed to create "+str(self.Kn.Knee)+"clusters")
            raise e


