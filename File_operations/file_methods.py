import pickle
import shutil
import os


class file_operation:
    def __init__(self,file_obj,log_obj):
        self.model_directory = 'C:/Users/malik/PycharmProjects/wafer_project/models/'
        self.file_obj=file_obj
        self.log_obj=log_obj

    def save_model(self,model,filename):
        self.log_obj.log(self.file_obj,"entered the save model method of file_operation class")
        try:
            path=os.path.join(self.model_directory,filename)  # creating seperate directory for each cluster
            if os.path.isdir(path):            # remove previously existed model for each cluster
                shutil.rmtree(self.model_directory)
                os.makedirs(path)
            else:
                os.makedirs(path)
            with open(path+'/'+filename+'.sav','wb') as f:
                pickle.dump(model,f)
            self.log_obj.log(self.file_obj,"successfully saved the model"+filename)

        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred in save model method of file operation class:"str(e))
            self.log_obj.log(self.file_obj, "unable to save the model" + filename)
            raise e

    def load_model(self,filename):
        self.log_obj.log(self.file_obj,"entered the load model() method of file_operation class")
        try:
            with open(self.model_directory+'/'+filename+'/'+filename+'.sav','rb') as f:
                self.log_obj.log(self.file_obj,"Model file"+filename+"loaded successfully")
                return pickle.load(f)

        except Exception as e:
            self.log_obj.log(self.file_obj,"Error occurred in the load model() method of file operation class:"+str(e))
            self.log_obj.log(self.file_obj,"could not load model file "+filename)
            raise e

    def find_correct_model_file(self,cluster_no):
        try:
            self.cluster_no=cluster_no
            self.folder_name=self.model_directory
            self.list_of_model_files=[]
            self.list_of_files=os.listdir(self.folder_name)
            for file in self.list_of_files:
                try:
                    if(file.index(str(self.cluster_no))!=-1):
                        self.model_name=file
                except:
                    continue              # this continue statement is given bcz the above if statement will give error
                                          # when the if statement becomes false
            self.model_name=self.model_name.split('.')[0]
            self.log_obj.log(self.file_obj,"exited from the find_correct_model_file method of file operation class")
            return self.model_name

        except Exception as e:
            self.log_obj.log(self.file_obj,"Exception occurred in find_correct_model_file method of file operation class:"str(e))
            self.log_obj.log(self.file_obj,"could not find the correct model ---failed!!!")
            raise e










