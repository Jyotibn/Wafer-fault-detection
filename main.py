import os
from wsgiref import simple_server
from flask import Flask,render_template,request,Response
from training_Validation_Insertion import train_validation
from TrainingModel import TrainingModel
from prediction_Validation_Insertion import pred_Validation
from PredictFromModel import Prediction
import json
app=Flask(__name__)

@app.route('/',methods=['GET'])
def home():
    return render_template('index1.html')




@app.route('/training',methods=['POST'])
def trainRouteClient():
    try:
        # 1. DATA INGESTION
            #(i)Data (Batches) for training

        if request.json['folderPath'] is not None:   # folderPath for Training_Batch_files
            path=request.json['folderPath']

            #(ii) Data Validation

            train_val_object = train_validation(path)   # object creation for trainValidation class
            train_val_object.train_validation()     # calling the trainValidation() function using train_val_object

            train_model_object=TrainingModel()   # object for TrainingModel class
            train_model_object.ModelTraining()    # calling the ModelTraining() function
        return Response("Training Successful!!!")

    except ValueError:
        return Response("Error occurred :%s" %ValueError)
    except KeyError:
        return Response("Error occurred :%s"%KeyError)
    except Exception as e:
        return Response("Error occurred :%s"%e)





@app.route('/prediction',methods=['POST'])
def predictRouteClient():
    try:
        if request.json is not None:            # for Postman
            path=request.json['filepath']       # getting the folder path containing the batch files

            pred_val=pred_Validation(path)      # creating the object for pred_Validation class
            pred_val.prediction_Validation()    # calling the prediciton_Validation function

            pred=Prediction(path)                   # object creation for Prediction class
            path,json_prediction=pred.Predict_from_model()           # calling the Predict_from_model() method using pred object

            return Response("Prediction file created at!!!  "+str(path)+" and few of the prediction are"+str(json.loads(json_prediction)))

        elif request.form is not None:
            path = request.json['filepath']  # getting the folder path containing the batch files

            pred_val = pred_Validation(path)  # creating the object for pred_Validation class
            pred_val.prediction_Validation()  # calling the prediciton_Validation function

            pred = Prediction(path)  # object creation for Prediction class
            path, json_prediction = pred.Predict_from_model()  # calling the Predict_from_model() method using pred object

            return Response("Prediction file created at!!!  " + str(path) + " and few of the prediction are" + str(
                json.loads(json_prediction)))

        else:
            print("Nothing matched")

    except ValueError:
        return Response("Error occurred!!!%s"%ValueError)
    except KeyError:
        return Response("Error occurred !!!%s"%KeyError)
    except Exception as e:
        return Response("Error occurred !!!%s"%e)


port=int(os.getenv("PORT",5000))
if __name__=="__main__":
    host='0.0.0.0'
    httpd=simple_server.make_server(host,port,app)
    httpd.serve_forever()











