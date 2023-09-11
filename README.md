# Wafer-fault-detection
**Problem Statement:**

**Objective:** Create a machine learning model to predict whether a semiconductor wafer needs replacement based on sensor data. Two classes: +1 (working) and -1 (faulty).

**Data Description:**
- Sensor data for wafers with 590 columns.
- Target variable: +1 (Good) or -1 (Bad).
- A schema file with metadata (file names, column names, data types) is provided.

**Data Validation:**
1. Validate file names based on schema.
2. Check date and time format in file names.
3. Validate the number of columns.
4. Ensure column names match the schema.
5. Validate column data types.
6. Remove files with all NULL values.

**Data Insertion in Database:**
- Create a database with the given name.
- Create a table "Good_Data" with schema column names and data types.
- Insert valid files into the table.
- Handle data type mismatches.

**Model Training:**
1. Export data from the database.
2. Data preprocessing:
   - Impute null values with KNN imputer.
   - Remove columns with zero standard deviation.
3. Perform clustering using KMeans.
4. Select the best model (Random Forest or XGBoost) for each cluster based on AUC scores.
5. Save all models.

**Prediction:**
1. Receive data in batches.
2. Perform data validation (similar to training data).
3. Export data from the database.
4. Data preprocessing (impute nulls, remove low variance columns).
5. Use KMeans to predict clusters.
6. Select the appropriate model for each cluster.
7. Predict wafer status.
8. Save predictions in a CSV file.

**Conclusion:**
Create a robust machine learning pipeline to predict wafer status based on sensor data, with validation, data insertion, model training, and prediction steps.
