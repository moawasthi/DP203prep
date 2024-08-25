# Databricks notebook source
# MAGIC %md
# MAGIC # Importing libraries

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report


# COMMAND ----------

# MAGIC %md
# MAGIC # Create and Load Dataset

# COMMAND ----------

data = {
    'user_id' : [1,1,1,2,2,3,3,4,4],
    'movie_id': [101,102,103, 101,103,102,104,102,103],
    'movie_genre':[0,1,1,0,1,1,0,1,1], # 0: Action, 1: Comedy
    'user_age':[23,23,23,45,45,35,25,25,25],
    'user_gender':[0,0,0,1,1,0,0,1,1],
    'rating':[0,0,1,4,4,3,2,2,2]
}

df = pd.DataFrame(data)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Preprocessing

# COMMAND ----------

df['liked'] = df['rating'].apply(lambda x: 1 if x >= 4 else 0)
X = df.drop(['user_id','movie_id', 'rating', 'liked'], axis=1)
Y = df['liked']

print(X.head())
print(Y.head())


# COMMAND ----------

# MAGIC %md
# MAGIC # Split the data into training and testing Sets

# COMMAND ----------

X_train , X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)
print(f"Training set size {X_train.shape[0]}")
print(f"Test set size : {X_test.shape[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Initialize and Train Random Forest classifier

# COMMAND ----------

clf = RandomForestClassifier(n_estimators=100, random_state=42)
clf.fit(X_train, Y_train)


# COMMAND ----------

# MAGIC %md
# MAGIC # Make Predictions

# COMMAND ----------

y_predict = clf.predict(X_test)
print(y_predict)

# COMMAND ----------

# MAGIC %md
# MAGIC # Evaluate the Model 

# COMMAND ----------

accuracy = accuracy_score(Y_test, y_predict)
print(f"accuracy: {accuracy:.2f}")
confusion_matrix = confusion_matrix(Y_test, y_predict)
print(f"confusion matrix : {confusion_matrix}")
report = classification_report(Y_test, y_predict)
print(f"Classification report {report}")
