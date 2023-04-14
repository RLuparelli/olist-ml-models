# Databricks notebook source
# MAGIC %pip install feature-engine mlflow scikit-plot

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
from sklearn import ensemble

import scikitplot as skplt

import mlflow


from feature_engine import imputation

import pandas as pd

pd.set_option('display.max_rows', 1000)

# COMMAND ----------

# DBTITLE 1,SAMPLE
df = spark.table("silver.analytics.abt_olist_churn").toPandas()
# df_oot = df[df['dtReference']=='2018-01-01']

# df_train = df[df['dtReference']!='2018-01-01']
# df_train.shape

# Base Out Of Time
df_oot = df[df['dtReference']=='2018-01-01']

# Base de Treino
df_train = df[df['dtReference']!='2018-01-01']


# COMMAND ----------

# DBTITLE 1,Untitled
df_train.head()

var_identity = ['dtReference','idVendedor']
target = 'flChurn'
to_remove = ['qtdRecencia', target] + var_identity
features = df.columns.tolist()
features = list(set(features) - set(to_remove))
features.sort()
features


# COMMAND ----------

# DBTITLE 1,Separando os Dados
X_train, X_test, y_train, y_test = model_selection.train_test_split(df_train[features],
                                                                    df_train[target],
                                                                    train_size=0.8,
                                                                    random_state=42)

print("Proporção resposta Treino:", y_train.mean())
print("Proporção resposta Teste:", y_test.mean())

# COMMAND ----------

# DBTITLE 1,Descobrindo as Variaveis Erradas - "NaN" 
X_train.isna().sum().sort_values(ascending=False)

missing_minus_100 = ['avgIntervaloVendas',
                     'maxNota',
                     'medianNota',
                     'minNota',
                     'avgNota',
                     'avgVolumeProduto',
                     'minVolumeProduto',
                     'maxVolumeProduto',
                     'medianVolumeProduto',
                    ]

missing_0 = ['medianQtdeParcelas',
             'avgQtdeParcelas',
             'minQtdeParcelas',
             'maxQtdeParcelas',
            ]

# COMMAND ----------

# DBTITLE 1,Definindo o Experimento
mlflow.set_experiment("/Users/luparelli@hotmail.com.br/Olist-churn-lupa2")

# COMMAND ----------

# DBTITLE 1,Modelo
with mlflow.start_run():

    mlflow.sklearn.autolog()
    
    mlflow.autolog()
    
    ##Transformando os dados errados

    imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100,
                                                              variables=missing_minus_100)

    imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                      variables=missing_0)

    ##Modelo de arvore de decisao

    model = tree.DecisionTreeClassifier(min_samples_leaf=25)

    ##Passando o modelo no PipeLine 

    model_pipeline = pipeline.Pipeline([("Imputer -100", imputer_minus_100),
                                            ("Imputer 0", imputer_0),
                                            ("Decision Tree", model),
                                            ])  

    ##Treinando o algoritimo

    model_pipeline.fit(X_train, y_train)

    auc_train = metrics.roc_auc_score(y_train, model_pipeline.predict_proba(X_train)[:,1])
    auc_test = metrics.roc_auc_score(y_test, model_pipeline.predict_proba(X_test)[:,1])
    auc_oot = metrics.roc_auc_score(df_oot[target], model_pipeline.predict_proba(df_oot[features])[:,1])

    metrics_model = {"auc_train": auc_train,
                        "auc_test": auc_test,
                        "auc_oot": auc_oot}


    mlflow.log_metrics(metrics_model)


