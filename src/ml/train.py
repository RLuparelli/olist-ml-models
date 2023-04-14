# Databricks notebook source
# MAGIC %pip install feature-engine

# COMMAND ----------

from sklearn import model_selection
from sklearn import tree
from sklearn import pipeline
from sklearn import metrics
from sklearn import ensemble

import scikitplot as skplt

import lightgbm


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

# DBTITLE 1,Transformando os dados errados
imputer_minus_100 = imputation.ArbitraryNumberImputer(arbitrary_number=-100,
                                                          variables=missing_minus_100)

imputer_0 = imputation.ArbitraryNumberImputer(arbitrary_number=0,
                                                  variables=missing_0)

# COMMAND ----------

# DBTITLE 1,Modelo
model = tree.DecisionTreeClassifier()

# COMMAND ----------

# DBTITLE 1,Passando o modelo no PipeLine 
model_pipeline = pipeline.Pipeline([("Imputer -100", imputer_minus_100),
                                        ("Imputer 0", imputer_0),
                                        ("LGBM Model", model),
                                        ])  

# COMMAND ----------

# DBTITLE 1,Ajustando o Modelo
model_pipeline.fit(X_train, y_train)


# COMMAND ----------

# DBTITLE 1,Predicao
model_pipeline.predict(X_test)
