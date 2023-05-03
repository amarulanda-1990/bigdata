# Databricks notebook source
df = spark.read.table('supermarket_sales___sheet1_4_csv')
display(df)

# COMMAND ----------

# Tendencia central: son aquellas que describen lo que es típico en el estudio de datos. Corresponden a los valores que se ubican en la parte central de un conjunto de datos y ayudan a resumir los datos con un sólo dato.

# COMMAND ----------

#MEDIA: también conocida como promedio, es el valor que se obtiene al dividir la suma de un conjunto de números entre la cantidad de ellos.
import matplotlib.pyplot as plt
from pyspark.sql.functions import avg
df = spark.read.table('supermarket_sales___sheet1_4_csv')
media_ingreso = df.select(avg("gross income")).collect()[0][0]
print("gross income", media_ingreso)


# COMMAND ----------

#MEDIANA: Es el número intermedio de un grupo de números; es decir, la mitad de los números son superiores a la mediana y la mitad de los números tienen valores menores que la mediana.
import matplotlib.pyplot as plt
from pyspark.sql.functions import avg
df = spark.read.table('supermarket_sales___sheet1_4_csv')
mediana_ingreso = df.approxQuantile("gross income", [0.5], 0)[0]
print("gross income", mediana_ingreso)

# COMMAND ----------

#MODA: Es el valor que aparece con mayor frecuencia en un conjunto de datos
import matplotlib.pyplot as plt
from pyspark.sql.functions import avg
df = spark.read.table('supermarket_sales___sheet1_4_csv')
moda_ingreso = df.groupBy("gross income").count().sort(("count")).collect()[0][0]
print("gross income", moda_ingreso)

# COMMAND ----------

#Medidas de dispersión (desviacion estandar, varianza)
#Las medidas de dispersión son estadísticos que nos dan información al respecto de la variabilidad o separación de los datos generalmente respecto a la media, Por ejemplo, si la desviación estándar es alta, esto sugiere que los datos están muy dispersos. Si la varianza es baja, esto sugiere que los datos están agrupados cerca de la media.

import matplotlib.pyplot as plt
from pyspark.sql.functions import mean, stddev, variance

#lectura de la tabla y calculo del valor_medio respecto a los ingresos brutos
df = spark.read.table(' supermarket_sales___sheet1_4_csv')
valor_medio=df.select(avg("gross income")).collect()[0][0]
print("Valor medio:", valor_medio)

#calculo de los valores minimo y maximo y resultado
valor_minimo = df.agg({"gross income": "min"}).collect()[0][0]
valor_maximo = df.agg({"gross income": "max"}).collect()[0][0]
print("Valor mínimo:", valor_minimo)
print("Valor máximo:", valor_maximo)

#calculo de la desviacion estandar y resultado
desviacion_estandar = df.select(stddev(df['gross income'])).collect()[0][0]
print("Desviación Estandar:", desviacion_estandar)

#calculo de la varianza y resultado
#otra forma es: varianza = df.agg({'gross income': 'variance'}).collect()[0][0]
varianza = df.select(variance(df['gross income'])).collect()[0][0]
print("Varianza:", varianza)

# COMMAND ----------

# MAGIC %sql
# MAGIC --Seleccionar todas las columnas de una tabla llamada "supermarketsales_2_csv":
# MAGIC SELECT * FROM supermarket_sales___sheet1_4_csv;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Seleccionar todas las filas de la tabla "supermarketsales_2_csv" donde el valor de la columna "Gender" es "Female":
# MAGIC SELECT * FROM supermarket_sales___sheet1_4_csv WHERE Gender = 'Female';

# COMMAND ----------

# MAGIC %sql
# MAGIC --Seleccionar todas las filas de la tabla "supermarketsales_2_csv" donde el valor de la columna "Total" es mayor a 1000:
# MAGIC SELECT * FROM supermarket_sales___sheet1_4_csv WHERE Total > 1000;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Calcular el número de ventas y el promedio de calificación de cada sucursal:
# MAGIC SELECT Branch, COUNT(*) AS Num_Ventas, AVG(Rating) AS Promedio_Calificacion
# MAGIC FROM supermarket_sales___sheet1_4_csv
# MAGIC GROUP BY Branch;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Calcular el total de ingresos por cada linea de producto:
# MAGIC SELECT `Product line`, SUM(Total) AS Total_Ingresos
# MAGIC FROM supermarket_sales___sheet1_4_csv
# MAGIC GROUP BY `Product line`;

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC SELECT payment, COUNT(*) as conteo
# MAGIC FROM supermarket_sales___sheet1_4_csv
# MAGIC GROUP BY Payment
# MAGIC ORDER BY conteo DESC
# MAGIC
# MAGIC --Con este query podemos saber cuales son los medios de pago mas utilizados.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Product line`, MAX(`Tax 5%`) as max_tax
# MAGIC FROM supermarket_sales___sheet1_4_csv
# MAGIC GROUP BY `Product line`
# MAGIC ORDER BY max_tax DESC
# MAGIC
# MAGIC --Con este query podemos saber cuales son los productos con impuestos mas altos ordenados de mayor a menor

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `Customer type`, `Gender`, COUNT(*) AS Gender_Count
# MAGIC FROM supermarket_sales___sheet1_4_csv
# MAGIC GROUP BY `Customer type`, `Gender`;
# MAGIC
# MAGIC --Cantidad de tipo de clientes (Miembro, Normales)organizados por genero

# COMMAND ----------

# MAGIC %sql
# MAGIC --Mostrar el total de ventas de cada ciudad:
# MAGIC SELECT City, SUM(Total) AS Total_Sales
# MAGIC FROM supermarket_sales___sheet1_4_csv
# MAGIC GROUP BY City;
