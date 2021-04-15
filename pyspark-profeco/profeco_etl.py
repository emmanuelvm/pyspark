import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.session import SparkSession
from google.cloud import storage
import io
from datetime import date

current_date = date.today()

sc = SparkContext()
spark = SQLContext(sc)

file_path = 'gs://gnp-storage/Profeco/resources/Sin-fecha/profeco.pdf'

#Creating temporal table
data = spark.read.csv(file_path, header=True)
data.registerTempTable("profeco_table")

#Saving new CSV file
# data.coalesce(1).write.format("com.databricks.spark.csv").option("header", "false").save('gs://gnp-storage/Profeco/resources/Con-fecha/Profeco_' + str(current_date) + '.csv')

#Generating first query and store the result in a variable
num_rows_qry = "SELECT count(*) FROM profeco_table"
num_rows = spark.sql(num_rows_qry).collect()[0][0]

#Generating second query and store the result in a variable
num_cat_qry = "SELECT count(DISTINCT categoria) FROM profeco_table"
num_cat = spark.sql(num_cat_qry).collect()[0][0]

#Generating subquery as temp table
qry_temp = "SELECT estado, producto, COUNT(producto) AS Total_Producto FROM profeco_table WHERE estado IS NOT NULL GROUP BY estado,producto"
temp_res = spark.sql(qry_temp)
temp_res.registerTempTable("estado_producto")

#Creating query using subquery
prod_mas_mon_qry = """SELECT 
                        estado,MAX(Total_Producto) Total
                      FROM 
                        estado_producto 
                      WHERE 
                        estado IN(
                                    'AGUASCALIENTES','BAJA CALIFORNIA"','BAJA CALIFORNIA SUR','CAMPECHE','CHIAPAS',
                                    'CHIHUAHUA','COAHUILA DE ZARAGOZA','COLIMA','DISTRITO FEDERAL','DURANGO','GUANAJUATO',
                                    'GUERRERO','HIDALGO','JALISCO','MICHOACÁN DE OCAMPO','MORELOS','MÉXICO','NAYARIT',
                                    'NUEVO LEÓN','OAXACA','PUEBLA','QUERÉTARO','QUINTANA ROO','SAN LUIS POTOSÍ','SINALOA',
                                    'SONORA','TABASCO','TAMAULIPAS','TLAXCALA','VERACRUZ DE IGNACIO DE LA LLAVE','YUCATÁN','ZACATECAS'
                                    ) 
                      GROUP BY 
                        estado
                      ORDER BY 
                        estado ASC """

#Storing subquery as temp table 
prod_mas_mon = spark.sql(prod_mas_mon_qry)
prod_mas_mon.registerTempTable("estado_producto_top_n")

#Generating 3º query and store the result in a variable
finqry = """SELECT X.estado,Y.producto,X.Total FROM estado_producto_top_n AS X
            JOIN (SELECT * FROM estado_producto) Y
            ON X.estado = Y.estado AND X.Total = Y.Total_Producto
            ORDER BY X.estado ASC """

prod_mas_mon3 = spark.sql(finqry).collect()

#Generating 4º query and store the result in a variable
cadena_mayor_var_qry = "SELECT cadenaComercial, COUNT(DISTINCT producto) AS Total_Productos FROM profeco_table GROUP BY cadenaComercial ORDER BY COUNT(producto) DESC LIMIT 1"
cadena_mayor_var = spark.sql(cadena_mayor_var_qry).collect()

#Format for the text file report
table = """
Productos mas monitoreados por estado:

"""
for i in prod_mas_mon3:
    text = 'Estado:' + str(i[0]) + '\n' + 'Producto: ' + str(i[1]) + '\n' + 'Total: ' + str(i[2]) + '\n' + '-----------------------------------------\n'
    table = table + text

header = """
---------------------------------------------------------------------------
                                 REPORTE
---------------------------------------------------------------------------
"""

resp1 = 'Número de registros en total: ' + str(num_rows) + '\n\n'
resp2 = 'Número de categorías de productos: ' + str(num_cat) + '\n\n'
resp4 = 'Cadena con mayor variedad de productos : ' + str(cadena_mayor_var[0][0]) + '\n' + 'Cantidad productos: ' + str(cadena_mayor_var[0][1]) + '\n\n'
all_resp = header + resp1 + resp2 + resp4 + table

#Writing string in to text file in Google Cloud Storage
bucket = 'gnp-storage'
path = 'Profeco/Reportes/Reporte_' + str(current_date) + '.txt'

storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket)

destination_blob_pathname = path
blob = bucket.blob(destination_blob_pathname)

output = io.StringIO(all_resp)
blob.upload_from_string(output.read(), content_type="text/plain")

output.close()


