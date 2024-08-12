# -*- coding: utf-8 -*-
"""
Materia     : Laboratorio de datos - FCEyN - UBA
Tema        : Trabajo Práctico 01
Integrantes : Marcos Rostan
              Valentín Rozenblit
              Agustín Dramis
Objetivo    : existencia de relación entre los flujos monetarios netos de Inversión Extranjera Directa (IED) de cada 
              país y la cantidad de sedes en el exterior que tiene Argentina en dicho país.
"""
# IMPORTANTE: Modificar la variable carpeta para adecuarse a la 
# ubicación del script y los archivos.


import pandas as pd
from inline_sql import sql, sql_val
import matplotlib.pyplot as plt
import seaborn as sns


carpeta = "C:/Users/Marcos/Desktop/TP1-LDD/" # Carpeta Marcos
carpeta = "~/Documentos/Labo/Cursadas/Labo de Datos/" # Carpeta Agustin

lista_sedes = pd.read_csv(carpeta+"lista-sedes.csv")

lista_secciones = pd.read_csv(carpeta+"lista-secciones.csv")

ied = pd.read_csv(carpeta+"flujos-monetarios-netos-inversion-extranjera-directa.csv").T.reset_index()
ied.columns = ied.iloc[0] # Usar la primera fila como encabezados de columna
ied = ied.drop(ied.index[0]) # Eliminar la primera fila (que ahora es el encabezado de columna original)
ied.columns = ['nombre' if x=='indice_tiempo' else "año" + str(x).split("-")[0] for x in ied.columns]
paises = pd.read_csv(carpeta+"paises.csv")
paises.rename(columns=lambda x: x.strip(), inplace=True)

lista_sedes_datos = pd.read_csv(carpeta+"lista-sedes-datos.csv", sep=',', on_bad_lines="skip")

# Simplificacion del nombre para poder asociar a las tablas de IED
paises = sql^"""
                SELECT *, 
                REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(LOWER(nombre), 'ñ', 'nn'),' ','_'),'ú','u'),'ó','o'),'í','i'),'é','e'),'á','a') AS nombre_simplificado

                FROM  paises
             """

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
###Generacion de tablas de modelo relacional

# Tabla pais (iso3, nombre)

pais = sql^  """
                SELECT iso3, nombre
                FROM paises
             """

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Tabla sede (sede_id, region_geografica, cantidad_secciones, 
# iso3 como foreign key)

sedes_con_cantidad_de_secciones   = sql^"""
                 SELECT sede_id, 
                        COUNT(*) AS cantidad_secciones
                 FROM lista_secciones
                 GROUP BY sede_id
             """

sede = sql^"""
           SELECT secc.*, dat.region_geografica, dat.pais_iso_3 AS iso3, dat.sede_desc_castellano AS nombre_sede
           FROM sedes_con_cantidad_de_secciones AS secc
           LEFT JOIN lista_sedes_datos AS dat
           ON secc.sede_id=dat.sede_id
           """

sede.dropna(inplace=True) # Elimina las filas NaN 

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Tabla Red (url, tipo_red, sede_id como foreign key)

#Selecciono redes con url valida (no @algo, no NaNs)

tabla_sedes_con_redes = sql^"""
                            SELECT dat.sede_id, dat.redes_sociales
                            FROM lista_sedes_datos AS dat
                            WHERE dat.redes_sociales LIKE '%.com%'
                            """

# El split es en base a "  //  ", notense los espacios para 
# diferenciarlo del 'https://' 
tabla_sedes_con_redes['redes_sociales'] = tabla_sedes_con_redes['redes_sociales'].str.split('  //  ')

# explode() pasa la columna de redes de lista amultiples filas
red = tabla_sedes_con_redes.explode('redes_sociales')

# Analisis previo

redes_paginas_validas = sql^"""
                            SELECT *
                           
                            FROM red
                            WHERE redes_sociales LIKE '%.com%'
                            """

expresiones_red = []

for i in redes_paginas_validas['redes_sociales']:
    expresion = i[0:str.find(i,'.com')]
    expresiones_red.append(expresion)

expresiones_red = set(expresiones_red)

# Las redes obtenidas fueron Facebook, Twitter, Flickr, Youtube, Instagram y Linkedin

# Extraigo red, pero surge de observacion de la tabla

red =                   sql^"""
                            SELECT sede_id, redes_sociales AS url,
                            CASE WHEN redes_sociales LIKE '%twitter%' THEN 'Twitter'
                                 WHEN redes_sociales LIKE '%youtube%' THEN 'Youtube'
                                 WHEN redes_sociales LIKE '%facebook%' THEN 'Facebook'
                                 WHEN redes_sociales LIKE '%flickr%' THEN 'Flickr'
                                 WHEN redes_sociales LIKE '%linkedin%' THEN 'Linkedin'
                                 WHEN redes_sociales LIKE '%instagram%' THEN 'Instagram'
                            END AS tipo_red
                           
                            FROM red
                            WHERE redes_sociales LIKE '%.com%'
                            """
red.dropna(inplace=True) # Elimina las filas NaN 

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# Tabla Flujo monetario (año, monto, iso3 como foreign key)

flujo_monetario = sql^ """
                        SELECT nombre, año1980 AS monto, 1980 AS año
                        FROM ied
                       """
for i in range(1981,2023):
    columna_año = "año" + str(i)
    flujo_monetario = sql^ """
                        SELECT nombre, """ + columna_año + """ AS monto, $i AS año
                        FROM ied
                        UNION
                        SELECT *
                        FROM flujo_monetario
                       """

flujo_monetario = sql^"""
                        SELECT mon.monto, mon.año, paises.iso3
                        FROM flujo_monetario AS mon
                        LEFT JOIN paises
                        ON mon.nombre=paises.nombre_simplificado
                      """
flujo_monetario.dropna(inplace = True) # Elimina las filas NaN

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# (h)
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# i)
# Para cada país informar cantidad de sedes, cantidad de secciones en
# promedio que poseen sus sedes y el flujo monetario neto de Inversión
# Extranjera Directa (IED) del país en el año 2022. El orden del reporte debe
# respetar la cantidad de sedes (de manera descendente). En caso de empate,
# ordenar alfabéticamente por nombre de país. 

# Consulta para contar el número de sedes por país
cant_sedes_por_pais = sql^"""
                            SELECT iso3, COUNT(sede_id) AS sedes
                            FROM sede
                            GROUP BY iso3
                        """

# Consulta para calcular el promedio de la cantidad de secciones por país
prom_secciones_por_pais = sql^"""
                            SELECT iso3, AVG(cantidad_secciones) AS secciones_promedio
                            FROM sede
                            GROUP BY iso3
                            """

# Consulta para seleccionar el flujo monetario para el año 2022 por país
flujo_por_pais_2022 = sql^"""
                            SELECT iso3, monto
                            FROM flujo_monetario
                            WHERE año = 2022
                        """

# Consulta para combinar la cantidad de sedes por país con el promedio de secciones por país
cant_con_prom = sql^"""
                    SELECT cant_sedes_por_pais.iso3, sedes, secciones_promedio
                    FROM cant_sedes_por_pais
                    INNER JOIN prom_secciones_por_pais
                    ON cant_sedes_por_pais.iso3 = prom_secciones_por_pais.iso3
                """

# Consulta para agregar el flujo monetario para el año 2022 a los resultados anteriores
cant_prom_flujo2022 = sql^"""
                            SELECT cant_con_prom.iso3, sedes, secciones_promedio, monto AS IED_2022_M_U$S
                            FROM cant_con_prom
                            INNER JOIN flujo_por_pais_2022
                            ON cant_con_prom.iso3 = flujo_por_pais_2022.iso3
                        """

# Consulta final para recuperar los resultados finales, incluyendo el nombre del país, el número de sedes, 
# el promedio de secciones y el flujo monetario para el año 2022
resultado_1 = sql^"""
                    SELECT nombre AS pais, sedes, secciones_promedio, IED_2022_M_U$S
                    FROM cant_prom_flujo2022
                    INNER JOIN pais
                    ON cant_prom_flujo2022.iso3 = pais.iso3
                    ORDER BY sedes DESC, pais
                 """


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# ii) Reportar agrupando por región geográfica: a) la cantidad de países en que
# Argentina tiene al menos una sede y b) el promedio del IED del año 2022 de
# esos países (promedio sobre países donde Argentina tiene sedes). Ordenar
# de manera descendente por este último campo.

# a)
# Consulta para contar la cantidad de países en los que Argentina tiene al menos una sede, agrupados por región geográfica
cant_paises_con_sedes_argentinas = sql^"""
                                    SELECT region_geografica, COUNT (DISTINCT iso3) AS Paises_con_sedes_Argentinas
                                    FROM sede
                                    GROUP BY region_geografica
                                   """
# b)
# Consulta para seleccionar el flujo monetario para el año 2022 por país
flujo_por_pais_2022 = sql^"""
                            SELECT iso3, monto
                            FROM flujo_monetario
                            WHERE año = 2022
                        """

# Consulta para obtener la región geográfica, el país y el flujo monetario para el año 2022
region_con_pais_y_flujo = sql^"""
                                SELECT sede.iso3, region_geografica, monto
                                FROM sede
                                INNER JOIN flujo_por_pais_2022
                                ON sede.iso3 = flujo_por_pais_2022.iso3
                              """

# Consulta para calcular el promedio del IED del año 2022 por región geográfica
prom_IED_2022_por_region = sql^"""
                                SELECT region_geografica, AVG(monto) AS prom_IED_2022
                                FROM region_con_pais_y_flujo
                                GROUP BY region_geografica
                               """

# Consulta final para recuperar los resultados finales, incluyendo la región geográfica, la cantidad de países 
# con sedes argentinas y el promedio del IED del año 2022 para esos países, ordenados de manera descendente por el 
# promedio del IED
resultado_2 = sql^"""
                    SELECT prom_IED_2022_por_region.region_geografica, Paises_con_sedes_Argentinas, prom_IED_2022
                    FROM prom_IED_2022_por_region
                    INNER JOIN cant_paises_con_sedes_argentinas
                    ON prom_IED_2022_por_region.region_geografica = cant_paises_con_sedes_argentinas.region_geografica
                    ORDER BY prom_IED_2022 DESC
                  """


#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#iii)Para saber cuál es la vía de comunicación de las sedes en cada país, nos hacemos la siguiente pregunta:
# ¿Cuán variado es, en cada el país, el tipo de redes sociales que utilizan las sedes? Se espera como respuesta 
# que para cada país se informe la cantidad de tipos de redes distintas utilizadas. Por ejemplo, si en Chile 
# utilizan 4 redes de facebook, 5 de instagram y 4 de twitter, el valor para Chile debería ser 3 (facebook, 
# instagram y twitter).

# Consulta para combinar la sede id y el país del cual proviene (utilizando INNER JOIN)
sede_con_su_pais = sql^"""
                          SELECT sede_id, nombre AS nombre_pais
                          FROM pais
                          INNER JOIN sede
                          ON pais.iso3 = sede.iso3
                        """
                                
# Consulta para combinar (sin filas repetidas) el país con su/s rede/s (utilizando INNER JOIN) 
pais_con_su_red = sql^"""
                        SELECT DISTINCT nombre_pais, tipo_red
                        FROM red
                        INNER JOIN sede_con_su_pais
                        ON red.sede_id = sede_con_su_pais.sede_id
                    """
# Consulta final para contar la cantidad de redes distintas que tiene cada país.
resultado_3 = sql^"""
                    SELECT nombre_pais AS pais, COUNT(tipo_red) AS cantidad_red
                    FROM pais_con_su_red
                    GROUP BY pais
                  """

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


#iv) Confeccionar un reporte con la información de redes sociales, donde se
#indique para cada caso: el país, la sede, el tipo de red social y url utilizada.
#Ordenar de manera ascendente por nombre de país, sede, tipo de red y
#finalmente por url.

redes_con_sedes = sql^"""
                      SELECT sede.nombre_sede,sede.iso3, red.*
                      FROM red
                      INNER JOIN sede
                      ON red.sede_id=sede.sede_id
                      """

resultado_4 = sql^"""
                  SELECT pais.nombre, rcs.nombre_sede, rcs.tipo_red, rcs.url
                  FROM redes_con_sedes AS rcs
                  INNER JOIN pais
                  ON rcs.iso3=pais.iso3
                  ORDER BY pais.nombre, rcs.nombre_sede, rcs.tipo_red, rcs.url
                  """

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
# (i)
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

#i) Cantidad de sedes por región geográfica. Mostrarlos ordenados de manera decreciente por dicha cantidad.

# Ordenar el dataframe de la cantidad de sedes por región geográfica de manera decreciente
resultado_2 = resultado_2.sort_values(by='Paises_con_sedes_Argentinas', ascending = False)

# Crear una figura y los ejes para el gráfico
fig, ax = plt.subplots()

# Establecer el tipo de letra 
plt.rcParams['font.family'] = 'sans-serif' 

# Crear un gráfico de barras utilizando el dataframe que contiene la cantidad de sedes que tiene cada región geográfica
ax.bar(data = resultado_2, x = 'region_geografica', height = 'Paises_con_sedes_Argentinas')

# Agregar título, etiquetas en los ejes y rotación de 90° de las regiones geográficas.
ax.set_title('Cantidad de sedes X Región geográfica')
ax.set_xlabel('Región')
ax.set_ylabel('Cantidad')
ax.set_xticklabels(resultado_2['region_geografica'], rotation = 90, fontsize = '8')

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# ii) Boxplot, por cada región geográfica, del valor correspondiente al promedio
# del IED (para calcular el promedio tomar los valores anuales
# correspondientes al período 2018-2022) de los países donde Argentina tiene
# una delegación. Mostrar todos los boxplots en una misma figura, ordenados
# por la mediana de cada región.


ieds_2018_2022 = sql^"""
                     SELECT iso3, AVG(monto) AS ied_promedio
                     FROM flujo_monetario
                     WHERE año<=2022 AND año>=2018
                     GROUP BY iso3"""

paises_con_region = sql^"""
                        SELECT DISTINCT iso3, region_geografica
                        FROM  sede
                        """

ieds_con_region = sql^"""
                      SELECT ieds.*, reg.region_geografica
                      FROM ieds_2018_2022 AS ieds
                      INNER JOIN paises_con_region AS reg
                      ON ieds.iso3=reg.iso3"""

grouped = ieds_con_region.groupby(["region_geografica"])
order = grouped.median()["ied_promedio"].sort_values().index

ax = sns.boxplot(x= "region_geografica",
                 y= "ied_promedio",
                 data=ieds_con_region,
                 order=order)

ax.set_title("IED promedio (período 2018 - 2022) por región geográfica")
ax.set_ylabel("IED promedio en período (M u$s)")
ax.set_xlabel("Región geográfica")
ax.set_xticklabels(order, rotation = 90, fontsize = '8')

ax.set_xticks(ieds_con_region['region_geografica'])


# Sin outliers, para observar mejor las deferencias

ax = sns.boxplot(x= "region_geografica",
                 y= "ied_promedio",
                 data=ieds_con_region,
                 order=order,
                 showfliers = False)

ax.set_title("IED promedio (período 2018 - 2022) por región geográfica")
ax.set_ylabel("IED promedio en período (M u$s)")
ax.set_xlabel("Región geográfica")
ax.set_xticklabels(order, rotation = 90, fontsize = '8')

ax.set_xticks(ieds_con_region['region_geografica'])

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# iii) Relación entre el IED de cada país (año 2022 y para todos los países que se
# tiene información) y la cantidad de sedes en el exterior que tiene Argentina
# en esos países.


fig, ax = plt.subplots()

ax.scatter(x= "sedes",
                 y= "IED_2022_M_U$S",
                 data=resultado_1)

ax.set_title("IED en función de la cantidad de sedes")
ax.set_xlabel("Cantidad de sedes diplomaticas")
ax.set_ylabel("Inversion extranjera directa (MU$S)")
