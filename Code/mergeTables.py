import os
import pandas as pd
import numpy as np
import pandas as pd
import os
import gc
import time
import xlrd

def joinRawTables(tables):
    # Iteramos por cada tabla:
    for table in tables:
        if (table not in mergedTables):
            print(table)
            # Cogemos el identificador de la tabla:
            identificador = table.replace(".csv","")[-4:]
            if os.path.exists(os.path.join(dataPath, identificador +".csv")):
                continue

            # Cogemos las tablas que contengan el identificador
            toMerge = [t for t in tables if identificador in t]

            # Ordenamos las tablas:
            toMerge.sort()
            
            # Y añadimos cada tabla:
            merged = pd.read_csv(os.path.join(rawPath,toMerge[0]))

            if "Acc_Index" in merged.columns:
                merged.rename(columns ={"Acc_Index":"Accident_Index"},
                                        inplace = True)
            
            for table in toMerge[1:]:
                partialTable = pd.read_csv(os.path.join(rawPath,table))
                if "Acc_Index" in partialTable.columns:
                    partialTable.rename(columns ={"Acc_Index":"Accident_Index"},
                                        inplace = True)
                merged = merged.merge(partialTable,
                                      how = "outer",
                                      on = "Accident_Index")
                
                

            # Las añadimos a la lista de tablas:
            mergedTables.extend(toMerge)
            merged.to_csv(os.path.join(dataPath, identificador +".csv"), sep = ";",
                                       index = False)
            del merged
            gc.collect()
            
            print(len(dictsList))
    result = pd.DataFrame.from_dict(dictsList)

    result.to_csv("result.csv", sep = ";", index = False)



def mergeAllTables(mergedTables,
                   dataPath):

    finalTable = pd.DataFrame()
    
    # Y las unimos entre ellas:
    for table in mergedTables:
        
        # leemos la tabla:
        print(table)
        t = pd.read_csv(os.path.join(dataPath, table), sep = ";")

        finalTable = finalTable.append(t, ignore_index = True)

    finalTable.to_csv("finalTable.csv", sep = ";", index = False)

def renameColumns():
    # Leemos también el archivo con las variables. Cada pestaña es el nombre de 
    # la columna, y tiene dos campos: code y label. Lo que queremos hacer es
    # reemplazar los valores de code por los de label en cada columna:
    xls = xlrd.open_workbook(r'variable lookup.xls', on_demand=True)

    # Tomamos todos los nombres de las columnas:
    columnasXls = xls.sheet_names()

    # El único problema es que los nombres de las páginas están separados por 
    # espacios y los del dataset por "_". También por minúsculas. Lo cambiaremos:
    columnasXlsCompare = [c.replace(" ","_").lower() for c in columnasXls]

    # Leemos los datos unidos:
    allData = pd.read_csv("finalTable.csv",sep = ";")

    # Iteraremos para cada columna del dataframe
    for columnaDf in allData.columns:

        # Si la columna es alguna de las columnas de las pestañas...
        if columnaDf.lower() in columnasXlsCompare:
            print(columnaDf.lower())

            # Cargamos la página en concreto:
            try:
                sheet = pd.read_excel("variable lookup.xls",
                                    sheet_name = columnaDf.replace("_"," ").strip(),
                                            )
            except:
                sheet = pd.read_excel("variable lookup.xls",
                                    sheet_name = columnaDf
                                            )

            sheet.columns = [c.lower() for c in sheet.columns]

            # Creamos el diccionario:
            diccionario = {sheet["code"].iloc[i]: sheet["label"].iloc[i] for i in range(len(sheet["label"]))}
            # Y cambiamos los valores:
            allData = allData.replace({columnaDf:diccionario})
            
    # Guardamos:
    allData.to_csv("processed.csv", sep = ";", index = False)
            

# La ruta donde guardaremos los datos
dataPath = os.path.abspath(os.path.join(
    os.pardir, "Data"))

# La ruta donde hay los archivos descargados
rawPath = os.path.abspath(os.path.join(
    dataPath, "Raw"))
 
# Cogemos las tablas que hemos unido en las funciones anteriores
mergedTables = [t for t in os.listdir(dataPath) if t.endswith(".csv") and not t.startswith("0")]
print(mergedTables)
# La lista de tablas:
tables = np.array([f for f in os.listdir(rawPath) if f.endswith(".csv")])


mergeAllTables(mergedTables, dataPath)

# La lista con las tablas ya unidas:
mergedTables = []

dictsList = []


