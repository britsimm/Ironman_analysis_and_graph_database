from pandas import read_csv
from neo4j import GraphDatabase
import time

# Conteo del tiempo para medir cuanto demora el procesamiento
start_time = time.time()

uri = "bolt://localhost:7687"
userName = "neo4j" 
password = "rootroot"

graphDB_Driver = GraphDatabase.driver(uri, auth=(userName, password))
file = '/Users/juanpellegrini/Documents/Maestría/df_completo.csv'

with graphDB_Driver.session() as graphDB_Session:
    df = read_csv(file, low_memory=False)
    largo = len(df.index) - 1

    for idx in range(1000,largo): 

        wanted_df_slice = df.iloc[[idx]]

        genero = str(wanted_df_slice.Gender.values[0])
        categoria = str(wanted_df_slice.AgeGroup.values[0])
        banda_categoria = str(wanted_df_slice.AgeBand.values[0])
        swimTime = str(wanted_df_slice.SwimTime.values[0])
        transition1Time = str(wanted_df_slice.Transition1Time.values[0])
        bikeTime = str(wanted_df_slice.BikeTime.values[0])
        transition2Time = str(wanted_df_slice.Transition2Time.values[0])
        runTime = str(wanted_df_slice.RunTime.values[0])
        finishTime = str(wanted_df_slice.FinishTime.values[0])
        eventYear = str(wanted_df_slice.EventYear.values[0])
        eventLocation = str(wanted_df_slice.EventLocation.values[0])
        lugar = str(wanted_df_slice.Lugar.values[0])
        country = str(wanted_df_slice.Country.values[0])

        print(idx)

        if str(categoria) == '00':
            print(categoria)
            categoria = 'NULL'
            print(categoria)
        if str(banda_categoria) == '00':
            banda_categoria = 'NULL'

        crear_participante  = "CREATE (p: Participante {Genero: $genero, Categoria: $categoria, Banda_Categoria: $banda_categoria, SwimTime: $swimTime, Transition1Time: $transition1Time, BikeTime: $bikeTime, Transition2Time: $transition2Time, RunTime: $runTime, FinishTime: $finishTime}) "
        relacion_pais_origen = "MERGE (n: Pais {Nombre_Pais: $country}) MERGE (p)-[w:ES_DE]-(n) "
        relacion_evento = "MERGE (e: Evento {Nombre_Evento:  $eventLocation, Año: $eventYear}) MERGE (p)-[s:PARTICIPO_EN]-(e) "
        relacion_lugar = "MERGE (l: Lugar {Nombre_Lugar:  $lugar}) MERGE (e)-[t:SE_CORRE_EN]-(l)"
      
        query = crear_participante + relacion_pais_origen + relacion_evento + relacion_lugar
        graphDB_Session.run(query, genero = genero, categoria = categoria, banda_categoria = banda_categoria, swimTime = swimTime,  transition1Time = transition1Time, bikeTime = bikeTime, transition2Time = transition2Time, runTime =runTime, finishTime = finishTime,  eventYear = eventYear, country = country, eventLocation= eventLocation, lugar = lugar)


# Conteo del tiempo final
end_time = time.time()

# Se imprime el tiempo total
execution_time = end_time - start_time
print('Tiempo de ejecucion de la funcion', execution_time)