# LuisFLC para Grandata

## Ejercicio #1 resultados
- La facturación total por el servicio en el periodo que abarcan los datos es de: 1696022.5 $.
- 100 usuarios con mayor facturación. Los datos comprimidos en formato parquet se encuentran en la carpeta /top_100.
- El histograma de llamadas por hora del día se encuentra en la carpeta /work.

## Ejercicio #2
### 1. La empresa cuenta con un cluster on premise de Hadoop en el cual se ejecuta, tanto el data pipeline principal de los datos, como los análisis exploratorios de los equipos de Data Science y Data Engineering. Teniendo en cuenta que cada proceso compite por un número específico de recursos del cluster:
#### ¿Cómo priorizaría los procesos productivos sobre los de análisis exploratorios?
Primero me gustaría investigar la recurrencia y el peso en procesamiento que estos procesos abarcan. Dependiendo del peso que estos procesos representen para el día a día, es como priorizaría unos sobre otros. Por ejemplo, si los procesos productivos requieren un procesamiento en tiempo real, sin duda daría prioridad para la disponibilidad de recursos a estos procesos. Por el contrario, si los procesos productivos se ejecutan 1 vez cada día, se puede dar más peso a el análisis exploratorios y cuando el proceso productivo necesite ejecutarse, que se le asignen solo los recursos necesarios para su terminación y después se asignen esos recursos al análisis exploratorio.

#### Debido a que los procesos productivos del pipeline poseen un uso intensivo tanto de CPU como de memoria, ¿qué estrategia utilizaría para administrar su ejecución durante el día? ¿Qué herramientas de scheduling conoce para tal fin?
Ultimamente he estado aprendiendo sobre Airflow como herramienta de orquestración. Este tipo de herramientas te permiten programar la ejecución de tus pipelines para ciertos días, y ciertas horas, de manera semanal, mensual, etc. Si se tienen bien definidos los tiempos de ejecución de un pipeline productivo, es más fácil evitar problemas de recursos entre estos procesos y los procesos analíticos.

### 2. Existe una tabla del Data Lake con alta transaccionalidad, que es actualizada diariamente con un gran volumen de datos. Consultas que cruzan información con esta tabla ven afectada su performance en tiempos de respuesta.
#### Según su criterio, ¿cuáles serían las posibles causas de este problema?
Aquí pueden existir varias causas:
a) El código de las consultas esta extrayendo información innecesaria para el análisis. Por ejemplo, si el área de ventas utiliza el data lake para algunas consultas de solo registros más recientes, entonces lo mejor sería extraer directamente del Lake lo más reciente,en lugar de jalar todo y luego filtrar del lado de ventas.
d) Los recursos de las áreas de consultas no estan siendo bien distribuidos y utilizados. Si várias áreas utilizan los mismos recursos para sus consultas por ejemplo, también es importante poder poner límites para cada área o distribuir mejor los recursos con base a el peso de cada consulta.
#### Dada la respuesta anterior, ¿qué sugeriría para solucionarlo?
Lo más común o primero que se viene a la mente es aumentar el procesamiento de las máquinas, pero esto es una solución temporal. Lo mejor sería evaluar el peso de los recursos que cada consulta utiliza y distribuir los recursos totales a cada área con base en esto. También, es bien importante la manera en la que las consultas jalan los datos del Lake. Como en el ejemplo del equipo de ventas que solo ve información reciente, para ellos no sería viable extraer todo el lake o leer todo el lake primero sabiendo que solo quieren datos a partir de cierta fecha.

### 3. Imagine un clúster Hadoop de 3 nodos, con 50 GB de memoria y 12 cores por nodo. Necesita ejecutar un proceso de Spark que utilizará la mitad de los recursos del clúster, dejando la otra mitad disponible para otros jobs que se lanzarán posteriormente.
#### ¿Qué configuraciones en la sesión de Spark implementaría para garantizar que la mitad del clúster esté disponible para los jobs restantes?
Hay unos parámetros dentro de la función SparkSession.builder que sirven para asignar recursos a tu sesión. Los siguientes parámetros sirven para asignar recursos a cada nodo de tu ambiente. Si se desea solo utilizar la mitad del total de los recursos, esto debería funcionar:
    .config("spark.executor.memory", "25g") \
    .config("spark.executor.cores", "6") \

### 4. Histograma de llamadas por hora del día
<img src="https://github.com/luisferlc/DataEnGrande/blob/main/hours-calls-histogram.png">
