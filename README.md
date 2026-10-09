# prueba_rappi
Prueba técnica RappiPay

## Sets de datos
En este prueba se entregan nueve set de datos a través de la página https://www.kaggle.com/megelon/meetup :
Cities.csv
Categories.csv
Events.csv
Groups.csv
Groups_topics.csv
Members.csv
Members_topics.csv
Topics.csv
Venues.csv
Descarga de archivos
Unos de los primeros pasos es crear el catalogo MEETUP_DB y el esquema RAW a través del script crear_db_raw_scheme.sql
Se creo un stage interno llamado meetup_stage evidenciado en el mismo script **crear_db_raw_scheme.sql** .

<img width="911" height="895" alt="image" src="https://github.com/user-attachments/assets/828405fb-059f-4afe-90e0-d0a7d47fdc3b" />

De acuerdo a lo que se ve en la imagen, se intentó subir los archivos de manera manual en el stage creado, donde el único que no se puede subir es el members.csv debido a que por le método manual de upload el limite máximo es de 250 MB, se desarrollo un cuaderno en python **cargar_archivos_meetup.ipynb** ejecutado desde Jupyter Notebook,cargando de manera satisfactoria todos los archivos tal como se evidencia en la imagen:

<img width="921" height="468" alt="image" src="https://github.com/user-attachments/assets/11b7cd5e-436d-4672-af8c-399166bb8aae" />

Después se procede a la creación de tablas físicas auxiliares en el esquema Raw para cargar los archivos disponibles en el stage MEETUP_STAGE, se evidencia en el script **crear_tablas_raw.sql**.

## Mover los datos del Stage a las tablas crudas (RAW)
Una vez que termina de ejecutar de las tablas creadas en RAW, se pasa la información ejecutando los comandos COPY INTO en (por ejemplo, COPY INTO CATEGORIES_RAW FROM @MEETUP_STAGE/categories.csv FILE_FORMAT = (FORMAT_NAME = 'csv_meetup_format') ON_ERROR = 'CONTINUE';). Antes de esto, teniendo en cuenta que los archivos están comprimidos en .gz, le agregaremos el parámetro COMPRESSION = 'GZIP' para que Snowflake los descomprima al vuelo mientras los lee, se crea un formato llamado csv_meetup_format. Este paso se evidencia en el script **insertar_raw.sql**.

## Planificando Orquestación con Apache Airflow
Se debió  instalar Docker Desktop y luego instalar Astronomer desde Windows Shell 
- winget install Astronomer.Astro
Después se crea una carpeta llamada prueba_snowflake_airflow desde la terminal
mkdir prueba_snowflake_airflow
cd prueba_snowflake_airflow
Después de acceder en esta carpeta se ejecuta en la terminal el siguiente comando:
astro dev init
Esto creará automáticamente una estructura de carpetas en tu directorio. Las más importantes son la carpeta dags/ y el archivo requirements.txt.

<img width="921" height="547" alt="image" src="https://github.com/user-attachments/assets/1b25c3ca-67ce-419f-ab64-06d77a5c701f" />

Se abre el archivo requirements.txt que se acaba de crear y se pegan estas líneas para que Airflow pueda conectarse a Snowflake y Slack:
-apache-airflow-providers-snowflake apache
-airflow-providers-slack
Entrar a la carpeta llamada dags/ que se generó dentro del proyecto.

<img width="921" height="273" alt="image" src="https://github.com/user-attachments/assets/ce8f7772-5c38-461a-8bcf-a43740f0fa72" />

Se crea un archivo nuevo de Python llamado **meetup_dag.py** donde se usa MERGE cada 15 minutos y las  alertas para los procesos y que se muestren en Slack usando Airflow .El DAG se llamará automation_meetup_snowflake.
Para simular de manera realista que llegan "nuevos datos" cada 15 minutos en un entorno estático, utilizaremos una estrategia de ingeniería avanzada, donde se crearán los siguientes objetos:
Esquema: 
MEETUP_DB.PROCESSED
Tablas:
MEETUP_DB.PROCESSED.DIM_GROUPS
MEETUP_DB.PROCESSED.DIM_MEMBERS
MEETUP_DB.PROCESSED.EVENTS_ANALYTICS

Además, se diseñó un esquema analítico maduro que incluye tablas de hechos de comportamiento (FACT_MEMBERSHIP_ENGAGEMENT), Tablas Puente de relación N:M (DIM_TOPICS_BRIDGE) y Tablas de Agregación de Geo-Marketing (AGG_VENUE_GEOMARKETING).

### FACT_MEMBERSHIP_ENGAGEMENT (Tabla de Hechos de Comportamiento)
Une la tabla masiva de miembros con sus intereses y sus grupos. Permite responder preguntas de negocio como: ¿Qué ciudades tienen los usuarios más activos? o ¿Los miembros con biografías extensas participan en más grupos?.
### DIM_TOPICS_BRIDGE (Estructura Muchos a Muchos Desnormalizada)
En los datos de Kaggle, los intereses (topics) están separados de los grupos. Esta tabla puente los unifica. 
### AGG_VENUE_GEOMARKETING (Tabla de Agregación de Locaciones)
Esta es una tabla Agregada (capa Gold). No guarda transacciones, guarda resúmenes listos para que herramientas como PowerBI o Tableau hagan mapas de calor. Calcula el promedio de estrellas y volumen de eventos de cada establecimiento.
Las tablas se poblarán de manera incremental cada 15 minutos con muestras del 10, 15 , o  20  % según el caso, utilizando  MERGE, a partir de las tablas temporales generadas de las tablas ingestadas en el esquema RAW.

Después de crear el archivo .py se enciende Airflow con el siguiente comando , ejecutado en la terminal. 
astro dev start
Despues de esto se crea la conexión de snowflake en Admin->  Connections con los siguientes parámetros:
o	Nombre conexión: snowflake_default
o	Conn Type: Snowflake
o	Login: FFOREROA
o	Password: XXXXXX
o	Account: pabyqwc-zq79803
o	Warehouse: COMPUTE_WH
o	Database: MEETUP_DB
o	Schema: RAW
En el DAG, automation_meetup_snowflake se hace  clic en el botón de "Play" a la derecha para ejecutar tu primera prueba de automatización en caliente.

<img width="921" height="490" alt="image" src="https://github.com/user-attachments/assets/59a1f406-112a-4765-add4-08621df72405" />

## Alertas Slack

### Configuraciones Slack

Primero se debe crear un espacio de trabajo llamado Prueba Tecnica Rappi y después un canal llamado #alertas-pipeline 
Despues de esto se debe crear el bot en slack – Nueva app-  llamado Airflow Bot donde también nos pedirá datos como el worskpace creado y el canal creado en el paso anterior -donde van llegar los mensajes, despue de esto se generara un Webhook URL que nos servirá de mucha ayuda para tenerlo en el código donde estamos implementado en DAG.

Se modifica el archivo donde se crea el DAG creando una función nativa para enviar la alerta a slack y un paso para esta alerta después de ejecuta el paso para ejecutar las tablas físicas relacionada con las dimensiones y los eventos.
Se realiza una prueba con la siguiente evidencia:

<img width="921" height="386" alt="image" src="https://github.com/user-attachments/assets/951ae5d6-63e8-4f88-ae4f-b298f6b21b0b" />

<img width="921" height="340" alt="image" src="https://github.com/user-attachments/assets/28d4531d-404b-4824-be7f-f8c23da909c1" />

## Exportación de tablas procesadas en S3.

En la cuenta gratuita de AWS se crea un bicket llamado  **meetup-processed-fforero** , y se genera un  key_id y una clave secreta , indispensables para el paso que sigue.

Despues de esto en Snowflake se ejecutan unos pasos en el **script cargar_s3.sql** donde se exportan las tablas procesadas primero se crea un stage llamado MEETUP_DB.PROCESSED.AWS_S3_FINAL_STAGE donde se configurará el bucket , la key_id y la clave secreta.
Despues se exportan los tres archivos a a S3 por medio de un COPY INTO CON EL formato creado en RAW llamado **csv_meetup_format**.

<img width="921" height="376" alt="image" src="https://github.com/user-attachments/assets/4e314e77-c5a7-4232-b75e-e4315793532a" />


<img width="921" height="399" alt="image" src="https://github.com/user-attachments/assets/bfc40f69-348f-40ae-9c15-278c26058180" />

<img width="921" height="417" alt="image" src="https://github.com/user-attachments/assets/71a4ed23-7438-49e8-9574-f20cb9fa7abb" />

Estas tareas para para exportar a S3 se pueden automatizar en el DAG.

## Monitoreo de inserción DAG cada quince minutos:

Se desarrolla el script llamado monitor_task.sql e inclusive se adjunta las siguientes evidencias de ejecución cada 15 minutos del DAG.

## Evidencias

<img width="921" height="534" alt="image" src="https://github.com/user-attachments/assets/f53542c3-12c2-4a43-a3fc-d8b3edba4cf1" />

<img width="921" height="365" alt="image" src="https://github.com/user-attachments/assets/4eeb1660-0f91-48dc-b3a3-e3d7e7992427" />


<img width="921" height="372" alt="image" src="https://github.com/user-attachments/assets/b6e0ce44-bdbf-44a3-aa4d-1e236d4b7e53" />

<img width="921" height="405" alt="image" src="https://github.com/user-attachments/assets/624573fb-e0a4-4bef-92c3-e556e1be5594" />


<img width="921" height="422" alt="image" src="https://github.com/user-attachments/assets/9601bb43-71ac-4975-b162-db345e5eaef1" />

<img width="921" height="397" alt="image" src="https://github.com/user-attachments/assets/fbb5f35d-97dd-482f-8f3b-c1d9bf642473" />

<img width="921" height="428" alt="image" src="https://github.com/user-attachments/assets/df9b79b9-d331-4484-a995-f4984ecc213e" />

<img width="921" height="444" alt="image" src="https://github.com/user-attachments/assets/61c99f19-46c7-4d60-8961-163aff6f260a" />

<img width="921" height="504" alt="image" src="https://github.com/user-attachments/assets/1ce61cc2-8086-4eba-8674-ca465dccbabe" />

<img width="921" height="440" alt="image" src="https://github.com/user-attachments/assets/7fe6beb9-5f19-4539-ada1-9af499201efd" />

<img width="921" height="371" alt="image" src="https://github.com/user-attachments/assets/b5345867-2e75-405f-8dd0-4b1989a05155" />

<img width="921" height="383" alt="image" src="https://github.com/user-attachments/assets/d738464b-8de7-42b3-b591-7c2447a95a9b" />

<img width="921" height="356" alt="image" src="https://github.com/user-attachments/assets/498d02c2-8031-4fcd-ab58-2d7afd6d8b31" />















