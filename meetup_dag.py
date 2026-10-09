from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

# 1. Función nativa en Python para enviar la alerta a Slack 
def send_slack_notification_via_api():
    webhook_url = "https://hooks.slack.com/services/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"
    payload = {
        "text": "🚀 *¡Pipeline de Meetup Completado con Éxito!* Los datos han sido procesados de forma incremental en Snowflake cada 15 minutos."
    }
    headers = {"Content-Type": "application/json"}
    
    response = requests.post(webhook_url, json=payload, headers=headers)
    
    if response.status_code == 200:
        print("¡Mensaje enviado a Slack con éxito!")
    else:
        raise ValueError(f"Fallo al enviar mensaje a Slack. Código: {response.status_code}, Respuesta: {response.text}")

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'automation_meetup_snowflake',
    default_args=default_args,
    description='Pipeline ELT incremental ejecutado cada 15 minutos con alertas HTTP',
    schedule='*/15 * * * *', 
    catchup=False,
    max_active_runs=1
) as dag:

    # Tarea 1: El proceso de transformación y MERGE en Snowflake
    transform_and_merge = SQLExecuteQueryOperator(
        task_id='transform_and_merge_data',
        conn_id='snowflake_default',
        sql="""
        CREATE SCHEMA IF NOT EXISTS MEETUP_DB.PROCESSED;
        
        -- 1. Crear la tabla de dimensión enriquecida en el esquema analítico
        CREATE TABLE IF NOT EXISTS MEETUP_DB.PROCESSED.DIM_GROUPS (
            group_id INT PRIMARY KEY,
            group_name VARCHAR(255),
            category_name VARCHAR(150),
            city_name VARCHAR(150),
            country_code VARCHAR(10),
            total_members INT,
            group_rating NUMBER(3,2),
            organizer_name VARCHAR(150),
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );

        -- 2. Proceso ELT Creativo Avanzado para tu DAG (Simulando actualizaciones incrementales)
        CREATE OR REPLACE TEMPORARY TABLE MEETUP_DB.PROCESSED.STG_GROUPS_BATCH AS
        SELECT * FROM 
        (SELECT 
            g.group_id,
            g.group_name,
            g.category_name,
            g.city as city_name,
            g.country as country_code,
            g.members as total_members,
            g.rating as group_rating,
            COALESCE(g.organizer_name, 'Sin asignar') as organizer_name
        FROM MEETUP_DB.RAW.GROUPS_RAW g
        QUALIFY ROW_NUMBER() OVER (PARTITION BY g.group_id ORDER BY g.group_id DESC) = 1 
        )
        SAMPLE (20); -- Tomamos un lote del 20% para la automatización de los 15 minutos

        -- 3. Consolidación incremental mediante MERGE
        MERGE INTO MEETUP_DB.PROCESSED.DIM_GROUPS target
        USING MEETUP_DB.PROCESSED.STG_GROUPS_BATCH source
        ON target.group_id = source.group_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.group_name = source.group_name,
                target.total_members = source.total_members,
                target.group_rating = source.group_rating,
                target.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (group_id, group_name, category_name, city_name, country_code, total_members, group_rating, organizer_name)
            VALUES (source.group_id, source.group_name, source.category_name, source.city_name, source.country_code, source.total_members, source.group_rating, source.organizer_name);


         -- 1. Asegurar que la tabla definitiva exista
        CREATE TABLE IF NOT EXISTS MEETUP_DB.PROCESSED.DIM_MEMBERS (
            member_id     INT PRIMARY KEY,
            member_name   VARCHAR(255),
            city          VARCHAR(150),
            country       VARCHAR(10),
            member_status VARCHAR(50),
            joined_date   DATE,
            has_bio       BOOLEAN,
            processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );

        -- 2. CORREGIDO: Eliminar duplicados en el origen usando QUALIFY antes del SAMPLE
        CREATE OR REPLACE TEMPORARY TABLE MEETUP_DB.PROCESSED.STG_MEMBERS_BATCH AS
        SELECT * FROM (
            SELECT 
                member_id,
                COALESCE(TRIM(member_name), 'Anonymous Member') AS member_name,
                UPPER(TRIM(city)) AS city,
                LOWER(TRIM(country)) AS country,
                COALESCE(LOWER(member_status), 'unknown') AS member_status,
                TO_DATE(joined) AS joined_date,
                CASE WHEN bio IS NOT NULL AND LENGTH(TRIM(bio)) > 0 THEN TRUE ELSE FALSE END AS has_bio
            FROM MEETUP_DB.RAW.MEMBERS_RAW
            WHERE member_id IS NOT NULL
            -- Truco de ingeniería: Si el ID está repetido, se queda solo con el primero que encuentre
            QUALIFY ROW_NUMBER() OVER (PARTITION BY member_id ORDER BY joined DESC) = 1
        ) SAMPLE (15); -- Muestreo aplicado de forma segura sobre registros únicos

        -- 3. El MERGE ahora correrá de forma impecable sin riesgo de colisión
        MERGE INTO MEETUP_DB.PROCESSED.DIM_MEMBERS target
        USING MEETUP_DB.PROCESSED.STG_MEMBERS_BATCH source
        ON target.member_id = source.member_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.member_name = source.member_name,
                target.city = source.city,
                target.member_status = source.member_status,
                target.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (member_id, member_name, city, country, member_status, joined_date, has_bio)
            VALUES (source.member_id, source.member_name, source.city, source.country, source.member_status, source.joined_date, source.has_bio);


        
        CREATE TABLE IF NOT EXISTS MEETUP_DB.PROCESSED.EVENTS_ANALYTICS (
        event_id                     VARCHAR(50) PRIMARY KEY,
        event_name                   VARCHAR(255),
        event_status                 VARCHAR(255),
        group_name                   VARCHAR(255),
        category_name                VARCHAR(255),
        venue_name                   VARCHAR(255),          
        venue_city                   VARCHAR(150),
        venue_state                  VARCHAR(255),
        fee_amount                   NUMBER(10,2), -- Aumentado a 10 para mayor seguridad con montos largos  
        maybe_rsvp_count             INT, 
        duration_minutes             INT,
        estimated_potential_revenue  NUMBER(10,2),
        group_engagement_rate        NUMBER(5,2), 
        is_premium_event             BOOLEAN,
        rsvp_occupancy_rate          NUMBER(5,2),
        processed_at                 TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
    );

    CREATE OR REPLACE TEMPORARY TABLE MEETUP_DB.PROCESSED.STG_EVENTS_BATCH AS
        SELECT * FROM (
            SELECT 
                e.event_id,
                e.event_name,
                e.event_status,
                g.group_name,
                g.category_name,
                v.venue_name,
                v.city as venue_city,
                v.state as venue_state,
                e.fee_amount,
                e.maybe_rsvp_count,
                e.duration / 60 AS duration_minutes,
                CASE 
                    WHEN e.fee_amount > 0 THEN (e.maybe_rsvp_count * e.fee_amount)
                    ELSE 0 
                END AS estimated_potential_revenue,
                CASE 
                    WHEN g.members > 0 THEN ROUND((e.maybe_rsvp_count / g.members) * 100, 2)
                    ELSE 0 
                END AS group_engagement_rate,
                CASE WHEN e.fee_amount > 0 THEN TRUE ELSE FALSE END as is_premium_event,
                CASE WHEN e.rsvp_limit > 0 THEN ROUND((e.maybe_rsvp_count / e.rsvp_limit) * 100, 2) ELSE 0.00 END as rsvp_occupancy_rate
            FROM MEETUP_DB.RAW.EVENTS_RAW e
            INNER JOIN MEETUP_DB.RAW.GROUPS_RAW g ON e.group_id = g.group_id
            LEFT JOIN MEETUP_DB.RAW.VENUES_RAW v ON e.venue_id = v.venue_id
            WHERE e.event_name IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY e.event_id ORDER BY e.event_id DESC) = 1
        ) SAMPLE (10);  

    MERGE INTO MEETUP_DB.PROCESSED.EVENTS_ANALYTICS target
    USING MEETUP_DB.PROCESSED.STG_EVENTS_BATCH source
    ON target.event_id = source.event_id
    WHEN MATCHED THEN
        UPDATE SET 
            target.event_name = source.event_name,
            target.event_status = source.event_status,
            target.group_name = source.group_name,
            target.category_name = source.category_name,
            target.venue_name = source.venue_name,
            target.venue_city = source.venue_city,
            target.venue_state = source.venue_state,
            target.fee_amount = source.fee_amount,
            target.maybe_rsvp_count = source.maybe_rsvp_count,
            target.duration_minutes = source.duration_minutes,
            target.estimated_potential_revenue = source.estimated_potential_revenue,
            target.group_engagement_rate = source.group_engagement_rate,
            target.is_premium_event = source.is_premium_event,
            target.rsvp_occupancy_rate = source.rsvp_occupancy_rate,
            target.processed_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        -- CORREGIDO: Mapeo explícito y completo de todas las columnas del INSERT
        INSERT (
            event_id, event_name, event_status, group_name, category_name, 
            venue_name, venue_city, venue_state, fee_amount, maybe_rsvp_count, 
            duration_minutes, estimated_potential_revenue, group_engagement_rate, 
            is_premium_event, rsvp_occupancy_rate
        )
        VALUES (
            source.event_id, source.event_name, source.event_status, source.group_name, source.category_name, 
            source.venue_name, source.venue_city, source.venue_state, source.fee_amount, source.maybe_rsvp_count, 
            source.duration_minutes, source.estimated_potential_revenue, source.group_engagement_rate, 
            source.is_premium_event, source.rsvp_occupancy_rate
        );
        
        -- 1. Crear la tabla de hechos unificada en PROCESSED
        CREATE TABLE IF NOT EXISTS MEETUP_DB.PROCESSED.FACT_MEMBERSHIP_ENGAGEMENT (
            member_id           INT,
            group_id            INT,
            member_name         VARCHAR(255),
            group_name          VARCHAR(255),
            city_name           VARCHAR(150),
            days_since_joined   INT,
            is_active_member    BOOLEAN,
            processed_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (member_id, group_id)
        );

        -- 2. Carga por lotes con eliminación de duplicados para tu DAG
        CREATE OR REPLACE TEMPORARY TABLE MEETUP_DB.PROCESSED.STG_FACT_MEMBERSHIP AS
        SELECT * FROM (
            SELECT 
                m.member_id,
                m.group_id,
                COALESCE(m.member_name, 'Anonymous') AS member_name,
                g.group_name,
                UPPER(m.city) AS city_name,
                -- Calcular la antigüedad del miembro en días desde que se unió
                DATEDIFF('day', m.joined, CURRENT_DATE()) AS days_since_joined,
                CASE WHEN m.member_status = 'active' THEN TRUE ELSE FALSE END AS is_active_member
            FROM MEETUP_DB.RAW.MEMBERS_RAW m
            INNER JOIN MEETUP_DB.RAW.GROUPS_RAW g ON m.group_id = g.group_id
            QUALIFY ROW_NUMBER() OVER (PARTITION BY m.member_id, m.group_id ORDER BY m.joined DESC) = 1
        ) sub
        SAMPLE (10);

        -- 3. Ingesta Incremental mediante MERGE
        MERGE INTO MEETUP_DB.PROCESSED.FACT_MEMBERSHIP_ENGAGEMENT target
        USING MEETUP_DB.PROCESSED.STG_FACT_MEMBERSHIP source
        ON target.member_id = source.member_id AND target.group_id = source.group_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.member_name = source.member_name,
                target.group_name = source.group_name,
                target.days_since_joined = source.days_since_joined,
                target.is_active_member = source.is_active_member,
                target.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (member_id, group_id, member_name, group_name, city_name, days_since_joined, is_active_member)
            VALUES (source.member_id, source.group_id, source.member_name, source.group_name, source.city_name, source.days_since_joined, source.is_active_member);

       -- 1. Crear la tabla puente analítica
        CREATE TABLE IF NOT EXISTS MEETUP_DB.PROCESSED.DIM_TOPICS_BRIDGE (
            group_id       INT,
            topic_id       INT,
            group_name     VARCHAR(255),
            topic_name     VARCHAR(255),
            category_name  VARCHAR(150),
            processed_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
            PRIMARY KEY (group_id, topic_id)
        );

        -- 2. Query de transformación para tu DAG
        CREATE OR REPLACE TEMPORARY TABLE MEETUP_DB.PROCESSED.STG_TOPICS_BRIDGE AS
        SELECT * FROM (
            SELECT 
                gt.group_id,
                gt.topic_id,
                g.group_name,
                t.topic_name,
                g.category_name
            FROM MEETUP_DB.RAW.GROUPS_TOPICS_RAW gt
            INNER JOIN MEETUP_DB.RAW.GROUPS_RAW g ON gt.group_id = g.group_id
            INNER JOIN MEETUP_DB.RAW.TOPICS_RAW t ON gt.topic_id = t.topic_id
            QUALIFY ROW_NUMBER() OVER (PARTITION BY gt.group_id, gt.topic_id ORDER BY gt.group_id) = 1
        ) sub
        SAMPLE (20);

        -- 3. Merge de Consolidación
        MERGE INTO MEETUP_DB.PROCESSED.DIM_TOPICS_BRIDGE target
        USING MEETUP_DB.PROCESSED.STG_TOPICS_BRIDGE source
        ON target.group_id = source.group_id AND target.topic_id = source.topic_id
        WHEN MATCHED THEN
            UPDATE SET target.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (group_id, topic_id, group_name, topic_name, category_name)
            VALUES (source.group_id, source.topic_id, source.group_name, source.topic_name, source.category_name);
      
              -- 1. Crear la tabla puente analítica
       
        
        -- 1. Tabla agregada final
        CREATE TABLE IF NOT EXISTS MEETUP_DB.PROCESSED.AGG_VENUE_GEOMARKETING (
            venue_id           INT PRIMARY KEY,
            venue_name         VARCHAR(255),
            city_name          VARCHAR(150),
            state_code         VARCHAR(50),
            total_events_held  INT,
            avg_venue_rating   NUMBER(3,2),
            total_rsvps_hosted INT,
            processed_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        );

        -- 2. Query de agregación analítica (Usa GROUP BY en lugar de SAMPLE para asegurar consistencia métrica)
        CREATE OR REPLACE TEMPORARY TABLE MEETUP_DB.PROCESSED.STG_VENUE_AGG AS
        SELECT 
            v.venue_id,
            COALESCE(v.venue_name, 'Establecimiento Privado/Virtual') AS venue_name,
            v.city AS city_name,
            v.state AS state_code,
            COUNT(e.event_id) AS total_events_held,
            AVG(v.rating) AS avg_venue_rating,
            SUM(COALESCE(e.maybe_rsvp_count, 0)) AS total_rsvps_hosted
        FROM MEETUP_DB.RAW.VENUES_RAW v
        LEFT JOIN MEETUP_DB.RAW.EVENTS_RAW e ON v.venue_id = e.venue_id
        WHERE v.venue_id IS NOT NULL
        GROUP BY v.venue_id, v.venue_name, v.city, v.state;

        -- 3. Sincronización analítica
        MERGE INTO MEETUP_DB.PROCESSED.AGG_VENUE_GEOMARKETING target
        USING MEETUP_DB.PROCESSED.STG_VENUE_AGG source
        ON target.venue_id = source.venue_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.total_events_held = source.total_events_held,
                target.avg_venue_rating = source.avg_venue_rating,
                target.total_rsvps_hosted = source.total_rsvps_hosted,
                target.processed_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN
            INSERT (venue_id, venue_name, city_name, state_code, total_events_held, avg_venue_rating, total_rsvps_hosted)
            VALUES (source.venue_id, source.venue_name, source.city_name, source.state_code, source.total_events_held, source.avg_venue_rating, source.total_rsvps_hosted);
       
       COPY INTO @MEETUP_DB.PROCESSED.AWS_S3_FINAL_STAGE/pipeline_output_dim_brigde
       FROM MEETUP_DB.PROCESSED.DIM_TOPICS_BRIDGE 
       FILE_FORMAT = (FORMAT_NAME = 'MEETUP_DB.RAW.csv_meetup_format')
       HEADER = TRUE
       OVERWRITE = TRUE;
       
       
        """
    )

    # Tarea 2: Alerta automatizada hacia Slack usando PythonOperator (Estrategia Blindada)
    send_slack_alert = PythonOperator(
        task_id='send_slack_notification',
        python_callable=send_slack_notification_via_api
    )

    # Definir el orden del pipeline
    transform_and_merge >> send_slack_alert
