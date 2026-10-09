CREATE OR REPLACE DATABASE MEETUP_DB;

-- 2. Crear el esquema para los datos crudos (sin procesar)
CREATE OR REPLACE SCHEMA MEETUP_DB.RAW;

-- 3. Asegurar que estamos posicionados en el contexto correcto
USE DATABASE MEETUP_DB;
USE SCHEMA RAW;

-- Crear un contenedor interno (Stage) para subir los archivos de Kaggle
CREATE OR REPLACE STAGE meetup_stage;

