USE DATABASE MEETUP_DB;
USE SCHEMA RAW;

-- 2. Ejemplo para la tabla de Categorías (categories.csv)
CREATE OR REPLACE TABLE MEETUP_DB.RAW.CATEGORIES_RAW (
    category_id INT,
    category_name STRING,
    shortname STRING,
    sort_name STRING
);

CREATE OR REPLACE TABLE MEETUP_DB.RAW.CITIES_RAW (
    city STRING,
    city_id INT,
    
    country STRING,
    distance FLOAT,
    latitude FLOAT,
    localized_country_name STRING,
    longitude FLOAT,
    member_count INT,
    ranking INT,
    state STRING,
    zip INT
);

USE DATABASE MEETUP_DB;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE MEETUP_DB.RAW.EVENTS_RAW (
    -- Columnas de la primera captura (A - M)
    event_id         VARCHAR(50),
    created          TIMESTAMP,
    description      VARCHAR,
    duration         INT,
    event_url        VARCHAR(1000),
    fee_accepts      VARCHAR(100),
    fee_amount       NUMBER(10,2),
    fee_currency     VARCHAR(10),
    fee_descripti    VARCHAR(255),
    fee_label        VARCHAR(100),
    fee_required     INT,
    group_created    TIMESTAMP,
    group_group_lat  FLOAT,
    
    -- Columnas de la segunda captura (N - V)
    group_group_lon  FLOAT,
    group_id         INT,
    group_join_mode  VARCHAR(50),
    group_name       VARCHAR(255),
    group_urlname    VARCHAR(255),
    group_who        VARCHAR(100),
    headcount        INT,
    how_to_find_us   VARCHAR,
    maybe_rsvp_count INT,
    
    -- Nuevas columnas de la tercera captura (W - AH)
    event_name       VARCHAR(255),
    photo_url        VARCHAR(1000),
    rating_average   FLOAT,
    rating_count     INT,
    rsvp_limit       INT,
    event_status     VARCHAR(50),
    event_time       TIMESTAMP,
    updated          TIMESTAMP,
    utc_offset       INT,
    venue_address_1  VARCHAR(500),
    venue_address_2  VARCHAR(500),
    venue_city       VARCHAR(150),
     -- Seccion Final (AI - AT)
    venue_country                 VARCHAR(10),
    venue_id                      INT,
    venue_lat                     FLOAT,
    venue_localized_country_name  VARCHAR(100),
    venue_lon                     FLOAT,
    venue_name                    VARCHAR(255),
    venue_phone                   VARCHAR(50),
    venue_repinned                INT,
    venue_state                   VARCHAR(50),
    venue_zip                     INT,
    visibility                    STRING,
    waitlist_count                INT,
    why                           STRING,	
    yes_rsvp_count                INT
    
);

CREATE OR REPLACE TABLE GROUPS_RAW (
    group_id                 INT,
    category_id              INT,
    category_name            VARCHAR(150),
    category_shortname       VARCHAR(150),
    city_id                  INT,
    city                     VARCHAR(150),
    country                  VARCHAR(10),
    created                  TIMESTAMP,
    description              VARCHAR,
    group_photo_base_url     VARCHAR(1000),
    group_photo_highres_link VARCHAR(1000),
    group_photo_photo_id     INT,
    group_photo_photo_link   VARCHAR(1000),
    group_photo_thumb_link   VARCHAR(1000),
    group_photo_type         VARCHAR(50),
    join_mode                VARCHAR(50),
    lat                      FLOAT,
    link                     VARCHAR(1000),
    lon                      FLOAT,
    members                  INT,
    group_name               VARCHAR(255),
    organizer_member_id      INT,
    organizer_name           VARCHAR(150),
    organizer_photo_base_url VARCHAR(1000),
    organizer_photo_highres_link VARCHAR(1000),
    organizer_photo_photo_id     INT,
    organizer_photo_photo_link   VARCHAR(1000),
     organizer_photo_thumb_link   VARCHAR(1000),
    organizer_photo_type         VARCHAR(50),
    rating                       NUMBER(3,2),
    state                        VARCHAR(50),
    timezone                     VARCHAR(100),
    urlname                      VARCHAR(255),
    utc_offset                   INT,
    visibility                   VARCHAR(50),
    who                          VARCHAR(100)
    
);

CREATE OR REPLACE TABLE GROUPS_TOPICS_RAW (
    topic_id int,
    topic_key string,
    topic_name string,
    group_id int
);

CREATE OR REPLACE TABLE MEETUP_DB.RAW.MEMBERS_RAW (
    member_id     INT,
    bio           VARCHAR,
    city          VARCHAR(150),
    country       VARCHAR(10),
    hometown      VARCHAR(255),
    joined        TIMESTAMP,
    lat           FLOAT,
    link          VARCHAR(1000),
    lon           FLOAT,
    member_name   VARCHAR(255),
    state         VARCHAR(50),
    member_status VARCHAR(50),
    visited       TIMESTAMP,
    group_id      INT
    );

CREATE OR REPLACE TABLE MEETUP_DB.RAW.MEMBERS_TOPICS_RAW
(
topic_id  int,
topic_key string,
topic_name string,
member_id int
);

CREATE OR REPLACE TABLE MEETUP_DB.RAW.TOPICS_RAW
(
    topic_id      INT ,
    description   STRING,
    link          STRING,
    members       INT,
    topic_name    STRING,
    urlkey        STRING,
    main_topic_id INT
);

CREATE OR REPLACE TABLE MEETUP_DB.RAW.VENUES_RAW (
    venue_id               INT PRIMARY KEY,
    address_1              VARCHAR(500),
    city                   VARCHAR(150),
    country                VARCHAR(10),
    distance               FLOAT,
    lat                    FLOAT,
    localized_country_name VARCHAR(100),
    lon                    FLOAT,
    venue_name             VARCHAR(255),
    rating                 NUMBER(3,2),
    rating_count           INT,
    state                  VARCHAR(50),
    zip                    VARCHAR(20),
    normalised_rating      FLOAT
);

