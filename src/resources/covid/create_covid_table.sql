-- Table: public.COVID-DATA-TBL

-- DROP TABLE IF EXISTS public."COVID-DATA-TBL";

CREATE TABLE IF NOT EXISTS public."COVID_DATA_TBL"
(
    "ID" integer NOT NULL,
    "Date" date,
    "City" text COLLATE pg_catalog."default",
    "Active_cases" integer CONSTRAINT positive_active_cases check("Active_cases" >= 0),
    "Active_today" integer CONSTRAINT positive_active_today check("Active_cases" >= 0),
    "Death_cases" integer CONSTRAINT positive_death_cases check("Active_cases" >= 0),
    "Death_today" integer CONSTRAINT positive_death_today check("Active_cases" >= 0),
    CONSTRAINT "COVID-DATA-TBL_pkey" PRIMARY KEY ("ID")
)

TABLESPACE pg_default;

ALTER TABLE IF EXISTS public."COVID-DATA-TBL"
    OWNER to postgres;