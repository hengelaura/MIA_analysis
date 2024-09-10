
DROP SCHEMA IF EXISTS mia_inventory;
CREATE SCHEMA mia_inventory;
USE mia_inventory;

SET NAMES UTF8MB4;
SET character_set_client = UTF8MB4;


CREATE TABLE departments (
    dept_name varchar(100),
    dept_id int,
    artwork_id int
);

CREATE TABLE artworks (
    accession_number varchar(50),
    artist varchar(750),
    classification varchar(100),
    continent varchar(100),
    country varchar(100),
    creditline varchar(900),
    curator_approval int,
    art_id int,
    medium varchar(700),
    style varchar(100),
    display int,
    height float,
    width float,
    depth float,
    start int,
    end int,
    age varchar(20)
);

CREATE TABLE exhibits (
    exhibit_id int,
    start_date date,
    end_date date,
    days int
);

CREATE TABLE exhibit_artwork (
    exhibit_id int,
    art_id int
);

