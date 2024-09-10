SHOW VARIABLES LIKE "local_infile";

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/artworks.csv'
INTO TABLE mia_inventory.artworks FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exhibits.csv'
INTO TABLE mia_inventory.exhibits FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/departments.csv'
INTO TABLE mia_inventory.departments FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/exhibit_art.csv'
INTO TABLE mia_inventory.exhibit_artwork FIELDS TERMINATED BY ','
ENCLOSED BY '"' LINES TERMINATED BY '\n';