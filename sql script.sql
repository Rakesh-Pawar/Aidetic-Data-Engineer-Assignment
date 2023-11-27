-- create new database
CREATE DATABASE  earthquake_data;

-- use database
USE earthquake_data;

-- Create the neic_earthquakes table

DROP TABLE IF EXISTS neic_earthquakes;

CREATE TABLE neic_earthquakes (
    Date VARCHAR(255) DEFAULT NULL,
    Time VARCHAR(255) DEFAULT NULL,
    Latitude VARCHAR(255) DEFAULT NULL,
    Longitude VARCHAR(255) DEFAULT NULL,
    Type VARCHAR(255) DEFAULT NULL,
    Depth VARCHAR(255) DEFAULT NULL,
    Depth_Error VARCHAR(255) DEFAULT NULL,
    Depth_Seismic_Stations VARCHAR(255) DEFAULT NULL,
    Magnitude VARCHAR(255) DEFAULT NULL,
    Magnitude_Type VARCHAR(255) DEFAULT NULL,
    Magnitude_Error VARCHAR(255) DEFAULT NULL,
    Magnitude_Seismic_Stations VARCHAR(255) DEFAULT NULL,
    Azimuthal_Gap VARCHAR(255) DEFAULT NULL,
    Horizontal_Distance VARCHAR(255) DEFAULT NULL,
    Horizontal_Error VARCHAR(255) DEFAULT NULL,
    Root_Mean_Square VARCHAR(255) DEFAULT NULL,
    ID VARCHAR(255) DEFAULT NULL,
    Source VARCHAR(255) DEFAULT NULL,
    Location_Source VARCHAR(255) DEFAULT NULL,
    Magnitude_Source VARCHAR(255) DEFAULT NULL,
    Status VARCHAR(255) DEFAULT NULL
);