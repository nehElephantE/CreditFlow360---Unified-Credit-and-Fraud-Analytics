
CREATE DATABASE IF NOT EXISTS creditflow360 
CHARACTER SET utf8mb4 
COLLATE utf8mb4_unicode_ci;

USE creditflow360;

GRANT ALL PRIVILEGES ON reditflow360.* TO 'creditflow_user'@'%' IDENTIFIED BY 'creditflow_pass';
GRANT ALL PRIVILEGES ON creditflow360.* TO 'creditflow_user'@'localhost' IDENTIFIED BY 'creditflow_pass';

FLUSH PRIVILEGES;