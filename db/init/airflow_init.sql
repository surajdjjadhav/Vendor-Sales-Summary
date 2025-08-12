CREATE DATABASE IF NOT EXISTS airflow_db;
CREATE USER IF NOT EXISTS 'airflow_user'@'%' IDENTIFIED BY 'airflow_password';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow_user'@'%';
FLUSH PRIVILEGES;
