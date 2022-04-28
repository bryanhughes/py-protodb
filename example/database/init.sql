CREATE USER example_db WITH SUPERUSER PASSWORD 'example_db' CREATEDB;
GRANT ALL PRIVILEGES ON DATABASE example_db TO example_db;
CREATE EXTENSION if not exists "uuid-ossp";
CREATE EXTENSION if not exists "postgis";