--set hive to accept partition dynamic
DROP DATABASE raw_data_adventure CASCADE;
DROP DATABASE data_adventure CASCADE;

CREATE DATABASE data_adventure;

CREATE DATABASE raw_data_adventure;

SET hive.exec.dynamic.partition.mode=nonstrict;