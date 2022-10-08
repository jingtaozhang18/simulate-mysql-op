-- init databases
CREATE DATABASE IF NOT EXISTS db_world;

USE db_world;

CREATE TABLE IF NOT EXISTS `persons` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `age` INT(11),
  `sum` INT(11),
  PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;

CREATE TABLE IF NOT EXISTS `person_infos` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `person_id` INT(11),
  `sub` INT(11),
  `info` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (id),
  FOREIGN KEY (person_id) REFERENCES persons(id)
) ENGINE = InnoDB DEFAULT CHARSET = latin1;

begin;

INSERT INTO
  persons(age, sum)
VALUES
  (10, 100),
  (20, 200),
  (30, 300);

INSERT INTO
  person_infos(person_id, sub, info)
VALUES
  (1, 20, '1_a'),
  (1, 30, '1_b'),
  (1, 50, '1_c'),
  (2, 200, '2_a'),
  (3, 300, '3_a');

commit;

CREATE TABLE db_world.debezium_signal (id varchar(64), type varchar(32), data varchar(2048));
