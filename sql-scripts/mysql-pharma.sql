DROP SCHEMA IF EXISTS pharmaStudent;

CREATE DATABASE pharmaStudent;

USE pharmaStudent;

CREATE TABLE `pharmacy` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `storeNumber` varchar(6) NOT NULL,
  `storeChainName` varchar(25) NOT NULL DEFAULT 'CVS',
  `storeName` varchar(50) NOT NULL,
  `streetAddress` varchar(100) NOT NULL,
  `phone` varchar(12) NOT NULL,
  `city` varchar(25) NOT NULL,
  `state` char(2) NOT NULL,
  `zip` char(5) NOT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `storeNumber_UNIQUE` (`storeNumber`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=latin1;

INSERT INTO pharmacy
(storeNumber, storeChainName, storeName, streetAddress, phone, city, state, zip)
VALUES
('1001', 'CVS', 'CVS Belle Hall', '1000 Belle Hall Ave', '853-901-0432', 'Mount Pleasant', 'SC', '29464')
, ('1002', 'CVS', 'CVS Summerville 1', '111 Jones Ave', '853-222-2222', 'Summerville', 'SC', '29485')
, ('2000', 'Walgreens', 'Shops of Wando', '100 Wando Blvd', '853-222-3333', 'Wando', 'SC', '29488')
, ('4532', 'Walmart', 'Walmart 4532 Hwy 17 N', '2000 Wally World Road', '853-222-3333', 'Mount Pleasant', 'SC', '29466')
, ('3030', 'Walmart', 'Walmart 3030 Hwy 51 ', '123 Hwy 51', '843 212-2343', 'Goose Creek', 'SC', '29456');


CREATE TABLE `patient` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `patientNumber` varchar(6) NOT NULL,
  `firstName` varchar(25) NOT NULL,
  `lastName` varchar(50) NOT NULL,
  `birthdate` date NOT NULL,
  `gender` char(1) NOT NULL,
  `ethnicity` varchar(2) NOT NULL,
  `last4SSN` char(4) NOT NULL DEFAULT '-1',
  PRIMARY KEY (`ID`),
  UNIQUE KEY `patientNumber_UNIQUE` (`patientNumber`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;


INSERT INTO patient
(patientNumber, firstName, lastName, birthdate, gender, ethnicity, last4SSN)
VALUES
('100', 'William', 'Jefferson', '1956-01-01', 'M', 'H', "2021")
, ('101', 'Willa', 'Roof', '1938-01-02', 'F', 'W', "4837")
, ('102', 'Jeff', 'Cave', '1969-02-05', 'M', 'H', '-1')
, ('103', 'Steve', 'Perry', '1956-05-11', 'M', 'W', "2321")
, ('104', 'Collins', 'Bootsy', '1952-06-11', 'M', 'AA', "3982");

CREATE TABLE `patientCondition` (
  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `patientID` int(10) unsigned NOT NULL,
  `condition` varchar(50) NOT NULL,
  PRIMARY KEY (`ID`),
  KEY `FK_patientCondition_patientID_idx` (`patientID`),
  CONSTRAINT `FK_patientCondition_patientID` FOREIGN KEY (`patientID`) REFERENCES `patient` (`ID`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO patientCondition
(patientID, `condition`)
VALUES
(5, 'hypertension')
, (5, 'diabeties')
, (5, 'ulcers')
, (3, 'hypertension')
, (2, 'hypoglycemia')
, (2, 'anxiety');


CREATE TABLE med (
  ID int(11) unsigned NOT NULL AUTO_INCREMENT,
  name varchar(50) NOT NULL,
  label varchar(75) NOT NULL,
  amount int(10) unsigned NOT NULL,
  unit varchar(4) NOT NULL,
  form enum('syrup','patch','tablet') NOT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY label_UNIQUE (label)
);

INSERT INTO med
(name, label, amount, unit, form)
VALUES
 ('Nicorette', 'Nicorette 100mg patch', '100', 'mg', 'patch')
,('Nicorette', 'Nicorette 200mg patch', '200', 'mg', 'patch')
,('Humira', 'Humira 100mg tablet', '100', 'mg', 'tablet')
,('Nexium', 'Nexium 20mg tablet', '20', 'mg', 'tablet')
,('Nexium', 'Nexium 10mg tablet', '10', 'mg', 'tablet')
,('Lortab', 'Lortab 100mg tablet', '100', 'mg', 'tablet')
,('Lortab', 'Lortab 200mg tablet', '200', 'mg', 'tablet');


CREATE TABLE medIngredient (
  ID int(11) unsigned NOT NULL AUTO_INCREMENT,
  medID int(11) unsigned NOT NULL,
  ingredient varchar(50) NOT NULL,
  PRIMARY KEY (ID),
  KEY FK_med_medID_idx (medID),
  CONSTRAINT FK_med_medID FOREIGN KEY (medID) REFERENCES med (ID) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO medIngredient (medID, ingredient)
VALUES
(3, 'adalimumab')
, (4, 'esomeprazole')
, (5, 'esomeprazole')
, (6, 'acetaminophen')
, (6, 'hydrocodone');
