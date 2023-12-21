CREATE TABLE Prize
(
  year INT NOT NULL,
  category VARCHAR NOT NULL,
  id INT NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE Organization
(
  organizationName VARCHAR NOT NULL,
  organizationCountry VARCHAR NOT NULL,
  organizationCity VARCHAR NOT NULL,
  PRIMARY KEY (organizationName)
);

CREATE TABLE Laureate
(
  laureateID INT NOT NULL,
  fullName VARCHAR NOT NULL,
  gender VARCHAR NOT NULL,
  born DATE NOT NULL,
  organizationName VARCHAR NOT NULL,
  PRIMARY KEY (laureateID),
  FOREIGN KEY (organizationName) REFERENCES Organization(organizationName)
);

CREATE TABLE PrizeLaureates
(
  id INT NOT NULL,
  laureateID INT NOT NULL,
  PRIMARY KEY (id, laureateID),
  FOREIGN KEY (id) REFERENCES Prize(id),
  FOREIGN KEY (laureateID) REFERENCES Laureate(laureateID)
);
