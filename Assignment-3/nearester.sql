WITH FACILITY_COMBINED (FACILITYNAME, GEOLOCATION, DISTANCE, ATTRIBUTEVALUE ) AS 
(SELECT a.FACILITYNAME, db2gse.st_point(LONGITUDE, LATITUDE , 1) AS GEOLOCATION,
db2gse.st_distance(db2gse.st_point(LONGITUDE, LATITUDE , 1), db2gse.st_point(-72.993983, 40.824369, 1), 'STATUTE MILE'),
ATTRIBUTEVALUE
FROM CSE532.FACILITYORIGINAL a JOIN CSE532.FACILITYCERTIFICATION b ON a.FACILITYID = b.FACILITYID)
SELECT  FACILITYNAME, db2gse.st_astext(GEOLOCATION)AS GEOLOCATION, DISTANCE, ATTRIBUTEVALUE FROM FACILITY_COMBINED 
WHERE  DISTANCE< 25.0 
AND ATTRIBUTEVALUE = 'Emergency Department'
ORDER BY DISTANCE LIMIT 1;
