WITH FACILITY_COMBINED (FACILITYNAME, GEOLOCATION, DISTANCE, ATTRIBUTEVALUE ) AS 
(SELECT a.FACILITYNAME, db2gse.st_astext(geolocation), 
db2gse.st_distance(geolocation, db2gse.st_point( 40.824369,-72.993983, 1), 'STATUTE MILE'),
ATTRIBUTEVALUE
FROM CSE532.FACILITY a JOIN CSE532.FACILITYCERTIFICATION b ON a.FACILITYID = b.FACILITYID)
SELECT  FACILITYNAME, GEOLOCATION, DISTANCE, ATTRIBUTEVALUE FROM FACILITY_COMBINED
WHERE  DISTANCE< 25.0 
AND ATTRIBUTEVALUE = 'Emergency Department'
ORDER BY DISTANCE LIMIT 1;