CREATE PROCEDURE StandardDev()

LANGUAGE SQL

BEGIN

DECLARE SQLSTATE CHAR(5) DEFAULT '00000';

DECLARE sal DECIMAL(20,5);

DECLARE sum_sal DECIMAL(20,5);

DECLARE sum_salsq DECIMAL(20,5);

DECLARE mean_sq DECFLOAT(34);

DECLARE mean_salsq DECFLOAT(34);

DECLARE stddev DECIMAL(20,9);

DECLARE count_sal DECIMAL(10,5);

DECLARE c CURSOR FOR SELECT SALARY FROM EMPLOYEE;

SET sum_sal = 0;

SET sum_salsq = 0;

SET count_sal = 0;

OPEN c;

FETCH FROM c INTO sal;

WHILE(SQLSTATE = '00000') DO

SET sum_sal = sum_sal + sal;

SET sum_salsq = sum_salsq + POWER(sal ,2) ; 

SET count_sal = count_sal+1;

FETCH FROM c INTO sal;

END WHILE;

CLOSE c;

SET mean_sq = POWER(CAST((sum_sal /count_sal)AS DECFLOAT(34)),2);

SET mean_salsq = sum_salsq/count_sal ;

SET stddev = CAST(SQRT(mean_salsq - mean_sq)AS DECFLOAT(34)) ;

CALL DBMS_OUTPUT.PUT_LINE('Standard Deviation : '|| stddev);  

END@

SET SERVEROUTPUT ON@

CALL StandardDev()@

SET SERVEROUTPUT OFF@
