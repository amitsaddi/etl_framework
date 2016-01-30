
CREATE OR REPLACE PROCEDURE PRC_TEST (A IN NUMBER DEFAULT 0)
AS
BEGIN
INSERT INTO TEST VALUES (seq_general.nextval,TO_CHAR(A|| '+' || A));
COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE PRC_TEST1_1 (A IN NUMBER DEFAULT 11)
AS
BEGIN
INSERT INTO TEST VALUES (seq_general.nextval,TO_CHAR(A|| '+' || A));
COMMIT;
END;
/

CREATE OR REPLACE PROCEDURE PRC_TEST2 (A IN NUMBER DEFAULT 2)
AS
BEGIN
INSERT INTO TEST VALUES (seq_general.nextval,TO_CHAR(A|| '+' || A));
COMMIT;
END;
/


create or replace procedure prc_log 
(V_ETL_PROCESS_ID IN NUMBER , V_MSG IN VARCHAR2, v_srno in number default NULL)
AS
BEGIN
case 
when upper(v_msg) = 'INITIATE' THEN
  INSERT INTO ETL_PROCESS_MASTER_LOG (SRNO,ETL_PROCESS_ID ,BATCH_ID, PROCEDURE_NAME, START_TIME)
    VALUES (SEQ_GENERAL.NEXTVAL, V_ETL_PROCESS_ID , -1, 'PRC_STARTUP',SYSDATE);
when upper(v_msg) = 'END' THEN
  UPDATE ETL_PROCESS_MASTER_LOG 
    SET END_TIME = SYSDATE WHERE ETL_PROCESS_ID = v_ETL_PROCESS_ID;
ELSE
  UPDATE ETL_PROCESS_MASTER_LOG
  SET STATUS = v_MSG
  WHERE ETL_PROCESS_ID = v_ETL_PROCESS_ID
  and srno = v_srno;
  
  
END CASE;
COMMIT;
END;
/


CREATE OR REPLACE PROCEDURE PRC_STARTUP(BATCH_ID IN NUMBER DEFAULT -1)
AS
V_ETL_ID NUMBER ;
v_srno NUMBER := NULL;
BEGIN
  select seq_etl.nextval into v_etl_id from dual;
-- GET ALL VALID PROCEDURES TO BE EXECUTED 
  prc_log(v_etl_id, 'initiate' , v_srno );
begin 
  INSERT INTO ETL_PROCESS_MASTER_LOG
  SELECT SEQ_GENERAL.NEXTVAL,V_ETL_ID,  	
  BATCH_ID , BATCH_SEQ_NO, PROCEDURE_NAME, PROCEDURE_TYPE, IS_VALID, NULL, NULL, DEPENDENT_BATCHID, NULL, NULL, NULL, NULL
  FROM ETL_PROCESS_MASTER
  WHERE UPPER(IS_VALID) = 'Y';
  commit ;
end;
dbms_output.put_line ('ETL id = '|| v_etl_id);

-- LOOP THRU THESE RECORDS TO EXECUTE EACH PROCEDURE ONE BY ONE ASYNC'ly
declare
	cursor c_etl is
	select SRNO, BATCH_ID, BATCH_SEQ_NO, DEPENDENT_BATCHID, PROCEDURE_NAME, PROCEDURE_TYPE
	FROM ETL_PROCESS_MASTER_LOG WHERE
	ETL_PROCESS_ID = v_etl_id 
	and batch_id <> -1
	order by BATCH_ID, BATCH_SEQ_NO;
	v_job_name VARCHAR2(100);
	v_prc_name ETL_PROCESS_MASTER_LOG.PROCEDURE_NAME%TYPE;
/* for every record
-- Implement business rules
-- Validate dependencies 
-- execute the proc
	*/

BEGIN 
	FOR c1 in c_etl loop
	v_prc_name := c1.procedure_name;
  v_job_name := 'JOB_'||V_ETL_ID||'_'||c1.BATCH_ID||'_'||v_prc_name;
	-- ?? log execution here ??
	dbms_output.put_line ('initiating job');
dbms_output.put_line (v_prc_name);
dbms_output.put_line (v_job_name);

	dbms_scheduler.create_job ( 
		job_name => v_job_name
    ,job_type => 'STORED_PROCEDURE'
		, job_action => v_prc_name
		, start_date => sysdate
		--, number_of_arguments => 0
		, enabled => TRUE
	  , AUTO_DROP => true
		, comments => 'sample job'); 

END LOOP;

END;

	prc_log(v_etl_id, 'end', v_srno );

END;
/

