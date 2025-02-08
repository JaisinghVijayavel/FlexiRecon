DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_fetch_reconinfo` $$

CREATE PROCEDURE `pr_fetch_reconinfo`
(
	in in_recon_code varchar(32),
	in in_user_code varchar(32)
)
me:BEGIN
	DECLARE current_table_name VARCHAR(255);
	Declare current_dataset_name VARCHAR(255);
	Declare current_dataset_type VARCHAR(255);
	Declare current_dataset_code VARCHAR(255);
	Declare current_last_job_gid VARCHAR(255);
	Declare current_scheduler_gid VARCHAR(255);
	Declare current_sno  VARCHAR(255); 
	Declare current_dataset_table_name VARCHAR(255);
	Declare current_dataset_table_field VARCHAR(255); 
	Declare current_pipeline_code VARCHAR(255);
	Declare current_pipeline_name VARCHAR(255);
	Declare current_dataset_code1 VARCHAR(255);
	Declare current_dataset_name1 VARCHAR(255);
	DECLARE done2 INT DEFAULT FALSE;
	DECLARE done1 INT DEFAULT FALSE;
	DECLARE done3 INT DEFAULT FALSE;
	Declare v_sql text default '';
	DECLARE v_count INT;
	DECLARE v_count1 INT;
	DECLARE v_count2 INT;
	Declare current_particular text default '';
  
	SET @row_number = 0;
	SET @row_number1 = 0;
	SET @row_number2 = 0; 
	SET @row_number3 = 0; 
  
		-- Dataset Summary
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tdataset;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tdatasetsummary;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_treconsummary;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipeline;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipelinevaildate;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipelinecount;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipelinefield;
          
		CREATE TEMPORARY TABLE recon_tmp_tdataset (
				dataset_code VARCHAR(32) NOT NULL,
                dataset_name VARCHAR(128) NOT NULL,
				dataset_table_name VARCHAR(128) NOT NULL,
                dataset_type VARCHAR(128) NOT NULL,
				last_job_gid VARCHAR(128) NOT NULL,
                scheduler_gid VARCHAR(128) NOT NULL,
				PRIMARY KEY (dataset_code, dataset_name, dataset_table_name,dataset_type,last_job_gid,scheduler_gid));
		CREATE TEMPORARY TABLE recon_tmp_tdatasetsummary (
				DatasetName VARCHAR(255) NOT NULL,
                Count INT,
                dataset_type VARCHAR(255) NOT NULL,
				dataset_code VARCHAR(255) NOT NULL,	
                last_job_gid VARCHAR(255) NOT NULL,
                last_Tran_count VARCHAR(255) NOT NULL,
				PRIMARY KEY (DatasetName,Count,dataset_type,dataset_code,last_job_gid,last_Tran_count));
		CREATE TEMPORARY TABLE recon_tmp_treconsummary (
				Sno INT,
				Particulars VARCHAR(255) NOT NULL,
                Count INT,
                dataset_table_name VARCHAR(255) NOT NULL,
                dataset_table_field VARCHAR(255) NOT NULL,
                dataset_type VARCHAR(255) NOT NULL,
                dataset_code VARCHAR(255) NOT NULL,
                type VARCHAR(255) NOT NULL,
				PRIMARY KEY (Particulars,dataset_table_name,dataset_table_field,dataset_type,dataset_code));
        CREATE TEMPORARY TABLE recon_tmp_tpipeline (
				Sno INT,
				pipeline_code VARCHAR(32) NOT NULL,
                pipeline_name VARCHAR(128) NOT NULL,
				dataset_code VARCHAR(128) NOT NULL,
                dataset_name VARCHAR(128) NOT NULL,
                PRIMARY KEY (pipeline_code,pipeline_name,dataset_code,dataset_name));
                 
		CREATE TEMPORARY TABLE recon_tmp_tpipelinevaildate (				
				ppl_field_name VARCHAR(32) NOT NULL,
                pplfieldmapping_flag VARCHAR(128) NOT NULL,
				field_name VARCHAR(128) NOT NULL,
                field_mandatory VARCHAR(128) NOT NULL,
                pipeline_code VARCHAR(128) NOT NULL,
                dataset_code VARCHAR(128) NOT NULL,
                dataset_name VARCHAR(255) NOT NULL,
                Particulars VARCHAR(255) NOT NULL,
                pipeline_name VARCHAR(255) NOT NULL,
                PRIMARY KEY (ppl_field_name,pplfieldmapping_flag,pipeline_code,dataset_code,field_name));
                
		CREATE TEMPORARY TABLE recon_tmp_tpipelinecount (
				Sno INT,
				pipeline_code VARCHAR(32) NOT NULL,
                pipeline_name VARCHAR(128) NOT NULL,
                pipelinefield_count VARCHAR(128) NOT NULL,
				dataset_code VARCHAR(128) NOT NULL,
                dataset_name VARCHAR(128) NOT NULL,
                datasetfield_count VARCHAR(128) NOT NULL,
                PRIMARY KEY (pipeline_code,pipeline_name,dataset_code,dataset_name));
                
		CREATE TEMPORARY TABLE recon_tmp_tpipelinefield (				
				pipeline_code VARCHAR(32) NOT NULL,
                pipeline_name VARCHAR(128) NOT NULL,
                field_name VARCHAR(255) NOT NULL,
				dataset_code VARCHAR(128) NOT NULL,
                dataset_name VARCHAR(128) NOT NULL,
                PRIMARY KEY (pipeline_code,pipeline_name,field_name,dataset_code,dataset_name));
                
		INSERT INTO recon_tmp_tdataset (dataset_code, dataset_name, dataset_table_name,dataset_type,last_job_gid,scheduler_gid)
			select a.dataset_code,b.dataset_name,dataset_table_name,
					fn_get_mastername(a.dataset_type, 'QCD_DS_TYPE') as 'dataset_type',
					last_job_gid,scheduler_gid
				from recon_mst_trecondataset a
				inner join recon_mst_tdataset b on a.dataset_code=b.dataset_code
				inner join recon_trn_tscheduler c on b.last_job_gid = c.job_gid and a
				where recon_code = in_recon_code and dataset_type in ('B','S','L') and a.active_status = 'Y' and b.active_status = 'Y'
				and a.delete_flag='N' and b.delete_flag='N' and c.delete_flag='N' and last_job_gid > 0;
      
		INSERT INTO recon_tmp_treconsummary (Sno,Particulars, Count,dataset_table_name,dataset_table_field,dataset_type,dataset_code,type)
			select (@row_number1 := @row_number1 + 1) as Sno,
				   Particulars,Count,'' as dataset_table_name,'' as dataset_table_field,
				   ''as dataset_type,'' as dataset_code,'Recon Valiadtion' as type 
				   from (
						select 'Recon - Tran Date Not Available' as Particulars,count(*) as Count from recon_trn_ttran 
							where recon_code = in_recon_code and tran_date ='' and delete_flag='N' 
					union
						select 'Recon - Future Tran Date' as Particulars,count(*) as Count from recon_trn_ttran 
							where recon_code = in_recon_code and tran_date > now() and delete_flag='N' 
					union
						select 'Recon - Tran Value' as Particulars,count(*) as Count from recon_trn_ttran 
							where recon_code = in_recon_code and tran_value = 0 and delete_flag='N' 
					union
						select 'Supporting - Tran Date Not Available' as Particulars,count(*) as Count from recon_trn_ttranbrkp 
						    where recon_code = in_recon_code and tran_date ='' and delete_flag='N' 
					union
						select 'Supporting - Future Tran Date' as Particulars,count(*) as Count from recon_trn_ttranbrkp 
							where recon_code = in_recon_code and tran_date > now() and delete_flag='N' 
					union
						select 'Supporting - Tran Value' as Particulars,count(*) as Count from recon_trn_ttran 
							where recon_code = in_recon_code and tran_value = 0 and delete_flag='N' ) a;

	-- Loop through recon_tmp_tdataset
		Dataset_block:begin
		DECLARE cur2 CURSOR FOR SELECT dataset_name,dataset_table_name,dataset_type,dataset_code,
										last_job_gid,scheduler_gid FROM recon_tmp_tdataset;
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done2 = TRUE;
    
		OPEN cur2;
			read_loop2: LOOP
			FETCH cur2 INTO current_dataset_name,current_table_name,current_dataset_type,current_dataset_code,
							current_last_job_gid,current_scheduler_gid;
			IF done2 THEN
				LEAVE read_loop2;
			END IF;
  
			set @query1 = CONCAT('SELECT COUNT(*) INTO @v_count  FROM ', current_table_name,';');
			set @query2 = CONCAT('SELECT COUNT(*) INTO @v_count1  FROM ', current_table_name,' where scheduler_gid =' ,current_scheduler_gid,';');
	   
			prepare stmt from @query1;
			execute stmt;
			deallocate prepare stmt;
    
			prepare stmt2 from @query2;
			execute stmt2;
			deallocate prepare stmt2;   
   
		-- Insert the result into the summary table
			INSERT INTO recon_tmp_tdatasetsummary (DatasetName, Count,dataset_type,dataset_code,last_job_gid,last_Tran_count)
			VALUES (current_dataset_name, @v_count,current_dataset_type,current_dataset_code,current_last_job_gid,@v_count1);
      
			INSERT INTO recon_tmp_treconsummary (Sno,Particulars, Count,dataset_table_name,dataset_table_field,dataset_type,dataset_code,type)
			select  (@row_number1 := @row_number1 + 1) as Sno,
					concat(b.dataset_name,' - ',field_name ,' - Value Not Available'),0,dataset_table_name,a.dataset_table_field,'' as 'dataset_type',
                    b.dataset_code,'Dataset Validation' as type
                    from recon_mst_tdatasetfield a
					inner join recon_mst_tdataset b on a.dataset_code=b.dataset_code
					where field_mandatory ="Y" and a.dataset_code = current_dataset_code and a.delete_flag='N' and b.delete_flag='N' and a.active_status='Y' and b.active_status='Y';      
   	                 
            INSERT INTO recon_tmp_tpipeline (Sno,pipeline_code,pipeline_name,dataset_code,dataset_name)
            select  dataset_gid as Sno,pipeline_code,pipeline_name,dataset_code,dataset_name from con_mst_tpipeline a
					inner join recon_mst_tdataset b on a.target_dataset_code = b.dataset_code
					where target_dataset_code = current_dataset_code and pipeline_status='Active' 
                    and a.delete_flag ='N' and b.delete_flag ='N' and b.active_status='Y';
                      
			INSERT INTO recon_tmp_tpipelinecount (Sno,pipeline_code,pipeline_name,pipelinefield_count,dataset_code,dataset_name,datasetfield_count)
			select  dataset_gid as Sno,a.pipeline_code,pipeline_name,pipelinefield_count,b.dataset_code,dataset_name,datasetfield_count from con_mst_tpipeline a
					inner join recon_mst_tdataset b on a.target_dataset_code = b.dataset_code
                    inner join (select dataset_code,count(dataset_code) as datasetfield_count from recon_mst_tdatasetfield 
						where dataset_code = current_dataset_code and active_status='Y' and delete_flag ='N' group by dataset_code) c on b.dataset_code = c.dataset_code
                      inner join (select pipeline_code,count(pipeline_code) as pipelinefield_count from con_trn_tpplfieldmapping 
                       where dataset_code = current_dataset_code and delete_flag ='N' group by pipeline_code) d on a.pipeline_code=d.pipeline_code
					where target_dataset_code = current_dataset_code and pipeline_status='Active' 
                    and a.delete_flag ='N' and b.delete_flag ='N' and b.active_status='Y';  
			END LOOP;
		CLOSE cur2;
		end Dataset_block;   


			INSERT INTO recon_tmp_tpipelinevaildate(ppl_field_name,pplfieldmapping_flag,field_name,field_mandatory,pipeline_code,dataset_code,dataset_name,Particulars,pipeline_name)
            select a.ppl_field_name,a.pplfieldmapping_flag,b.field_name,b.field_mandatory,a.pipeline_code,a.dataset_code,dataset_name,concat(dataset_name,' - ',field_name,' - ','Not Avaliable in Dataset ',' - ',
                    'Avaliable in Pipleline') as Particulars,pipeline_name
				from con_trn_tpplfieldmapping  a  
				inner join recon_mst_tdatasetfield b on a.dataset_code = b.dataset_code and a.ppl_field_name= b.field_name
                inner join recon_mst_tdataset c on a.dataset_code =c.dataset_code
                inner join con_mst_tpipeline d on d.pipeline_code =a.pipeline_code
				where a.pipeline_code in (select pipeline_code from recon_tmp_tpipeline)  and field_mandatory='N' and pplfieldmapping_flag = 1;   
               
			INSERT INTO recon_tmp_tpipelinevaildate(ppl_field_name,pplfieldmapping_flag,field_name,field_mandatory,pipeline_code,dataset_code,dataset_name,Particulars,pipeline_name)
            select a.ppl_field_name,a.pplfieldmapping_flag,b.field_name,b.field_mandatory,a.pipeline_code,a.dataset_code,dataset_name,concat(dataset_name,' - ',ppl_field_name,' - ',' Avaliable in Dataset ',' - '
                    ' Not Avaliable in Pipleline ') as Particulars,pipeline_name
				from con_trn_tpplfieldmapping  a  
				inner join recon_mst_tdatasetfield b on a.dataset_code = b.dataset_code and a.ppl_field_name= b.field_name
                inner join recon_mst_tdataset c on a.dataset_code =c.dataset_code
                inner join con_mst_tpipeline d on d.pipeline_code =a.pipeline_code
				where a.pipeline_code in (select pipeline_code from recon_tmp_tpipeline)  and field_mandatory='Y' and pplfieldmapping_flag = 0; 
               
	-- Loop through recon_tmp_treconsummary
		Recon_block:begin
			DECLARE cur1 CURSOR FOR SELECT Sno,dataset_table_name,dataset_table_field FROM recon_tmp_treconsummary where dataset_table_field !='';
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done1 = TRUE;    
		OPEN cur1;
			read_loop1: LOOP
				FETCH cur1 INTO current_sno,current_dataset_table_name,current_dataset_table_field;
				IF done1 THEN
					LEAVE read_loop1;
				END IF;
        
				set @query3 = CONCAT('SELECT COUNT(*) INTO @v_count2  FROM ', current_dataset_table_name,' where ',current_dataset_table_field,' = "";');
        
				prepare stmt from @query3;
				execute stmt;
				deallocate prepare stmt;
        
				update recon_tmp_treconsummary set Count = @v_count2 where sno = current_sno;
        
			END LOOP;
		CLOSE cur1;
		end Recon_block;
        
        pipeline_block:begin
			DECLARE cur3 CURSOR FOR SELECT pipeline_code,pipeline_name,dataset_code,dataset_name FROM recon_tmp_tpipeline;
			DECLARE CONTINUE HANDLER FOR NOT FOUND SET done3 = TRUE;    
		OPEN cur3;
			read_loop3: LOOP
				FETCH cur3 INTO current_pipeline_code,current_pipeline_name,current_dataset_code1,current_dataset_name1;
				IF done3 THEN
					LEAVE read_loop3;
				END IF;
       
         INSERT INTO recon_tmp_tpipelinefield (pipeline_code,pipeline_name,field_name,dataset_code,dataset_name)
				select distinct current_pipeline_code,current_pipeline_name,field_name,dataset_code,current_dataset_name1 from recon_mst_tdatasetfield  where dataset_table_field not in 
						(select dataset_field_name from con_trn_tpplfieldmapping where dataset_code = current_dataset_code1 and pipeline_code = current_pipeline_code)  
						and dataset_code = current_dataset_code1 and active_status='Y' and delete_flag ='N' ;       
			END LOOP;
		CLOSE cur3;
		end pipeline_block;    
   
	-- Recon Summary
		select 
			(@row_number := @row_number + 1) as SNo,
			Particulars,
			Count,
			DatasetCode as 'Dataset Code',
			ReconDatasetType as 'Recon Dataset Type',
			Type as 'Type',
			pipe_code as 'Pipeline Code',
			pipe_name as 'Pipeline Name',
			LastTransactionDate as 'Last Transaction Date',
			LastTransactionDate as 'Last Transaction By',
			lastTransactioncount as 'Last Transaction Count' 
			from (
					select 		
						Particulars,
						Count,
						'' as  'DatasetCode',
						'' as  'ReconDatasetType',
						'Recon' as  'Type',
						'' as  'pipe_code',
						'' as  'pipe_name',
						'' as  'LastTransactionDate',
						'' as  'LastTransactionBy',
						'' as  'lastTransactioncount'
						from (
						select 'Tran Exception' as Particulars ,count(*) as 'Count' from recon_trn_ttran where recon_code = in_recon_code and delete_flag='N'
						union
						select 'Supporting Tran Exception' as Particulars ,count(*) as 'Count' from recon_trn_ttranbrkp where recon_code = in_recon_code and tran_gid > 0 and delete_flag='N'
						union
						select 'Supporting Tran Not Posted' as Particulars, count(*) as 'Count' from recon_trn_ttranbrkp where recon_code = in_recon_code and tran_gid = 0 and delete_flag='N'
						) s
				union
					select			
						DatasetName as 'Particulars',		
						Count as 'Count',
						dataset_code as 'DatasetCode',
						dataset_type as 'ReconDatasetType',
						'Dataset' as  'Type',
						'' as  'pipe_code',
						'' as  'pipe_name',
						start_date as 'LastTransactionDate',
						job_initiated_by as 'LastTransactionBy',
						last_Tran_count as 'lastTransactioncount'
					from recon_tmp_tdatasetsummary a
					inner join recon_trn_tjob b on a.last_job_gid=b.job_gid  
				union  
					select 		
						Particulars,
						Count,
						f.dataset_code as  'DatasetCode',
						fn_get_mastername(g.dataset_type, 'QCD_DS_TYPE')  as  'ReconDatasetType',
						type as  'Type',
						'' as  'pipe_code',
						'' as  'pipe_name',
						'' as  'LastTransactionDate',
						'' as  'LastTransactionBy',
						'' as  'lastTransactioncount' 
					from recon_tmp_treconsummary f 
					left join recon_mst_trecondataset g on f.dataset_code = g.dataset_code and recon_code = in_recon_code
				union 
					select * from (
						select						
						dataset_name as Particulars,
						0 as Count,
						dataset_code as  'DatasetCode',
						''  as  'ReconDatasetType',
					    'Pipeline' as  'Type',
						pipeline_code as  'pipe_code',
						pipeline_name as  'pipe_name',
						'' as  'LastTransactionDate',
						'' as  'LastTransactionBy',
						'' as  'lastTransactioncount' 
					from recon_tmp_tpipeline order by Sno,dataset_name asc ) g
				union
					select						
						Particulars as Particulars,
						0 as Count,
						dataset_code as  'DatasetCode',
						''  as  'ReconDatasetType',
					    'Pipeline Mandatory Valiadtion' as  'Type',
						pipeline_code as  'pipe_code',
						pipeline_name as  'pipe_name',
						'' as  'LastTransactionDate',
						'' as  'LastTransactionBy',
						'' as  'lastTransactioncount' 
					from recon_tmp_tpipelinevaildate 
				union 
				  select * from (
						select						
						concat(dataset_name ,' - ',datasetfield_count,'  Dataset Fields ') as Particulars,
						datasetfield_count - pipelinefield_count as Count,
						dataset_code as  'DatasetCode',
						''  as  'ReconDatasetType',
					   'Pipeline Field Count Mismatch' as  'Type',
						pipeline_code as  'pipe_code',
						concat(pipeline_name,' - ',pipelinefield_count,'  Pipeline Fields ') as  'pipe_name',
						'' as  'LastTransactionDate',
						'' as  'LastTransactionBy',
						'' as  'lastTransactioncount' 
					from recon_tmp_tpipelinecount order by Sno,dataset_name asc ) g where count > 0
				union
					select						
						concat(dataset_name ,' - ', field_name) as Particulars,
						0 as Count,
						dataset_code as  'DatasetCode',
						''  as  'ReconDatasetType',
					   'Pipeline Missing Fields' as  'Type',
						pipeline_code as  'pipe_code',
						pipeline_name as  'pipe_name',
						'' as  'LastTransactionDate',
						'' as  'LastTransactionBy',
						'' as  'lastTransactioncount' 
					from recon_tmp_tpipelinefield 
		) v;       
		      
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tdataset;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tdatasetsummary;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_treconsummary;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipeline;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipelinevaildate;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipelinecount;
		DROP TEMPORARY TABLE IF EXISTS recon_tmp_tpipelinefield;
	END $$
DELIMITER ;