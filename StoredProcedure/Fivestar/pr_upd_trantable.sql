DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_upd_trantable` $$
CREATE PROCEDURE `pr_upd_trantable`(
  in in_file_gid int,
  in in_file_type char(1),
  out out_msg text,
  out out_result int
 )
me:BEGIN

  declare v_file_gid int default 0;
  declare v_file_type char default '';
  declare v_field_name text default '';
  declare v_tran_gid text default ''; 
  declare v_field_value text default '';
  declare v_tranbrkup_gid int default 0;
  declare v_sql text default '';
  declare v_sql1 text default '';
  
  set v_file_gid=in_file_gid;
  set v_file_type=in_file_type;
  
  if v_file_type='Q' then
    
    update_block:begin
       declare update_done int default 0;
	   declare update_cursor cursor for
         select  field_name,
                field_value,
                tranbrkup_gid
         from recon_trn_ttranbrkup_updvalue
         where file_gid =v_file_gid
         and delete_flag='N';
      
	   declare continue handler for not found set update_done=1;

       open update_cursor;
      
         update_loop:loop
            fetch update_cursor into v_field_name,v_field_value,v_tranbrkup_gid;
            if update_done = 1 then leave update_loop; end if;
           
            if lower(v_field_value)='blank'  then
              set v_field_value='';
			end if;
            
			if lower(v_field_value) = 'null'  then
               set v_sql='';
               SET v_sql = CONCAT('UPDATE recon_trn_ttranbrkp SET ', v_field_name, ' = null 
                               WHERE tranbrkp_gid = ', v_tranbrkup_gid, ' 
                              AND delete_flag = "N"');
            else
			   set v_sql='';
               SET v_sql = CONCAT('UPDATE recon_trn_ttranbrkp SET ', v_field_name, ' = "', v_field_value, '" 
                               WHERE tranbrkp_gid = ', v_tranbrkup_gid, ' 
                              AND delete_flag = "N"');
             end if;

         
			   set v_sql1= concat('update recon_trn_ttranbrkup_updvalue set update_flag=''Y''
                              WHERE tranbrkup_gid = ', v_tranbrkup_gid, ' 
                              AND delete_flag = "N"');
        
		       set @v_sql = v_sql; 
		       prepare _sql from @v_sql;
		       execute _sql; 
		       deallocate prepare _sql;
        
               set @v_sql1 = v_sql1; 
		       prepare _sql from @v_sql1;
		       execute _sql; 
		       deallocate prepare _sql;
         
		 end loop update_loop;

	   close update_cursor;
    end update_block;
       
       set out_msg='Records Updated Successfully';
       set out_result=1;
  else
     
	update_block:begin
       declare update_done int default 0;
	   declare update_cursor cursor for
         select  field_name,
                field_value,
                tran_gid
         from recon_trn_ttran_updvalue
         where file_gid =v_file_gid
         and delete_flag='N';
      
	   declare continue handler for not found set update_done=1;

       open update_cursor;
      
         update_loop:loop
           fetch update_cursor into v_field_name,v_field_value,v_tran_gid;
           if update_done = 1 then leave update_loop; end if;
           
		   if lower(v_field_value)='blank'  then
              set v_field_value='';
		   end if;
            
		   if lower(v_field_value)='null'  then
               set v_sql='';
               SET v_sql = CONCAT('UPDATE recon_trn_ttran SET ', v_field_name, ' = null 
                               WHERE tran_gid = ', v_tran_gid, ' 
                              AND delete_flag = "N"');
		   else
               set v_sql='';
               SET v_sql = CONCAT('UPDATE recon_trn_ttran SET ', v_field_name, ' = "', v_field_value, '" 
                            WHERE tran_gid = ', v_tran_gid, ' 
							AND delete_flag = "N"');
           end if;
           
           
			   set v_sql1=concat('update recon_trn_ttran_updvalue set update_flag=''Y''
                           WHERE tran_gid = ', v_tran_gid, ' 
                           AND delete_flag = "N"');
        
		       set @v_sql = v_sql; 
		       prepare _sql from @v_sql;
		       execute _sql; 
		       deallocate prepare _sql;
         
		      set @v_sql1 = v_sql1; 
		      prepare _sql from @v_sql1;
		      execute _sql; 
		      deallocate prepare _sql;
         
		 end loop update_loop;

	   close update_cursor;
    end update_block;
      
  end if;
     set out_msg='Records Updated Successfully';
	 set out_result=1;
  
END $$

DELIMITER ;