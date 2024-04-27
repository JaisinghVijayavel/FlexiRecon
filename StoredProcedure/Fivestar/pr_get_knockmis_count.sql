DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_get_knockmis_count` $$
CREATE PROCEDURE `pr_get_knockmis_count`(
  in in_recon_gid text,
  in in_no_recons int,
  in in_tran_from date,
  in in_tran_to date
)
me:BEGIN
  declare v_recon_gid text default '';
  declare v_all_recon_gid text default '';
  declare v_sql text default '';
  declare err_msg varchar(45) default '';
  declare err_flag bool default false;
  declare v_manual_count bigint  default 0;
  declare v_system_count bigint  default 0;
  declare v_total_count bigint  default 0;
  declare v_undo_system_count bigint  default 0;
  declare v_undo_manual_count bigint  default 0;
  declare v_in_no_recons int  default 0;
  declare v_komis_timestamp timestamp ;
  declare v_curr_timestamp timestamp ;
  declare v_excp_count bigint default 0;
  declare v_system_count1 bigint  default 0;
  declare v_manual_count1 bigint  default 0;
  declare v_automch_totalper text default '';
  declare v_manual_totalper text default '';
  declare v_totalper   text default '';
  declare v_tran_from date;
  declare v_tran_to date;
  declare v_date1 date;
  declare v_entity_name varchar(45);
  declare v_date_count date;
  declare v_recon_count int default 0;
  declare v_all_recon_count int default 0;
  declare recon int default 0;
  declare i int default 0;
  declare v_count int default 0;

  set v_recon_gid=in_recon_gid;
  set v_tran_from = in_tran_from;
  set v_tran_to = in_tran_to;

  set v_in_no_recons=in_no_recons;
  set i=0;
  
  
  select config_value into v_komis_timestamp 
     from admin_mst_tconfig 
	 where config_name='last_komis_timestamp' 
     and delete_flag='N';

  select group_concat(recon_gid),count(*) 
     into v_all_recon_gid,v_all_recon_count
     from recon_mst_trecon 
     where delete_flag='N'
     and active_status='Y';
     
  
 
  select config_value into v_entity_name
     from admin_mst_tconfig 
	 where config_name='entity_name' 
     and delete_flag='N';
  
  select v_entity_name ,date_format(v_tran_from,'%Y-%m-%d') as Date1,date_format(v_tran_to,'%Y-%m-%d') as Date2;
 
all_recon_loop:loop
  
  if v_all_recon_count=0 then  
       leave all_recon_loop;
   end if;
    
  set i=i+1;
  set recon=fn_get_splitstr(v_all_recon_gid,',',i);
  
  if not exists(select recon_gid from recon_trn_tknockoffcount
                where recon_gid=recon) then
		  insert into recon_trn_tknockoffcount values(0,recon,v_system_count,v_manual_count,0,v_excp_count);
   end if;
  
  set v_all_recon_count =v_all_recon_count -1; 
end loop all_recon_loop;

   set i=0;
recon_loop:loop
      
  if v_in_no_recons=0 then  
       leave recon_loop;
  end if;
    
  set i=i+1;
  set recon=fn_get_splitstr(v_recon_gid,',',i);

  -- system count
  select
    count(distinct c.tran_gid) into v_count
  from recon_trn_tko a
  inner join recon_trn_tkodtl b on a.ko_gid=b.ko_gid
    and b.delete_flag = 'N'
  inner join recon_trn_ttran as c on b.tran_gid = c.tran_gid
    and c.delete_flag = 'N'
  where a.recon_gid=recon
  and a.rule_gid>0
  and a.manual_matchoff<>'Y'
  and c.tran_date >= date_format(v_tran_from,'%Y-%m-%d')
  and c.tran_date <= date_format(v_tran_to,'%Y-%m-%d')
  and a.delete_flag='N';

  set v_system_count = ifnull(v_count,0);

  select
    count(distinct c.tran_gid) into v_count
  from recon_trn_tko a
  inner join recon_trn_tkodtl b on a.ko_gid=b.ko_gid
    and b.delete_flag = 'N'
  inner join recon_trn_ttranko as c on b.tran_gid = c.tran_gid
    and c.delete_flag = 'N'
  where a.recon_gid=recon
  and a.rule_gid>0
  and a.manual_matchoff<>'Y'
  and c.tran_date >= date_format(v_tran_from,'%Y-%m-%d')
  and c.tran_date <= date_format(v_tran_to,'%Y-%m-%d')
  and a.delete_flag='N';

  set v_system_count = v_system_count + ifnull(v_count,0);

  -- manual count
  select
    count(distinct c.tran_gid) into v_count
  from recon_trn_tko a
  inner join recon_trn_tkodtl b on a.ko_gid=b.ko_gid
    and b.delete_flag = 'N'
  inner join recon_trn_ttran as c on b.tran_gid = c.tran_gid
    and c.delete_flag = 'N'
  where a.recon_gid=recon
  and a.rule_gid=0
  and a.manual_matchoff='Y'
  and c.tran_date >= date_format(v_tran_from,'%Y-%m-%d')
  and c.tran_date <= date_format(v_tran_to,'%Y-%m-%d')
  and a.delete_flag='N';

  set v_manual_count = ifnull(v_count,0);

  select
    count(distinct c.tran_gid) into v_count
  from recon_trn_tko a
  inner join recon_trn_tkodtl b on a.ko_gid=b.ko_gid
    and b.delete_flag = 'N'
  inner join recon_trn_ttranko as c on b.tran_gid = c.tran_gid
    and c.delete_flag = 'N'
  where a.recon_gid=recon
  and a.rule_gid=0
  and a.manual_matchoff='Y'
  and c.tran_date >= date_format(v_tran_from,'%Y-%m-%d')
  and c.tran_date <= date_format(v_tran_to,'%Y-%m-%d')
  and a.delete_flag='N';

  set v_manual_count = v_manual_count + ifnull(v_count,0);


  -- excp count
  select count(*) into v_excp_count
         from recon_trn_ttran
         where recon_gid=recon
         and tran_date >= date_format(v_tran_from,'%Y-%m-%d')
         and tran_date <= date_format(v_tran_to,'%Y-%m-%d')
         and delete_flag='N'
         and excp_amount>0;

  -- total count
  select count(*) into v_count
         from recon_trn_ttran
         where recon_gid=recon
         and tran_date >= date_format(v_tran_from,'%Y-%m-%d')
         and tran_date <= date_format(v_tran_to,'%Y-%m-%d')
         and delete_flag='N';

  set v_total_count= ifnull(v_count,0);

  select count(*) into v_count
         from recon_trn_ttranko
         where recon_gid=recon
         and tran_date >= date_format(v_tran_from,'%Y-%m-%d')
         and tran_date <= date_format(v_tran_to,'%Y-%m-%d')
         and delete_flag='N';

  set v_total_count= v_total_count + ifnull(v_count,0);

  update recon_trn_tknockoffcount
        set system_count=v_system_count,
        manual_count=v_manual_count,
        total_count=v_total_count,
        excp_count= v_excp_count
        where recon_gid=recon;

  set v_in_no_recons =v_in_no_recons -1;
 end loop recon_loop;

 

  set v_sql=concat('select 
            b.recon_name as `Bank Name`,
            ',char(39),concat('From ',date_format(v_tran_from,'%Y-%m-%d'),' To ',date_format(v_tran_to,'%Y-%m-%d')),char(39),' as `Upto Live Data`,
			      total_count as `Total Transaction`,
            system_count as `Auto match Total Count`,
            concat(round(( system_count/total_count * 100 ),2),''%'') as `Auto(%)`,
			      manual_count as `Manual match Total count`,
			      concat(round(( manual_count/total_count * 100 ),2),''%'') as `Manual(%)`,
            concat(round(( system_count/total_count * 100 ),2) + round(( manual_count/total_count * 100 ),2),''%'') as `Total KO (%)`,
            excp_count as `Exception Total Count`,
            ''Live'' as `Status`
			      from recon_trn_tknockoffcount a
            inner join recon_mst_trecon b on a.recon_gid=b.recon_gid
            where a.recon_gid in  (',v_recon_gid,') ');
  
  
      
  set @stm_str=v_sql;
  prepare _sql1 from @stm_str; 
  execute _sql1; 
  deallocate prepare _sql1;

      
END $$

DELIMITER ;