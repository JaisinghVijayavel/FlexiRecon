DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_create_datasettable` $$
CREATE PROCEDURE `pr_create_datasettable`
(
  in in_dataset_db_name varchar(128),
  in in_dataset_table_name varchar(128),
  out out_msg text,
  out out_result int(10)
)
BEGIN
  declare v_sql text default '';
  declare v_table_name text default '';

  set v_table_name = concat(in_dataset_db_name,'.',in_dataset_table_name);

  if not exists(select * from information_schema.tables
    where TABLE_SCHEMA = in_dataset_db_name
    and TABLE_NAME = in_dataset_table_name) then
    set v_sql = concat('create table ',concat(in_dataset_db_name,'.',in_dataset_table_name),' ');
    set v_sql = concat(v_sql,'select * from recon_tmp_tdatasetstru where 1 = 2');

    call pr_run_sql(v_sql,out_msg,out_result);

    if out_result = 1 then
      -- add primary key
      set v_sql = concat('alter table ',v_table_name, ' ');
      set v_sql = concat(v_sql,'add primary key(dataset_gid)');

      call pr_run_sql(v_sql,@msg,@result);

      -- auto increment
      set v_sql = concat('alter table ',v_table_name, ' ');
      set v_sql = concat(v_sql,'modify dataset_gid integer unsigned AUTO_INCREMENT');

      call pr_run_sql(v_sql,@msg,@result);

      -- add index scheduler_gid
      set v_sql = concat('create index idx_scheduler_gid on ',v_table_name,'(scheduler_gid)');

      call pr_run_sql(v_sql,@msg,@result);

      set out_result = 1;
      set out_msg = 'Table created successfully !';
    end if;
  else
    set out_result = 0;
    set out_msg = 'Table already exists !';
  end if;
END $$

DELIMITER ;