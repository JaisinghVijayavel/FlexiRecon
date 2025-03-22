DELIMITER $$

DROP procedure IF EXISTS `pr_set_PD_minibill` $$
CREATE procedure `pr_set_PD_minibill`
(
  in_recon_code varchar(32)
)
me:begin
  /*
    Created By : Vijayavel
    Created Date :

    Updated By : Vijayavel
    updated Date : 21-03-2025

    Version : 1
  */

  declare v_sql text default '';
  declare v_pdunit_code text default '';
  declare v_mini_field_name text default '';

	declare v_tran_table text default '';
	declare v_tranbrkp_table text default '';

  declare v_concurrent_ko_flag text default '';

  -- concurrent KO flag
  set v_concurrent_ko_flag = fn_get_configvalue('concurrent_ko_flag');

  if v_concurrent_ko_flag = 'Y' then
    set v_tran_table = concat(in_recon_code,'_tran');
    set v_tranbrkp_table = concat(in_recon_code,'_tranbrkp');
  else
    set v_tran_table = 'recon_trn_ttran';
    set v_tranbrkp_table = 'recon_trn_ttranbrkp';
  end if;

  drop temporary table if exists recon_tmp_tunit;

  -- creat unit code temporary table
  create temporary table recon_tmp_tunit(
    unit_code varchar(32) NOT NULL,
    PRIMARY KEY (unit_code)
  ) ENGINE = MyISAM;

  -- get mini field name
  select
    recon_field_name into v_mini_field_name
  from recon_mst_treconfield
  where recon_code = in_recon_code
  and recon_field_desc = 'Mini Bill No'
  and active_status = 'Y'
  and delete_flag = 'N';

  set v_mini_field_name = ifnull(v_mini_field_name,'');

  if v_mini_field_name = '' then
    leave me;
  end if;

  -- blank mini bill no
  set v_sql = concat("update ",v_tranbrkp_table," set
    ",v_mini_field_name,"='0'
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  -- blank mini bill no
  set v_sql = concat("update ",v_tran_table," set
    ",v_mini_field_name,"='0'
    where recon_code = '",in_recon_code,"'
    and delete_flag = 'N'");

  call pr_run_sql(v_sql,@msg,@result);

  set v_sql = concat("insert into recon_tmp_tunit (unit_code)
      select
        distinct split(col2,'-',1)
      from ",v_tranbrkp_table,"
      where recon_code = '",in_recon_code,"'
      and split(col2,'-',2) in ('OCS','OCR','ICS','ICR')
      and delete_flag = 'N'
      union
      select
        distinct split(col2,'-',1)
      from ",v_tran_table,"
      where recon_code = '",in_recon_code,"'
      and split(col2,'-',2) in ('OCS','OCR','ICS','ICR')
      and delete_flag = 'N'
      LOCK IN SHARE MODE;
    ");

  call pr_run_sql2(v_sql,@msg2,@result2);

  -- pdunit
  pdunit_block:begin
    declare pdunit_done int default 0;
    declare pdunit_cursor cursor for
      select unit_code from recon_tmp_tunit;

    declare continue handler for not found set pdunit_done=1;

    open pdunit_cursor;

    pdunit_loop: loop
      fetch pdunit_cursor into v_pdunit_code;

      if pdunit_done = 1 then leave pdunit_loop; end if;

      set v_pdunit_code = ifnull(v_pdunit_code,'');

      call pr_set_PDunit_minibill(in_recon_code,v_pdunit_code);
      call pr_set_PDunit_tranminibill(in_recon_code,v_pdunit_code);
    end loop pdunit_loop;

    close pdunit_cursor;
  end pdunit_block;

  drop temporary table if exists recon_tmp_tunit;
end $$

DELIMITER ;