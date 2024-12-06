DELIMITER $$

DROP procedure IF EXISTS `pr_set_PD_minibill` $$
CREATE procedure `pr_set_PD_minibill`
(
  in_recon_code varchar(32)
)
me:begin
  declare v_sql text default '';
  declare v_pdunit_code text default '';

  -- pdunit
  pdunit_block:begin
    declare pdunit_done int default 0;
    declare pdunit_cursor cursor for
      select
        distinct split(col2,'-',1)
      from recon_trn_ttranbrkp
      where recon_code = in_recon_code
      and split(col2,'-',2) in ('OCS','OCR','ICS','ICR')
      and delete_flag = 'N';
    declare continue handler for not found set pdunit_done=1;

    open pdunit_cursor;

    pdunit_loop: loop
      fetch pdunit_cursor into v_pdunit_code;

      if pdunit_done = 1 then leave pdunit_loop; end if;

      set v_pdunit_code = ifnull(v_pdunit_code,'');

      call pr_set_PDunit_minibill(in_recon_code,v_pdunit_code);
    end loop pdunit_loop;

    close pdunit_cursor;
  end pdunit_block;
end $$

DELIMITER ;