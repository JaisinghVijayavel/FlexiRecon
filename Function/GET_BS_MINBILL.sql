DELIMITER $$

DROP FUNCTION IF EXISTS `GET_CHK_MINBILL` $$
CREATE FUNCTION `GET_CHK_MINBILL`
(
  in_bill_no varchar(128)
) RETURNS int
begin
  declare v_sql text default '';
  declare v_unit_code text default '';
  declare v_bill_type text default '';
  declare v_bill_no text default '';

  set v_unit_code = SPLIT(in_bill_no,'-',1);
  set v_bill_type = SPLIT(in_bill_no,'-',2);
  set v_bill_no = cast(SPLIT(in_bill_no,'-',3) as unsigned);

  select
    min(cast(split(col2,'-',3) as unsigned)) into @min_bill
  from recon_flexi_dataset_poc.DS151
  where split(col2,'-',1) = v_unit_code
  and split(col2,'-',2) = v_bill_type
  and delete_flag = 'N';

  set @min_bill = ifnull(@min_bill,0);

  if v_bill_no < @min_bill then
    return 1;
  else
    return 0;
  end if;
end $$

DELIMITER ;