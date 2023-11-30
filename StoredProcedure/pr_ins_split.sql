DELIMITER $$

DROP PROCEDURE IF EXISTS `pr_ins_split` $$
CREATE PROCEDURE `pr_ins_split`(tb_name varchar(255), tb_value nvarchar(6500))
begin
    SET @sql = CONCAT('Insert Into ', tb_name,'(item) Values(?)');
    PREPARE s1 from @sql;
    SET @paramA = tb_value;
    EXECUTE s1 USING @paramA;
end $$

DELIMITER ;