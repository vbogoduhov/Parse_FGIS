--�������, ������������ ������������� id_mod �� �������
--tbmodofication �� ��������
create or replace function get_id_mod(modif text) returns int as $$
begin 
	return (select tbmodification.id_mod from tbmodification where tbmodification.modification = modif);
end;
$$ language plpgsql;
