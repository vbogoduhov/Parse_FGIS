--�������, ������������ ������������� id_org �� �������
--tborgmetrology �� ��������
create or replace function get_id_org(org_name text) returns int as $$
begin 
	return (select tborgmetrology.id_org from tborgmetrology where tborgmetrology.name_org = org_name);
end;
$$ language plpgsql;