--������� ��� ��������� �������������� ������ �� ������� tbmetrology
--� ������� ������������ ������ hyperlink
create or replace function get_id_for_href(hyperlink text) returns int as $$
begin 
	return (select id from tbmetrology t where href = hyperlink);
end;
$$ language plpgsql;

