--Процедура добавления данных в tborgmetrology
create or replace procedure add_org_metrology(organization text) as $$
begin
	insert into tborgmetrology (name_org) values (organization);
end;
$$ language plpgsql;
