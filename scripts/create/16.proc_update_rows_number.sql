create or replace procedure update_rows_number(id_record int, number_row int, change_d text default '') as $$
begin
	if change_d <> '' 
	then
		update tbmetrology set rows_number = number_row, change_date = change_d::date where id = id_record;
	else
		update tbmetrology set rows_number = number_row where id = id_record;
	end if;
end;
$$ language plpgsql;
