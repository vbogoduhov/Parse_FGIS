create or replace  function get_card_array(serial text) returns json as $$
 declare card card_si;
 arr card_si array;
 ind integer;
begin
	ind = 0;
	for card in (select tm.id, tt1.type_number, tmod.modification, tm.si_number, tm.valid_date, tm.docnum, tt1.type_title, tt.title, org.name_org, tm.applicability, tm.vri_id, tm.verif_date, tm.href, tm.change_date, tm.change_flag, tm.rows_number
			from tbmetrology tm, tbtitle tt, tbtype tt1, tbmodification tmod, tborgmetrology org
			where tm.mitnumber = tt1.id_type and tm.modification = tmod.id_mod and tm.mitype = tt1.id_type and tm.title = tt.id_title and tm.org_title = org.id_org and 
			tm.si_number = serial) loop
				ind = ind + 1;
				arr[ind] = card;
			end loop;
		return to_json(arr);
end;
$$
language plpgsql;
