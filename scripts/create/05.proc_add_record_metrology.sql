create or replace procedure add_record_metrology(mit_number int, 
												modif int, 
												serial text, 
												valid_d text, 
												doc text, 
												mi_type int, 
												t_title int, 
												organization int, 
												applic text, 
												vri text, 
												verif_d text, 
												hyperlink text, 
												ch_d date, 
												ch_f int,
												r_n int) as $$
begin 
	insert into tbmetrology (mitnumber,
							modification,
							si_number,
							valid_date,
							docnum,
							mitype,
							title,
							org_title,
							applicability,
							vri_id,
							verif_date,
							href,
							change_date,
							change_flag,
							rows_number) 
			values (mit_number, 
					modif, 
					serial, 
					valid_d, 
					doc, 
					mi_type, 
					t_title, 
					organization, 
					applic, 
					vri, 
					verif_d, 
					hyperlink, 
					ch_d, 
					ch_f,
					r_n);
end;
$$ language plpgsql;