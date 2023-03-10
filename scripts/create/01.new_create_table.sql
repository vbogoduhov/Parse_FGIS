create table if not exists tbtitle (id_title serial not null primary key, title text);
create table if not exists tbmodification (id_mod serial not null primary key, modification text);
create table if not exists tbtype (id_type serial not null primary key, type_title text, type_number text);
create table if not exists tborgmetrology (id_org serial not null primary key, name_org text);
create table if not exists tbmetrology 
	(id serial not null primary key,
	mitnumber integer not null references tbtype (id_type),
	modification integer not null references tbmodification (id_mod),
	si_number text,
	valid_date text,
	docnum text,
	mitype integer not null references tbtype (id_type),
	title integer references tbtitle (id_title),
	org_title integer references tborgmetrology (id_org),
	applicability text,
	vri_id text,
	verif_date text,
	href text,
	change_date date,
	change_flag integer,
	rows_number integer);