CREATE OR REPLACE FUNCTION staging.get_last_source(source_path character varying)
 RETURNS character varying
 LANGUAGE plpgsql
AS $function$
declare
	last_source varchar;
begin
	select coalesce(source_path, 'direct') into source_path;

	with input_source_tbl as (
		select
			unnest(string_to_array(source_path, '/')) as src
		), source_tbl as (	
			select
				row_number() OVER() as nrow,
				src
			from input_source_tbl
		), paid_source as (
			select 	
				1 as rep_type, *
			from source_tbl 
			where nrow = (select max(nrow) 
							from source_tbl 
							where src in ('vk-cpc', 'yandex-cpc'))
		), organic_source as (
			select 	
				2 as rep_type, *
			from source_tbl 
			where nrow = (select max(nrow) 
							from source_tbl 
							where src in ('yandex organic', 'google organic'))
		), other_source as (
			select 	
				3 as rep_type, *
			from source_tbl 
			where nrow = (select max(nrow) 
							from source_tbl 
							where src not in ('vk-cpc', 'yandex-cpc', 'yandex organic', 'google organic'))
		), source_result as (	
			select * from paid_source
			union all	
			select * from organic_source
			union all	
			select * from other_source
			order by rep_type
		)	
	select src into last_source from source_result limit 1;	
	return last_source;

exception when others 
then
	if last_source is null then select 'direct' into last_source; 
	end if;

return last_source;	
END
$function$
;