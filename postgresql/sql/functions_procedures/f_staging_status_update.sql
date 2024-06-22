CREATE OR REPLACE FUNCTION staging.function_orders_status_update()
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    min_date date;
BEGIN
    ---
    select current_date - interval '20 day' into min_date;
    ---  
    if min_date is not null then
        update staging.total_orders so
        set status = mo.status 
        from master.orders mo
        where so.id = mo.id and so.order_date >= min_date;
    end if;
    ---     
    return jsonb_pretty(jsonb_build_object(       
        'start_date', min_date
    ));
END
$function$
;
