CREATE OR REPLACE FUNCTION master.function_orders_status_update()
 RETURNS text
 LANGUAGE plpgsql
AS $function$
DECLARE
    min_date date;
    max_date date;
    current_timest timestamp;
BEGIN
    ---
    select min(order_date) from master.orders where status = 'pending' into min_date;
    select current_date - interval '1 day' into max_date;
    select current_timestamp into current_timest;
    ---  
    if min_date is not null then
        update master.orders
        set status = case when random() < 0.7 then 'accepted' else 'rejected' end,
            updated_at = current_timest 
        where status = 'pending' and order_date >= min_date and order_date < max_date;
    end if;
    ---     
    return jsonb_pretty(jsonb_build_object(       
        'start_date', min_date,
        'end_date',  max_date
    ));
END
$function$
;
