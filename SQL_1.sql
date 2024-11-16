select q4.time_index, q4.region_name, q4.temp, q4.max_temp, q4.min_temp, q4.gas_cons
from(
	select 
  		q1.time_index,
  		q1.region_name,
  		coalesce(q3.fact_temp, avg(q3.fact_temp) over (order by q1.region_name, q1.time_index rows between 5 preceding and 5 following)) as temp,
  		q1.max_temp,
  		q1.min_temp,
  		coalesce(q2.gas_cons, avg(q2.gas_cons) over (order by q1.region_name, q1.time_index rows between 1 preceding and 1 following)) as gas_cons,
  		row_number () over (partition by q1.time_index, q1.region_name order by q1.time_index) as row_num
	from (
		select 
			date_actual as time_index,
			initcap(region_name) as region_name,
			min(day_avg_temp) as min_temp,
			max(day_avg_temp) as max_temp
    	from weather_external_data
    	where 
      		(initcap(region_name) = 'Австрия' or initcap(region_name) = 'Франция') and (is_fact = false)
    	group by date_actual, initcap(region_name)
    	) as q1
	left join (
		select 
			gas_day,
			gas_cons_mcm as gas_cons,
			case 
      			when region_code = 'AT' then 'Австрия'
      			when region_code = 'FR' then 'Франция'
      			else region_code
    		end as region_code
  		from vm_eu_gas_consumption_raw
		) as q2
	on (q1.time_index = q2.gas_day) and (q1.region_name = q2.region_code)
	left join ( 
		select 
			date_actual,
			initcap(region_name) as region_name,
			day_avg_temp as fact_temp
		from weather_external_data
		where (initcap(region_name) = 'Австрия' or initcap(region_name) = 'Франция') and (is_fact = true)
		) as q3
	on (q1.time_index = q3.date_actual) and (q1.region_name = q3.region_name)
	order by q1.time_index desc) q4
where q4.row_num = 1


