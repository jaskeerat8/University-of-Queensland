select c.class_short_description as class,
    p.department_long_descript as dept,
    t.calendar_quarter_long_de as qtr,
    round(s.sales) as sales
from channel_sales_channel_view c,
    product_standard_view p,
    geography_regional_view g,
    time_view t,
    sales_cube_view s
where s.product = p.dim_key
    and p.level_name = 'DEPARTMENT'
    and s.channel = c.dim_key
    and c.level_name = 'CLASS'
    and s.geography = g.dim_key
    and g.level_name = 'ALL_REGIONS'
    and s.time = t.dim_key
    and t.level_name = 'CALENDAR_QUARTER'
    and t.calendar_quarter_short_d like '%CY2009%'
order by c.class_short_description, 
    p.department_long_descript,
    t.calendar_quarter_long_de;
