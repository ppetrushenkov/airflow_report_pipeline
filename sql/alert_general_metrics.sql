/*
The alert system will be check the main metrics like DAU, View, Likes etc for anomalies every 15th minute
*/
with feed_metrics as (
    select 
        toStartOfFifteenMinutes(time) as 15time,
        count(distinct user_id) as feed_dau,
        countIf(user_id, action='view') as views,
        countIf(user_id, action='like') as likes
    from
        simulator_20220820.feed_actions
    where 
        toDate(time) >= today() - 7
    group by
        15time
),
messages_metrics as (
    select 
        toStartOfFifteenMinutes(time) as 15time,
        count(distinct user_id) as messages_dau,
        count(reciever_id) as messages_sent
    from
        simulator_20220820.message_actions
    where 
        toDate(time) >= today() - 7
    group by
        15time
)

select
    15time,
    feed_dau,
    messages_dau,
    views,
    likes,
    likes / views as ctr,
    messages_sent
from
    feed_metrics
join 
    messages_metrics
using 
    15time
order by 
    15time