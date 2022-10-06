/*
Build plot of new / retained and gone users for the last 2 weeks daily.
*/
with gone_users as (
    select 
        user_id,
        -- transform time to date -> to Mondays -> form array for each users with mondays
        groupUniqArray(toDate(time)) as daily_visited,
        -- arrayJoin unzip array, so for each monday we find next monday as current week
        addDays(arrayJoin(daily_visited), +1) as current_day,
        addDays(current_day, -1) as previous_day,
        -- if current week is in Monday's array, it means the user returned to the application, otherwise he's gone
        if(has(daily_visited, current_day) = 1, 'retained', 'gone') as status
    from simulator_20220920.feed_actions
    where toDate(time) between yesterday() - 14 and yesterday()
    group by user_id
    having status = 'gone'
),
retained_and_new_users as (
    select 
        user_id,
        groupUniqArray(toDate(time)) as daily_visited,
        -- unzip each monday in array as current week
        arrayJoin(daily_visited) as current_day,
        addDays(current_day, -1) as previous_day,
        -- if previous week is in monday's array, it means user returned, otherwise he's new in app
        if(has(daily_visited, previous_day) = 1, 'retained', 'new') as status
    from simulator_20220920.feed_actions
    where toDate(time) between yesterday() - 14 and yesterday()
    group by user_id
)

SELECT 
    current_day as cdate,
    status,
    -uniq(user_id) as num_users
FROM 
    gone_users gu
WHERE cdate != today()
GROUP BY current_day, status
HAVING
    current_day != addDays(today(), +1) -- get rid of last week data

UNION ALL

SELECT 
    current_day as cdate,
    status,
    toInt64(uniq(user_id)) as num_users
FROM 
    retained_and_new_users rnu
WHERE cdate != today()
GROUP BY current_day, status
ORDER BY current_day, status