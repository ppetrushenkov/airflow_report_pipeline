# Report pipeline using Airflow and telegram bot

---------------
This repository contains the pipelines, that collect statistics with a given 
schedule interval using Airflow. Report is sent to specified telegram chat.

The repository has the following pipelines:
1. Report about newsfeed data
2. Report about newsfeed and messages data
3. Alert, if some metric has an outlier

## 1. Newsfeed data report:
Newsfeed data code is in the `feed_info_dag.py` file.
This report shows the Daily Active Users (DAU), Views, Likes and CTR for yesterday,
for day before yesterday and the average value for the last week.

Message: [StatixX] A short summary of feed metrics for 03-10-2022

<img height="256" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/feed_info_pics/feed_info_1.jpg?raw=true" title="short_metric_summary" width="768"/>

<img height="576" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/feed_info_pics/feed_info_2.jpg?raw=true" title="short_metric_summary" width="512"/>

## 2. Newsfeed and messages data report:
Code is in the `app_info_dag.py`.
This report shows the following metrics:
* User turnover (How many new / retained / gone users we have daily)
* Retention and stickiness metrics
* User actions (how many viewed / liked posts and messages sent)

Message: [StatixX] Short metric summary for the whole app for 05-10-2022

<img height="256" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/app_info_pics/tg_2_1.jpg?raw=true" title="short_metric_summary" width="768"/>

<img height="384" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/app_info_pics/tg_2_2.jpg?raw=true" title="stacked_turnover" width="768"/>

<img height="256" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/app_info_pics/tg_2_3.jpg?raw=true" title="retention_table" width="768"/>

<img height="512" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/app_info_pics/tg_2_4.jpg?raw=true" title="retention_plot" width="768"/>

<img height="256" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/app_info_pics/tg_2_5.jpg?raw=true" title="action_table" width="768"/>

<img height="384" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/app_info_pics/tg_2_6.jpg?raw=true" title="stacked_actions" width="768"/>

## 3. Alert, if some metric has an outlier:
The code is in the `alert_dag.py` file.
This report detects anomalies in data. It checks the following metrics each 15th minute:


Message: 

@ppetrushenkov

[ALERT] The metric feed_dau may have an anomaly.

Anomaly was detected by the following methods:
* Interquartile Range
* Z score
* Bollinger Bands.

Current value: 1921.0000

Previous value: 1581.0000

Deviation: 19.42%

Dashboard: http://superset.lab.karpov.courses/r/2030

<img height="384" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/outlier_alerts/feed_dau.jpg?raw=true" title="short_metric_summary" width="512"/>

(almost the same message for another metrics)

<img height="384" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/outlier_alerts/views.jpg?raw=true" title="short_metric_summary" width="512"/>

<img height="384" src="https://github.com/ppetrushenkov/report_pipeline/blob/main/pics/outlier_alerts/likes.jpg?raw=true" title="short_metric_summary" width="512"/>
