import sys

sys.path.insert(0, "/var/lib/airflow/airflow/airflow.git/dags/airflow-alerts/dags/p-petrushenkov-10")

from airflow.decorators import dag, task
from dataclasses import dataclass
from typing import Dict, List
from config import *
from alerts import *

import matplotlib.pyplot as plt
import pandas as pd
import telegram

plt.style.use('fivethirtyeight')
plt.rcParams['figure.figsize'] = (8, 6)


@dataclass
class AlertInfo:
    feature_name: str
    value: float
    difference: float
    indicators: List[str]
    timeseries: pd.Series


# ============== INIT BOT ==============
bot = telegram.Bot(token=TOKEN_NAME)


@dag(default_args=DEFAULT_DAG_ARGS,
     schedule_interval="*/15 * * * *",  # At every 15th minute
     catchup=False)
def statixx_alert_dag():
    @task()
    def extract_metrics():
        """
        Info:
        -----
        Function extract following metrics:
        * Feed DAU
        * Message DAU
        * Views
        * Likes
        * CTR
        * Messages sent

        Steps:
        ------
        1. Extract data (contains metrics for last 7 days)
        2. Initialize all methods we're going to use (like Bands, Isolation Forest and so on) in 'methods' dict
        3. For each column metric use all our methods to find out, if there is an outlier or not
        4. If metric have outlier -> append its name to 'features' dict with AlertInfo
        5. For each metric in 'features' send message with bot to specified user/group
        """
        df = get_data("alert_general_metrics.sql")
        df.set_index("15time", inplace=True)
        df.sort_index(inplace=True)
        for feature in df.columns:
            df[feature] = df[feature].apply(np.float32)
        return df.iloc[:-1, :]  # all but last row

    @task()
    def send_alerts(metrics: pd.DataFrame, chat_id: str = MY_CHAT_ID):
        """Check metrics and send alerts if it needs to"""
        alerts = check_metrics(metrics)
        print("[INFO] Got ALERT data")
        if alerts:
            for alert_name, alert_info in alerts.items():
                print("[INFO] Send info about ", alert_name)
                send_alert(alert_name, alert_info, chat_id=chat_id)
            print("[INFO] Messages have been sent")
        else:
            print("[INFO] There is no alerts now, nothing to send")

    def send_alert(feature_name: str, alert_info: AlertInfo, chat_id: str):
        """Send alert to the chat:"""
        send_info(feature_name, alert_info, chat_id)
        send_plot(alert_info, chat_id)
        return

    def check_metrics(metrics: pd.DataFrame) -> Dict[str, AlertInfo]:
        """
        Check each metric for anomaly. 
        Return dict with the feature name and Alert Info dataclass, 
        that contains the method names, that detected anomaly. 
        """
        features = {}
        methods = {
            "Previous days comparison": previous_days_alert,
            "Interquartile Range": iqr_alert,
            "Z score": zscore_alert,
            "Bollinger Bands": bollinger_bands_alert,
        }

        for feature in metrics.columns:
            metric = metrics[feature]
            scaled_metric = zscale(metric)

            indicators = []
            for method_name, method_func in methods.items():
                if method_func(scaled_metric):
                    indicators.append(method_name)

            if len(indicators) > 0:
                last_value = metric[-1]
                prev_value = metric[-2]
                pct_change = get_pct_change(last_value, prev_value)
                features[feature] = AlertInfo(feature_name=feature,
                                              value=last_value,
                                              difference=pct_change,
                                              indicators=indicators,
                                              timeseries=metric)
        return features

    def send_info(feature_name, alert_info: AlertInfo, chat_id: str):
        nl = '\n'
        p = "•"
        info = f"""
@ppetrushenkov
[ALERT] The metric {feature_name} may have an anomaly.
Anomaly was detected by the following methods: {str(nl + p)}{str(nl + p).join(alert_info.indicators)}.
Current value: {alert_info.value:.4f}
Previous value: {alert_info.timeseries[-2]:.4f}
Deviation: {alert_info.difference:.2f}%
Dashbord: {DASHBOARD_URL}
                """

        bot.sendMessage(text=info, chat_id=chat_id)
        return

    def send_plot(alert_info: AlertInfo, chat_id: str):
        """Form subplot of metrics and send it to specified chat_id"""
        last4days = alert_info.timeseries[-96 * 2:]
        ma, ub, lb = get_bollinger_bands(last4days, 21, 5)

        plt.plot(last4days)
        plt.plot(ma, color='red', linestyle="-")
        plt.plot(ub, color='green', linestyle=":")
        plt.plot(lb, color='green', linestyle=':')
        plt.title(alert_info.feature_name)
        plt.xticks(rotation=45)
        plt.tight_layout()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'anomaly_metric.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        return

    metrics = extract_metrics()
    print("[INFO] Data was loaded")
    send_alerts(metrics, chat_id=MY_CHAT_ID)
    return


dag = statixx_alert_dag()
