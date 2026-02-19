import pandas as pd
from datetime import timedelta
from power.models import RegionHourlyLoad
from power.utils.metadata import add_calendar_features
from power.ml.trainy.common import merge_live_weather, REGION_TO_STATES




def predict_region_hourly_data(region_model, region_code: str, periods: int = 24):
    # --- Last available datetime in dataset ---
    last_dt = (
        RegionHourlyLoad.objects
        .filter(region=region_code)
        .latest("datetime")
        .datetime
    )
    last_dt = pd.to_datetime(last_dt).tz_localize(None)

    # --- Future hourly timestamps ---
    future = pd.date_range(
        start=last_dt + timedelta(hours=1),
        periods=periods,
        freq="H"
    )
    df = pd.DataFrame({"ds": future})

    # --- Merge LIVE weather for all states in region ---
    states = REGION_TO_STATES[region_code]
    df = merge_live_weather(df, states)

    # --- Calendar features ---
    df = add_calendar_features(df)

    # --- Predict using Prophet model ---
    forecast = region_model.predict(df)
    df["yhat"] = forecast["yhat"].values

    return df[["ds", "yhat"]]
