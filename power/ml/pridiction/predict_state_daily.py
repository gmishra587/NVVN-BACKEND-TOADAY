import pandas as pd
from datetime import timedelta
from power.models import StateDailyLoad
from power.utils.metadata import add_calendar_features
from power.ml.trainy.common import merge_live_weather



def predict_state_daily_data(state_model, state: str, periods: int = 7):
    # --- Last available date in dataset ---
    last_date = (
        StateDailyLoad.objects
        .filter(state=state)
        .latest("date")
        .date
    )

    # --- Future dates for prediction ---
    future_dates = pd.date_range(
        start=last_date + timedelta(days=1),
        periods=periods,
        freq="D"
    )
    df = pd.DataFrame({"ds": future_dates})

    # --- Merge LIVE weather for forecast dates ---
    df = merge_live_weather(df, state)

    # --- Calendar features ---
    df = add_calendar_features(df)

    # --- Predict using Prophet model ---
    forecast = state_model.predict(df)
    df["yhat"] = forecast["yhat"].values

    return df[["ds", "yhat"]]
