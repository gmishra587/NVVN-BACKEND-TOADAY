import pandas as pd
from power.ml.models.prophet_model import train_prophet_model
from power.ml.progress import log_progress
from power.ml.trainy.common import clean_outliers, merge_live_weather
from power.models import StateDailyLoad
from power.utils.logger import get_logger
from power.utils.metadata import add_calendar_features




def train_state_daily_model(state: str):
    logger = get_logger(f"TRAIN-DAILY-{state}")

    log_progress(logger, "Fetching raw daily data", "prophet", 5)

    df = pd.DataFrame(
        StateDailyLoad.objects
        .filter(state=state)
        .values("date", "energy_mu")
        .order_by("date")
    )

    if df.empty:
        raise ValueError(f"No daily data for state {state}")

    log_progress(logger, "Preparing base dataframe", "prophet", 15)

    df.rename(columns={"date": "ds", "energy_mu": "y"}, inplace=True)
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_localize(None)

    log_progress(logger, "Cleaning outliers", "prophet", 25)

    df = clean_outliers(df)

    log_progress(logger, "Fetching LIVE weather", "prophet", 40)

    df = merge_live_weather(df, state)

    log_progress(logger, "Adding calendar features", "prophet", 60)

    df = add_calendar_features(df)

    log_progress(logger, "Starting model training", "prophet", 75)

    model = train_prophet_model(df)

    model.model_type = "prophet"
    model.state = state
    model.frequency = "daily"

    log_progress(logger, "Training completed", "prophet", 100)

    return model





























