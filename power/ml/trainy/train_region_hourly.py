import pandas as pd
from power.ml.models.prophet_model import train_prophet_model
from power.ml.progress import log_progress
from power.ml.trainy.common import REGION_TO_STATES, clean_outliers, merge_live_weather
from power.models import RegionHourlyLoad
from power.utils.logger import get_logger
from power.utils.metadata import add_calendar_features




def train_region_hourly_model(region: str):
    logger = get_logger(f"TRAIN-REGION-{region}")

    log_progress(logger, "Fetching raw hourly region data", "prophet", 5)

    df = pd.DataFrame(
        RegionHourlyLoad.objects
        .filter(region=region)
        .values("datetime", "load_mw")
        .order_by("datetime")
    )

    if df.empty:
        raise ValueError(f"No hourly data for region {region}")

    log_progress(logger, "Preparing base dataframe", "prophet", 15)

    df.rename(columns={"datetime": "ds", "load_mw": "y"}, inplace=True)
    df["ds"] = pd.to_datetime(df["ds"]).dt.tz_localize(None)

    log_progress(logger, "Cleaning outliers", "prophet", 25)

    df = clean_outliers(df)

    log_progress(logger, "Fetching LIVE weather", "prophet", 40)

    df = merge_live_weather(df, REGION_TO_STATES[region])

    log_progress(logger, "Adding calendar features", "prophet", 60)

    df = add_calendar_features(df)

    log_progress(logger, "Starting model training", "prophet", 75)

    model = train_prophet_model(df)

    model.model_type = "prophet"
    model.region = region
    model.frequency = "hourly"

    log_progress(logger, "Training completed", "prophet", 100)

    return model
