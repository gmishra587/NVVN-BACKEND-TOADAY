from django.tasks import task
from power.ml.model_store import save_model
from power.ml.trainy.train_region_hourly import train_region_hourly_model
from power.ml.trainy.train_state_5min import train_state_5min_model
from power.ml.trainy.train_state_daily import train_state_daily_model
from power.utils.logger import get_logger



logger = get_logger("ML-TASKS")




# ====================================================================
# Models Traning Tasks
# ====================================================================

@task
def train_region_task(region_code: str):
    logger.info("🚀 Training REGION model: %s", region_code)

    try:
        model = train_region_hourly_model(region_code)
        save_model(f"region_{region_code}.pkl", model)

        logger.info("✅ Saved REGION model: %s", region_code)
        return region_code

    except Exception as e:
        logger.exception("❌ REGION training failed: %s", region_code)
        return {
            "region": region_code,
            "status": "failed",
            "error": str(e),
        }




@task
def train_state_daily_task(state: str):
    logger.info("🚀 Training STATE DAILY model: %s", state)

    try:
        model = train_state_daily_model(state)
        save_model(f"state_daily_{state}.pkl", model)

        logger.info("✅ Saved STATE DAILY model: %s", state)
        return state

    except Exception as e:
        logger.exception("❌ STATE DAILY training failed: %s", state)
        return {
            "state": state,
            "status": "failed",
            "error": str(e),
        }





@task
def train_state_5min_task(state: str):
    logger.info("🚀 Training STATE 5-MIN model: %s", state)

    try:
        model = train_state_5min_model(state)
        save_model(f"state_5min_{state}.pkl", model)

        logger.info("✅ Saved STATE 5-MIN model: %s", state)
        return state

    except ValueError as e:
        # Known / expected issues (data missing etc.)
        logger.warning("⚠️ Skipping STATE %s: %s", state, e)
        return {
            "state": state,
            "status": "skipped",
            "reason": str(e),
        }

    except Exception as e:
        # Unknown / critical errors
        logger.exception("❌ STATE 5-MIN training failed: %s", state)
        return {
            "state": state,
            "status": "failed",
            "error": str(e),
        }










