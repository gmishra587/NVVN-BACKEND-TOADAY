from datetime import date
from power.utils.logger import get_logger
from power.ml.task import (train_region_task,train_state_daily_task,train_state_5min_task)

logger = get_logger(__name__)





STATE_TO_REGION = {
    "DL": "NR", "MH": "WR", "TN": "SR", "UP": "NR", "AP": "SR", "AR": "NER",
    "AS": "NER", "BR": "ER", "CH": "NR", "CG": "WR", "GA": "WR", "GJ": "WR",
    "HR": "NR", "HP": "NR", "JK": "NR", "JH": "ER", "KA": "SR", "KL": "SR",
    "MN": "NER", "ML": "NER", "MZ": "NER", "MP": "WR", "NL": "NER", "OD": "ER",
    "PY": "SR", "PB": "NR", "RJ": "WR", "SK": "NER", "TS": "SR", "TR": "NER",
    "UK": "NR", "WB": "ER",
}





def train_all_models():
    # ---------- REGION MODELS ----------
    # logger.info("\nStarting REGION models training\n")

    # for region_code in set(STATE_TO_REGION.values()):
    #     train_region_task.enqueue(region_code)




    # ---------- STATE DAILY MODELS ----------
    # logger.info("\nStarting STATE DAILY models training\n")

    # for state in STATE_TO_REGION.keys():
    #     train_state_daily_task.enqueue(state)




    # ---------- STATE 5-MINUTE MODELS ----------
    logger.info("\nStarting STATE 5-MINUTE models training\n")

    for state in STATE_TO_REGION.keys():
        #train_state_5min_task(state)
        train_state_5min_task.enqueue(state)

    logger.info(
        "\n\n############# ---> ALL MODEL TASKS ENQUEUED <--- ############\n\n"
    )
