import logging
import os.path
import pickle

import numpy as np

LOGGING_FORMAT = "%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s"
# initialize logger
logger = logging.getLogger("install")
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

models = {}


def predict(
    sensor_type,
    value,
    models_location="/home/ojedapi4/miniforge-pypy3/envs/scd41GadgetEnv/src/models",
):
    """
    Runs the anomaly detection on the loaded data
    :param sensor_type: the type of the sensor
    :param value: the value to test
    :param models_location: the location to load the models from
    :return: 1 for normal value, -1 for abnormal, None if no model exists
    """

    # check if model exists and load it if no already loaded
    if sensor_type.lower() not in models:
        model_path = f"{models_location}/if_{sensor_type}_best.pkl"
        if os.path.exists(model_path):
            with open(model_path, "rb") as file:
                models[sensor_type] = pickle.load(file)
                logger.info(f"loaded model for sensor: {sensor_type}")
        else:
            # no model exists return None
            print("Model does not exist")
            return None

    if sensor_type.lower() in models:
        return models[sensor_type.lower()].predict(np.array(value).reshape(1, -1))
    else:
        return None


# # benchmark temperature values
# sensor_type = "temperature"
# for sensor_value in range(10, 40, 1):
#     prediction = predict(sensor_type, sensor_value)
#     logger.info(f"type: {sensor_type} value: {sensor_value} prediction: {prediction}")

# # benchmark humidity values
# sensor_type = "humidity"
# for sensor_value in range(10, 80, 4):
#     prediction = predict(sensor_type, sensor_value)
#     logger.info(f"type: {sensor_type} value: {sensor_value} prediction: {prediction}")
