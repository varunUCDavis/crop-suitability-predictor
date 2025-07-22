# 1. Import the class (assuming it's in crop_model.py or the same script)
from crop_suitability_predictor.modules.models.crop_suitability_model import (
    CropSuitabilityModel,
)
from crop_suitability_predictor.config.config import (
    REDUCED_SOIL_FEATURES_FILE_PATH,
    SUITABILITY_SCORE_ALL_POINTS_FILE_PATH,
)  # or just define it above if in same file


def train_and_save_model():
    breakpoint()
    # 2. Initialize the model
    model = CropSuitabilityModel()

    # 3. Load and split the data
    X_train, X_test, y_train, y_test = model.load_data(
        SUITABILITY_SCORE_ALL_POINTS_FILE_PATH
    )

    # 4. Train the model
    model.train(X_train, y_train)

    # 5. Evaluate the model
    overall_rmse, overall_r2, r2_scores, y_pred = model.evaluate(X_test, y_test)

    # 6. Plot the metrics
    model.plot_metrics(overall_rmse, overall_r2, r2_scores, y_test, y_pred)

    # 7. Save the model to disk
    model.save_model()


def main() -> None:
    train_and_save_model()


if __name__ == "__main__":
    main()
