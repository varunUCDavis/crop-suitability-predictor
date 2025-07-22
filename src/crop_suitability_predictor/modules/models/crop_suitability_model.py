import joblib
import numpy as np
import pandas as pd

from sklearn.metrics import mean_squared_error, r2_score

from sklearn.model_selection import train_test_split
from sklearn.multioutput import MultiOutputRegressor
import matplotlib.pyplot as plt
import seaborn as sns
from lightgbm import LGBMRegressor


class CropSuitabilityModel:
    def __init__(
        self,
        model_path="lightgbm_crop_suitability_model.pkl",
        n_estimators=200,
        learning_rate=0.05,
        random_state=42,
    ):
        self.model = MultiOutputRegressor(
            LGBMRegressor(
                n_estimators=n_estimators,
                learning_rate=learning_rate,
                random_state=random_state,
            )
        )
        self.model_path = model_path
        self.feature_cols = []
        self.target_cols = []

    def load_data(self, csv_path):
        df = pd.read_csv(csv_path)
        df = df.drop(columns=["crop_code", "suitability_score"], errors="ignore")
        self.target_cols = [
            col for col in df.columns if col.startswith("soil_suitability")
        ]
        self.feature_cols = [col for col in df.columns if col not in self.target_cols]
        X = df[self.feature_cols]
        y = df[self.target_cols]
        return train_test_split(X, y, test_size=0.2, random_state=42)

    def train(self, X_train, y_train):
        self.model.fit(X_train, y_train)

    def evaluate(self, X_test, y_test):
        y_pred = self.model.predict(X_test)
        overall_rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        overall_r2 = r2_score(y_test, y_pred)

        r2_scores = [
            (col, r2_score(y_test[col], y_pred[:, i]))
            for i, col in enumerate(self.target_cols)
        ]

        return overall_rmse, overall_r2, r2_scores, y_pred

    def save_model(self):
        joblib.dump(self.model, self.model_path)

    def load_model(self):
        self.model = joblib.load(self.model_path)

    def plot_metrics(self, overall_rmse, overall_r2, r2_scores, y_test, y_pred):
        r2_df = pd.DataFrame(r2_scores, columns=["Crop", "R2 Score"]).sort_values(
            "R2 Score", ascending=False
        )

        # Bar plot
        plt.figure(figsize=(10, 6))
        sns.barplot(data=r2_df, x="R2 Score", y="Crop", palette="viridis")
        plt.title("Per-Crop R² Scores")
        plt.tight_layout()
        plt.show()

        # Overall metrics
        plt.figure(figsize=(6, 4))
        sns.barplot(
            x=["Overall R²", "RMSE"], y=[overall_r2, overall_rmse], palette="pastel"
        )
        plt.title("Overall Performance Metrics")
        plt.tight_layout()
        plt.show()

        # Top 3 crops
        top_3 = r2_df.head(3)["Crop"].tolist()
        for crop in top_3:
            i = self.target_cols.index(crop)
            plt.figure(figsize=(6, 6))
            sns.scatterplot(x=y_test[crop], y=y_pred[:, i])
            plt.plot([0, 1], [0, 1], color="red", linestyle="--")
            plt.title(f"Actual vs Predicted: {crop}")
            plt.xlabel("Actual")
            plt.ylabel("Predicted")
            plt.tight_layout()
            plt.show()
