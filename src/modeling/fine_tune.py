"""Execute grid search to fine-tune a pre-trained transformer model for sentiment classification."""

import itertools
import os

from datasets import ClassLabel, load_dataset
from huggingface_hub import login
from loguru import logger
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    Trainer,
    TrainingArguments,
)

from src.config import INTERIM_DATA_DIR, MODELS_DIR
from src.evaluation.metrics import compute_metrics

MODEL_ID = "cardiffnlp/twitter-xlm-roberta-base-sentiment"


def fine_tune(sample_path=INTERIM_DATA_DIR / "df_true_sentiment_samples.parquet") -> None:
    """Execute a grid search to fine-tune the sentiment classification model and save the best performer.

    Args:
        sample_path (Any, optional): The file path to the prepared Parquet dataset.
            Defaults to INTERIM_DATA_DIR / "df_true_sentiment_samples.parquet".
    """
    HF_TOKEN = os.environ.get("HF_TOKEN", "")
    if not HF_TOKEN:
        logger.error("Token not found.")
    else:
        login(token=HF_TOKEN)

    sample_path_str = str(sample_path)

    df = load_dataset("parquet", data_files=sample_path_str)
    hf_dataset = df["train"]

    sentiment_labels = ClassLabel(num_classes=3, names=["negative", "neutral", "positive"])
    hf_dataset = hf_dataset.cast_column("label", sentiment_labels)

    hf_dataset = hf_dataset.train_test_split(test_size=0.2, stratify_by_column="label")
    tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)

    def tokenize_function(examples):
        return tokenizer(examples["text"], padding="max_length", truncation=True, max_length=512)

    tokenized_datasets = hf_dataset.map(tokenize_function, batched=True)

    # Grid Search Setup
    learning_rates = [1e-5, 2e-5, 3e-5]
    epochs_list = [2, 3, 4]
    grid = list(itertools.product(learning_rates, epochs_list))

    best_macro_f1 = 0.0
    best_params = None
    best_model_path = str(MODELS_DIR / "best-kpop-sentiment-model")

    for lr, epochs in grid:
        logger.info(f"--- Testing Learning Rate: {lr} | Epochs: {epochs} ---\n")
        model = AutoModelForSequenceClassification.from_pretrained(MODEL_ID)

        output_dir = str(MODELS_DIR / f"sentiment-model-fine-tuning/lr_{lr}_ep_{epochs}")
        training_args = TrainingArguments(
            output_dir=output_dir,
            learning_rate=lr,
            num_train_epochs=epochs,
            per_device_train_batch_size=16,
            per_device_eval_batch_size=16,
            eval_strategy="epoch",
            save_strategy="epoch",
            load_best_model_at_end=True,
            metric_for_best_model="macro_f1",
            greater_is_better=True,
            logging_steps=10,
        )

        trainer = Trainer(
            model=model,
            args=training_args,
            train_dataset=tokenized_datasets["train"],
            eval_dataset=tokenized_datasets["test"],
            compute_metrics=compute_metrics,
        )

        trainer.train()
        eval_results = trainer.evaluate()

        current_macro_f1 = eval_results["eval_macro_f1"]
        if current_macro_f1 > best_macro_f1:
            best_macro_f1 = current_macro_f1
            best_params = {"learning_rate": lr, "epochs": epochs}  # noqa: F841
            logger.info(f"New Best Model Found (Macro F1: {best_macro_f1:.4f})! Saving...\n")

            trainer.save_model(best_model_path)
            tokenizer.save_pretrained(best_model_path)


if __name__ == "__main__":
    fine_tune()
