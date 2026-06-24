import numpy as np


def compute_metrics(eval_pred):
    logits, labels = eval_pred
    predictions = np.argmax(logits, axis=-1)
    accuracy = np.mean(predictions == labels)
    num_classes = 3
    cm = np.bincount(num_classes * labels + predictions, minlength=num_classes**2).reshape(
        num_classes, num_classes
    )

    f1_per_class = []
    support_per_class = []
    for i in range(num_classes):
        tp = cm[i, i]
        fp = np.sum(cm[:, i]) - tp
        fn = np.sum(cm[i, :]) - tp
        support = np.sum(cm[i, :])

        support_per_class.append(support)

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0

        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0.0
        f1_per_class.append(f1)

    macro_f1 = np.mean(f1_per_class)

    total_support = np.sum(support_per_class)
    weighted_f1 = (
        np.sum(np.array(f1_per_class) * np.array(support_per_class)) / total_support
        if total_support > 0
        else 0.0
    )

    print("======================== TRAINING HEALTH REPORT ========================")
    print("==================== Confusion Matrix ====================")
    print(f"{'':<12} | Pred 0 (Neg) | Pred 1 (Neu) | Pred 2 (Pos)")
    print("-" * 55)
    print(f"True 0 (Neg) | {cm[0, 0]:<12} | {cm[0, 1]:<12} | {cm[0, 2]:<12}")
    print(f"True 1 (Neu) | {cm[1, 0]:<12} | {cm[1, 1]:<12} | {cm[1, 2]:<12}")
    print(f"True 2 (Pos) | {cm[2, 0]:<12} | {cm[2, 1]:<12} | {cm[2, 2]:<12}\n")
    print(
        f"{'Overall Accuracy':<23} : {accuracy:.4f} | Macro F1 : {macro_f1:.4f} | Weighted F1 : {weighted_f1:.4f}"
    )
    print(
        f"Per-Class F1 -> Neg (0) : {f1_per_class[0]:.4f} | Neu (1)  : {f1_per_class[1]:.4f} | Pos (2)     : {f1_per_class[2]:.4f}"
    )
    print("=" * 72 + "\n\n")

    return {"macro_f1": float(macro_f1), "weighted_f1": float(weighted_f1)}
