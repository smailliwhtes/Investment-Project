from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
from sklearn.base import BaseEstimator, RegressorMixin


@dataclass(frozen=True)
class _OptimizerConfig:
    beta1: float = 0.9
    beta2: float = 0.999
    epsilon: float = 1e-8


class NumpyMlpRegressor(BaseEstimator, RegressorMixin):
    """Deterministic NumPy MLP regressor that fits the repo's offline ML contract."""

    def __init__(
        self,
        *,
        hidden_layer_sizes: Iterable[int] = (64, 32),
        learning_rate: float = 0.01,
        epochs: int = 300,
        l2_penalty: float = 1e-4,
        activation: str = "tanh",
        random_state: int = 42,
        patience: int = 30,
    ) -> None:
        self.hidden_layer_sizes = tuple(int(size) for size in hidden_layer_sizes)
        self.learning_rate = float(learning_rate)
        self.epochs = int(epochs)
        self.l2_penalty = float(l2_penalty)
        self.activation = activation
        self.random_state = int(random_state)
        self.patience = int(patience)

    def fit(self, X, y):
        X_arr = np.asarray(X, dtype=np.float64)
        y_arr = np.asarray(y, dtype=np.float64).reshape(-1, 1)

        if X_arr.ndim != 2:
            raise ValueError("Expected 2D feature matrix")
        if X_arr.shape[0] == 0 or X_arr.shape[1] == 0:
            raise ValueError("Cannot train neural backend on empty features")
        if X_arr.shape[0] != y_arr.shape[0]:
            raise ValueError("X and y row counts do not match")

        hidden_layers = self._resolve_hidden_layers()
        optimizer = _OptimizerConfig()
        rng = np.random.default_rng(self.random_state)

        layer_sizes = [X_arr.shape[1], *hidden_layers, 1]
        weights = []
        biases = []
        for fan_in, fan_out in zip(layer_sizes[:-1], layer_sizes[1:]):
            limit = np.sqrt(6.0 / (fan_in + fan_out))
            if self.activation == "relu":
                limit = np.sqrt(2.0 / fan_in)
            weights.append(rng.uniform(-limit, limit, size=(fan_in, fan_out)))
            biases.append(np.zeros((1, fan_out), dtype=np.float64))

        mw = [np.zeros_like(weight) for weight in weights]
        vw = [np.zeros_like(weight) for weight in weights]
        mb = [np.zeros_like(bias) for bias in biases]
        vb = [np.zeros_like(bias) for bias in biases]

        best_loss = np.inf
        best_state = None
        stagnant_epochs = 0
        loss_curve: list[float] = []

        for epoch in range(self.epochs):
            activations, predictions = self._forward(X_arr, weights, biases)
            error = predictions - y_arr
            loss = float(np.mean(np.square(error)))
            if self.l2_penalty > 0:
                loss += float(
                    self.l2_penalty * sum(np.sum(np.square(weight)) for weight in weights)
                )
            loss_curve.append(loss)

            if loss + 1e-10 < best_loss:
                best_loss = loss
                best_state = (
                    [weight.copy() for weight in weights],
                    [bias.copy() for bias in biases],
                )
                stagnant_epochs = 0
            else:
                stagnant_epochs += 1
                if stagnant_epochs >= self.patience:
                    break

            weight_grads, bias_grads = self._backward(
                activations=activations,
                y_true=y_arr,
                weights=weights,
                predictions=predictions,
            )
            self._apply_adam(
                weights=weights,
                biases=biases,
                weight_grads=weight_grads,
                bias_grads=bias_grads,
                mw=mw,
                vw=vw,
                mb=mb,
                vb=vb,
                optimizer=optimizer,
                step=epoch + 1,
            )

        if best_state is None:
            raise RuntimeError("Neural backend failed to initialize training state")

        self.weights_, self.biases_ = best_state
        self.loss_curve_ = loss_curve
        self.n_iter_ = len(loss_curve)
        self.n_features_in_ = X_arr.shape[1]
        self.feature_importances_ = self._compute_feature_importances(self.weights_)
        return self

    def predict(self, X):
        self._ensure_fitted()
        X_arr = np.asarray(X, dtype=np.float64)
        if X_arr.ndim != 2:
            raise ValueError("Expected 2D feature matrix")
        _, predictions = self._forward(X_arr, self.weights_, self.biases_)
        return predictions.ravel()

    def _resolve_hidden_layers(self) -> tuple[int, ...]:
        if not self.hidden_layer_sizes:
            raise ValueError("hidden_layer_sizes must contain at least one layer")
        layers = tuple(int(size) for size in self.hidden_layer_sizes if int(size) > 0)
        if not layers:
            raise ValueError("hidden_layer_sizes must contain positive integers")
        return layers

    def _activation(self, values: np.ndarray) -> np.ndarray:
        if self.activation == "tanh":
            return np.tanh(values)
        if self.activation == "relu":
            return np.maximum(values, 0.0)
        raise ValueError(f"Unsupported activation: {self.activation}")

    def _activation_grad(self, activated: np.ndarray) -> np.ndarray:
        if self.activation == "tanh":
            return 1.0 - np.square(activated)
        if self.activation == "relu":
            return (activated > 0.0).astype(np.float64)
        raise ValueError(f"Unsupported activation: {self.activation}")

    def _forward(
        self,
        X: np.ndarray,
        weights: list[np.ndarray],
        biases: list[np.ndarray],
    ) -> tuple[list[np.ndarray], np.ndarray]:
        activations = [X]
        current = X
        for layer_index, (weight, bias) in enumerate(zip(weights, biases)):
            linear = current @ weight + bias
            if layer_index == len(weights) - 1:
                current = linear
            else:
                current = self._activation(linear)
            activations.append(current)
        return activations, current

    def _backward(
        self,
        *,
        activations: list[np.ndarray],
        y_true: np.ndarray,
        weights: list[np.ndarray],
        predictions: np.ndarray,
    ) -> tuple[list[np.ndarray], list[np.ndarray]]:
        sample_count = y_true.shape[0]
        delta = (2.0 / sample_count) * (predictions - y_true)
        weight_grads = [np.zeros_like(weight) for weight in weights]
        bias_grads = [np.zeros((1, weight.shape[1]), dtype=np.float64) for weight in weights]

        for layer_index in range(len(weights) - 1, -1, -1):
            previous_activation = activations[layer_index]
            weight_grads[layer_index] = previous_activation.T @ delta
            if self.l2_penalty > 0:
                weight_grads[layer_index] += self.l2_penalty * weights[layer_index]
            bias_grads[layer_index] = np.sum(delta, axis=0, keepdims=True)
            if layer_index > 0:
                propagated = delta @ weights[layer_index].T
                delta = propagated * self._activation_grad(activations[layer_index])

        return weight_grads, bias_grads

    def _apply_adam(
        self,
        *,
        weights: list[np.ndarray],
        biases: list[np.ndarray],
        weight_grads: list[np.ndarray],
        bias_grads: list[np.ndarray],
        mw: list[np.ndarray],
        vw: list[np.ndarray],
        mb: list[np.ndarray],
        vb: list[np.ndarray],
        optimizer: _OptimizerConfig,
        step: int,
    ) -> None:
        for idx in range(len(weights)):
            mw[idx] = optimizer.beta1 * mw[idx] + (1.0 - optimizer.beta1) * weight_grads[idx]
            vw[idx] = optimizer.beta2 * vw[idx] + (1.0 - optimizer.beta2) * np.square(
                weight_grads[idx]
            )
            mb[idx] = optimizer.beta1 * mb[idx] + (1.0 - optimizer.beta1) * bias_grads[idx]
            vb[idx] = optimizer.beta2 * vb[idx] + (1.0 - optimizer.beta2) * np.square(
                bias_grads[idx]
            )

            mw_hat = mw[idx] / (1.0 - optimizer.beta1**step)
            vw_hat = vw[idx] / (1.0 - optimizer.beta2**step)
            mb_hat = mb[idx] / (1.0 - optimizer.beta1**step)
            vb_hat = vb[idx] / (1.0 - optimizer.beta2**step)

            weights[idx] -= self.learning_rate * mw_hat / (np.sqrt(vw_hat) + optimizer.epsilon)
            biases[idx] -= self.learning_rate * mb_hat / (np.sqrt(vb_hat) + optimizer.epsilon)

    def _compute_feature_importances(self, weights: list[np.ndarray]) -> np.ndarray:
        propagated = np.abs(weights[0])
        for next_layer in weights[1:]:
            propagated = propagated @ np.abs(next_layer)
        importances = propagated.ravel()
        total = float(importances.sum())
        if total > 0:
            return importances / total
        return np.zeros_like(importances)

    def _ensure_fitted(self) -> None:
        if not hasattr(self, "weights_") or not hasattr(self, "biases_"):
            raise RuntimeError("Model must be fit before prediction")
