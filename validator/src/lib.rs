use pyo3::prelude::*;
use pyo3::types::{PyDict, PyModule};

/// Результат валидации транзакции
#[pyclass]
#[derive(Debug, Clone)]
struct ValidationResult {
    #[pyo3(get)]
    is_valid: bool,
    #[pyo3(get)]
    errors: Vec<String>,
}

#[pymethods]
impl ValidationResult {
    fn __repr__(&self) -> String {
        format!(
            "ValidationResult(is_valid={}, errors={:?})",
            self.is_valid, self.errors
        )
    }
}

/// Внутренняя функция валидации — работает с обычными Rust-типами
/// чтобы можно было тестировать без Python runtime
fn validate_fields(
    amount: f64,
    currency: &str,
    account_from: &str,
    account_to: &str,
    status: &str,
    tx_type: &str,
) -> Vec<String> {
    let mut errors = Vec::new();

    // Проверка суммы
    if amount <= 0.0 {
        errors.push(format!("Invalid amount: {} (must be > 0)", amount));
    }
    if amount > 10_000_000.0 {
        errors.push(format!("Amount {} exceeds limit 10,000,000", amount));
    }

    // Проверка валюты
    let allowed_currencies = ["RUB", "USD", "EUR", "CNY", "GBP"];
    if !allowed_currencies.contains(&currency) {
        errors.push(format!("Unknown currency: {}", currency));
    }

    // Проверка счетов
    if account_from.is_empty() {
        errors.push("account_from is required".to_string());
    }
    if account_to.is_empty() {
        errors.push("account_to is required".to_string());
    }
    if !account_from.is_empty() && account_from == account_to {
        errors.push("account_from and account_to must be different".to_string());
    }

    // Проверка статуса
    let allowed_statuses = ["success", "failed", "pending"];
    if !allowed_statuses.contains(&status) {
        errors.push(format!("Unknown status: {}", status));
    }

    // Проверка типа
    let allowed_types = ["transfer", "payment", "withdrawal"];
    if !allowed_types.contains(&tx_type) {
        errors.push(format!("Unknown type: {}", tx_type));
    }

    errors
}

/// Валидация одной транзакции через Python dict
#[pyfunction]
fn validate_transaction(tx: &Bound<'_, PyDict>) -> PyResult<ValidationResult> {
    let amount: f64 = tx
        .get_item("amount")?
        .and_then(|v| v.extract::<f64>().ok())
        .unwrap_or(-1.0);

    let currency: String = tx
        .get_item("currency")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let account_from: String = tx
        .get_item("account_from")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let account_to: String = tx
        .get_item("account_to")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let status: String = tx
        .get_item("status")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let tx_type: String = tx
        .get_item("type")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let errors = validate_fields(
        amount,
        &currency,
        &account_from,
        &account_to,
        &status,
        &tx_type,
    );

    Ok(ValidationResult {
        is_valid: errors.is_empty(),
        errors,
    })
}

/// Валидация батча — возвращает только невалидные
#[pyfunction]
fn validate_batch(transactions: Vec<Bound<'_, PyDict>>) -> PyResult<Vec<ValidationResult>> {
    let mut invalid = Vec::new();
    for tx in &transactions {
        let result = validate_transaction(tx)?;
        if !result.is_valid {
            invalid.push(result);
        }
    }
    Ok(invalid)
}

/// Статистика валидации батча
#[pyfunction]
fn batch_stats(transactions: Vec<Bound<'_, PyDict>>) -> PyResult<PyObject> {
    let mut valid = 0usize;
    let mut invalid = 0usize;
    let mut total_amount = 0.0f64;

    for tx in &transactions {
        let result = validate_transaction(tx)?;
        if result.is_valid {
            valid += 1;
            let amount: f64 = tx
                .get_item("amount")?
                .and_then(|v| v.extract::<f64>().ok())
                .unwrap_or(0.0);
            total_amount += amount;
        } else {
            invalid += 1;
        }
    }

    Python::with_gil(|py| {
        let dict = PyDict::new_bound(py);
        dict.set_item("total", transactions.len())?;
        dict.set_item("valid", valid)?;
        dict.set_item("invalid", invalid)?;
        dict.set_item("valid_total_amount", total_amount)?;
        Ok(dict.into())
    })
}

#[pymodule]
fn tx_validator(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<ValidationResult>()?;
    m.add_function(wrap_pyfunction!(validate_transaction, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    m.add_function(wrap_pyfunction!(batch_stats, m)?)?;
    Ok(())
}

// ============================================================
// Unit-тесты (работают без Python runtime через rlib)
// ============================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_transaction() {
        let errors = validate_fields(
            5000.0, "RUB", "ACC00000001", "ACC00000002", "success", "transfer",
        );
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_negative_amount() {
        let errors = validate_fields(
            -100.0, "RUB", "ACC00000001", "ACC00000002", "success", "transfer",
        );
        assert!(errors.iter().any(|e| e.contains("Invalid amount")));
    }

    #[test]
    fn test_zero_amount() {
        let errors = validate_fields(
            0.0, "RUB", "ACC00000001", "ACC00000002", "success", "payment",
        );
        assert!(errors.iter().any(|e| e.contains("Invalid amount")));
    }

    #[test]
    fn test_amount_exceeds_limit() {
        let errors = validate_fields(
            15_000_000.0, "USD", "ACC00000001", "ACC00000002", "success", "transfer",
        );
        assert!(errors.iter().any(|e| e.contains("exceeds limit")));
    }

    #[test]
    fn test_unknown_currency() {
        let errors = validate_fields(
            1000.0, "JPY", "ACC00000001", "ACC00000002", "success", "payment",
        );
        assert!(errors.iter().any(|e| e.contains("Unknown currency")));
    }

    #[test]
    fn test_empty_currency() {
        let errors = validate_fields(
            1000.0, "", "ACC00000001", "ACC00000002", "success", "payment",
        );
        assert!(errors.iter().any(|e| e.contains("Unknown currency")));
    }

    #[test]
    fn test_same_accounts() {
        let errors = validate_fields(
            1000.0, "RUB", "ACC00000001", "ACC00000001", "success", "transfer",
        );
        assert!(errors.iter().any(|e| e.contains("must be different")));
    }

    #[test]
    fn test_empty_account_from() {
        let errors = validate_fields(
            1000.0, "RUB", "", "ACC00000002", "success", "transfer",
        );
        assert!(errors.iter().any(|e| e.contains("account_from is required")));
    }

    #[test]
    fn test_empty_account_to() {
        let errors = validate_fields(
            1000.0, "RUB", "ACC00000001", "", "success", "transfer",
        );
        assert!(errors.iter().any(|e| e.contains("account_to is required")));
    }

    #[test]
    fn test_unknown_status() {
        let errors = validate_fields(
            1000.0, "RUB", "ACC00000001", "ACC00000002", "approved", "transfer",
        );
        assert!(errors.iter().any(|e| e.contains("Unknown status")));
    }

    #[test]
    fn test_unknown_type() {
        let errors = validate_fields(
            1000.0, "USD", "ACC00000001", "ACC00000002", "success", "refund",
        );
        assert!(errors.iter().any(|e| e.contains("Unknown type")));
    }

    #[test]
    fn test_multiple_errors() {
        let errors = validate_fields(
            -50.0, "XYZ", "", "", "unknown", "invalid",
        );
        assert!(errors.len() >= 4, "Expected multiple errors, got: {:?}", errors);
    }

    #[test]
    fn test_all_valid_currencies() {
        for currency in &["RUB", "USD", "EUR", "CNY", "GBP"] {
            let errors = validate_fields(
                1000.0, currency, "ACC00000001", "ACC00000002", "success", "payment",
            );
            assert!(
                errors.is_empty(),
                "Currency {} should be valid, got: {:?}", currency, errors
            );
        }
    }

    #[test]
    fn test_all_valid_statuses() {
        for status in &["success", "failed", "pending"] {
            let errors = validate_fields(
                1000.0, "RUB", "ACC00000001", "ACC00000002", status, "transfer",
            );
            assert!(
                errors.is_empty(),
                "Status {} should be valid, got: {:?}", status, errors
            );
        }
    }

    #[test]
    fn test_all_valid_types() {
        for tx_type in &["transfer", "payment", "withdrawal"] {
            let errors = validate_fields(
                1000.0, "RUB", "ACC00000001", "ACC00000002", "success", tx_type,
            );
            assert!(
                errors.is_empty(),
                "Type {} should be valid, got: {:?}", tx_type, errors
            );
        }
    }
}