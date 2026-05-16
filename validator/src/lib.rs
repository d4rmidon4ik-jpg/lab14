use pyo3::prelude::*;
use pyo3::types::PyDict;

/// Результат валидации транзакции
#[pyclass]
#[derive(Debug)]
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

/// Валидация одной транзакции
/// Проверяет: amount > 0, currency в списке допустимых,
/// account_from != account_to, статус корректный
#[pyfunction]
fn validate_transaction(tx: &PyDict) -> PyResult<ValidationResult> {
    let mut errors = Vec::new();

    // Проверка суммы
    let amount: f64 = tx
        .get_item("amount")?
        .and_then(|v| v.extract::<f64>().ok())
        .unwrap_or(-1.0);

    if amount <= 0.0 {
        errors.push(format!("Invalid amount: {} (must be > 0)", amount));
    }
    if amount > 10_000_000.0 {
        errors.push(format!("Amount {} exceeds limit 10,000,000", amount));
    }

    // Проверка валюты
    let allowed_currencies = ["RUB", "USD", "EUR", "CNY", "GBP"];
    let currency: String = tx
        .get_item("currency")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    if !allowed_currencies.contains(&currency.as_str()) {
        errors.push(format!("Unknown currency: {}", currency));
    }

    // Проверка счетов
    let account_from: String = tx
        .get_item("account_from")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();
    let account_to: String = tx
        .get_item("account_to")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

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
    let status: String = tx
        .get_item("status")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let allowed_statuses = ["success", "failed", "pending"];
    if !allowed_statuses.contains(&status.as_str()) {
        errors.push(format!("Unknown status: {}", status));
    }

    // Проверка типа транзакции
    let tx_type: String = tx
        .get_item("type")?
        .and_then(|v| v.extract::<String>().ok())
        .unwrap_or_default();

    let allowed_types = ["transfer", "payment", "withdrawal"];
    if !allowed_types.contains(&tx_type.as_str()) {
        errors.push(format!("Unknown type: {}", tx_type));
    }

    Ok(ValidationResult {
        is_valid: errors.is_empty(),
        errors,
    })
}

/// Валидация батча транзакций — возвращает только невалидные
#[pyfunction]
fn validate_batch(transactions: Vec<&PyDict>) -> PyResult<Vec<PyObject>> {
    Python::with_gil(|py| {
        let mut invalid = Vec::new();
        for tx in transactions {
            let result = validate_transaction(tx)?;
            if !result.is_valid {
                let obj = result.into_py(py);
                invalid.push(obj);
            }
        }
        Ok(invalid)
    })
}

/// Статистика валидации батча
#[pyfunction]
fn batch_stats(transactions: Vec<&PyDict>) -> PyResult<PyObject> {
    let mut valid = 0;
    let mut invalid = 0;
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
        let dict = PyDict::new(py);
        dict.set_item("total", transactions.len())?;
        dict.set_item("valid", valid)?;
        dict.set_item("invalid", invalid)?;
        dict.set_item("valid_total_amount", total_amount)?;
        Ok(dict.into())
    })
}

#[pymodule]
fn tx_validator(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<ValidationResult>()?;
    m.add_function(wrap_pyfunction!(validate_transaction, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    m.add_function(wrap_pyfunction!(batch_stats, m)?)?;
    Ok(())
}