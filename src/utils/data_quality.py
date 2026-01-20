"""
Data Quality utilities for validating and monitoring data integrity.
Integrates with Great Expectations for comprehensive data validation.
"""
import logging
from typing import Dict, List, Optional, Any, Tuple, Union
from datetime import datetime, timedelta, timezone
import json
import pandas as pd
from decimal import Decimal

from src.config import settings

logger = logging.getLogger(__name__)


class DataQualityValidator:
    """Central validator for data quality checks."""
    
    def __init__(self):
        """Initialize data quality validator."""
        self.settings = settings
        self.validation_rules = self._load_validation_rules()
        self.stats = {
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'critical_failures': 0
        }
    
    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules configuration."""
        return {
            'transaction': {
                'required_fields': [
                    'transaction_id', 'timestamp', 'customer_id', 
                    'amount', 'currency', 'merchant_id', 'status'
                ],
                'field_types': {
                    'transaction_id': str,
                    'timestamp': (int, str),
                    'customer_id': str,
                    'amount': (str, float, Decimal),
                    'currency': str,
                    'merchant_id': str,
                    'status': str
                },
                'field_ranges': {
                    'amount': (0.01, 50000.00),  # $0.01 to $50,000
                    'currency': ['USD', 'EUR', 'GBP', 'CAD', 'JPY'],
                    'status': ['PENDING', 'APPROVED', 'DECLINED', 'CANCELLED', 'REFUNDED']
                },
                'field_patterns': {
                    'transaction_id': r'^[a-f0-9-]{36}$',  # UUID format
                    'currency': r'^[A-Z]{3}$',  # ISO currency code
                },
                'business_rules': {
                    'max_daily_amount_per_customer': 10000.00,
                    'max_transaction_amount': 25000.00,
                    'min_transaction_amount': 0.01,
                    'future_timestamp_tolerance_hours': 1
                }
            },
            'fraud_alert': {
                'required_fields': [
                    'alert_id', 'transaction_id', 'customer_id_hash',
                    'alert_type', 'severity', 'risk_score'
                ],
                'field_ranges': {
                    'risk_score': (0.0, 1.0),
                    'confidence_score': (0.0, 1.0),
                    'severity': ['LOW', 'MEDIUM', 'HIGH', 'CRITICAL']
                }
            }
        }
    
    def validate_transaction(self, transaction: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a single transaction record.
        
        Args:
            transaction: Transaction dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.stats['total_validations'] += 1
        errors = []
        
        # Check required fields
        missing_fields = self._check_required_fields(
            transaction, 
            self.validation_rules['transaction']['required_fields']
        )
        errors.extend(missing_fields)
        
        # Check field types
        type_errors = self._check_field_types(
            transaction,
            self.validation_rules['transaction']['field_types']
        )
        errors.extend(type_errors)
        
        # Check field ranges and values
        range_errors = self._check_field_ranges(
            transaction,
            self.validation_rules['transaction']['field_ranges']
        )
        errors.extend(range_errors)
        
        # Check field patterns
        pattern_errors = self._check_field_patterns(
            transaction,
            self.validation_rules['transaction']['field_patterns']
        )
        errors.extend(pattern_errors)
        
        # Check business rules
        business_errors = self._check_business_rules(transaction)
        errors.extend(business_errors)
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.stats['passed_validations'] += 1
        else:
            self.stats['failed_validations'] += 1
            
            # Check for critical errors
            critical_keywords = ['required', 'invalid_amount', 'future_timestamp']
            if any(keyword in error.lower() for error in errors for keyword in critical_keywords):
                self.stats['critical_failures'] += 1
        
        return is_valid, errors
    
    def validate_fraud_alert(self, alert: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate a fraud alert record.
        
        Args:
            alert: Fraud alert dictionary to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        self.stats['total_validations'] += 1
        errors = []
        
        # Check required fields
        missing_fields = self._check_required_fields(
            alert,
            self.validation_rules['fraud_alert']['required_fields']
        )
        errors.extend(missing_fields)
        
        # Check field ranges
        range_errors = self._check_field_ranges(
            alert,
            self.validation_rules['fraud_alert']['field_ranges']
        )
        errors.extend(range_errors)
        
        is_valid = len(errors) == 0
        
        if is_valid:
            self.stats['passed_validations'] += 1
        else:
            self.stats['failed_validations'] += 1
        
        return is_valid, errors
    
    def _check_required_fields(self, data: Dict[str, Any], required_fields: List[str]) -> List[str]:
        """Check if all required fields are present and not None."""
        errors = []
        
        for field in required_fields:
            if field not in data:
                errors.append(f"Missing required field: {field}")
            elif data[field] is None:
                errors.append(f"Required field is None: {field}")
            elif isinstance(data[field], str) and data[field].strip() == "":
                errors.append(f"Required field is empty: {field}")
        
        return errors
    
    def _check_field_types(self, data: Dict[str, Any], field_types: Dict[str, Any]) -> List[str]:
        """Check if field types match expected types."""
        errors = []
        
        for field, expected_type in field_types.items():
            if field in data and data[field] is not None:
                value = data[field]
                
                if isinstance(expected_type, tuple):
                    # Multiple allowed types
                    if not isinstance(value, expected_type):
                        errors.append(
                            f"Field {field} has wrong type. "
                            f"Expected {expected_type}, got {type(value)}"
                        )
                else:
                    # Single expected type
                    if not isinstance(value, expected_type):
                        errors.append(
                            f"Field {field} has wrong type. "
                            f"Expected {expected_type}, got {type(value)}"
                        )
        
        return errors
    
    def _check_field_ranges(self, data: Dict[str, Any], field_ranges: Dict[str, Any]) -> List[str]:
        """Check if field values are within expected ranges."""
        errors = []
        
        for field, range_constraint in field_ranges.items():
            if field in data and data[field] is not None:
                value = data[field]
                
                if isinstance(range_constraint, tuple) and len(range_constraint) == 2:
                    # Numeric range
                    min_val, max_val = range_constraint
                    try:
                        numeric_value = float(value) if isinstance(value, str) else value
                        if numeric_value < min_val or numeric_value > max_val:
                            errors.append(
                                f"Field {field} value {numeric_value} is outside "
                                f"valid range [{min_val}, {max_val}]"
                            )
                    except (ValueError, TypeError):
                        errors.append(f"Field {field} cannot be converted to numeric for range check")
                
                elif isinstance(range_constraint, list):
                    # Allowed values list
                    if value not in range_constraint:
                        errors.append(
                            f"Field {field} value '{value}' is not in allowed values: {range_constraint}"
                        )
        
        return errors
    
    def _check_field_patterns(self, data: Dict[str, Any], field_patterns: Dict[str, str]) -> List[str]:
        """Check if field values match expected patterns."""
        import re
        errors = []
        
        for field, pattern in field_patterns.items():
            if field in data and data[field] is not None:
                value = str(data[field])
                
                if not re.match(pattern, value):
                    errors.append(f"Field {field} value '{value}' does not match pattern {pattern}")
        
        return errors
    
    def _check_business_rules(self, transaction: Dict[str, Any]) -> List[str]:
        """Check business-specific validation rules."""
        errors = []
        
        # Check transaction amount limits
        if 'amount' in transaction:
            try:
                amount = float(transaction['amount'])
                
                max_amount = self.validation_rules['transaction']['business_rules']['max_transaction_amount']
                min_amount = self.validation_rules['transaction']['business_rules']['min_transaction_amount']
                
                if amount > max_amount:
                    errors.append(f"Transaction amount {amount} exceeds maximum allowed {max_amount}")
                
                if amount < min_amount:
                    errors.append(f"Transaction amount {amount} below minimum allowed {min_amount}")
                    
            except (ValueError, TypeError):
                errors.append("Invalid transaction amount format")
        
        # Check timestamp is not too far in the future
        if 'timestamp' in transaction:
            try:
                if isinstance(transaction['timestamp'], str):
                    transaction_time = datetime.fromisoformat(transaction['timestamp'].replace('Z', '+00:00'))
                elif isinstance(transaction['timestamp'], int):
                    transaction_time = datetime.fromtimestamp(transaction['timestamp'] / 1000)
                else:
                    transaction_time = transaction['timestamp']
                
                now = datetime.now(timezone.utc)
                tolerance_hours = self.validation_rules['transaction']['business_rules']['future_timestamp_tolerance_hours']
                max_future_time = now + timedelta(hours=tolerance_hours)
                
                if transaction_time > max_future_time:
                    errors.append(f"Transaction timestamp is too far in the future: {transaction_time}")
                    
            except (ValueError, TypeError, AttributeError):
                errors.append("Invalid timestamp format")
        
        # Check currency and amount consistency
        if 'currency' in transaction and 'amount' in transaction:
            currency = transaction['currency']
            try:
                amount = float(transaction['amount'])
                
                # Different validation rules based on currency
                if currency == 'JPY':
                    # JPY typically doesn't have decimal places
                    if amount != int(amount):
                        errors.append("JPY amounts should not have decimal places")
                elif currency in ['USD', 'EUR', 'GBP', 'CAD']:
                    # These currencies should have max 2 decimal places
                    if round(amount, 2) != amount:
                        errors.append(f"{currency} amounts should have at most 2 decimal places")
                        
            except (ValueError, TypeError):
                pass  # Already caught in amount validation
        
        return errors
    
    def validate_batch(self, records: List[Dict[str, Any]], record_type: str = 'transaction') -> Dict[str, Any]:
        """
        Validate a batch of records.
        
        Args:
            records: List of records to validate
            record_type: Type of records ('transaction' or 'fraud_alert')
            
        Returns:
            Validation summary
        """
        batch_start_time = datetime.now(timezone.utc)
        
        validation_func = (
            self.validate_transaction if record_type == 'transaction' 
            else self.validate_fraud_alert
        )
        
        valid_records = []
        invalid_records = []
        all_errors = []
        
        for i, record in enumerate(records):
            is_valid, errors = validation_func(record)
            
            if is_valid:
                valid_records.append(record)
            else:
                invalid_record = {
                    'record_index': i,
                    'record': record,
                    'errors': errors
                }
                invalid_records.append(invalid_record)
                all_errors.extend(errors)
        
        batch_end_time = datetime.now(timezone.utc)
        processing_time = (batch_end_time - batch_start_time).total_seconds()
        
        # Calculate error frequencies
        error_frequencies = {}
        for error in all_errors:
            error_type = error.split(':')[0] if ':' in error else error
            error_frequencies[error_type] = error_frequencies.get(error_type, 0) + 1
        
        validation_summary = {
            'batch_size': len(records),
            'valid_count': len(valid_records),
            'invalid_count': len(invalid_records),
            'validation_rate': len(valid_records) / len(records) if records else 0,
            'processing_time_seconds': processing_time,
            'records_per_second': len(records) / processing_time if processing_time > 0 else 0,
            'error_frequencies': error_frequencies,
            'invalid_records': invalid_records,
            'timestamp': batch_start_time.isoformat()
        }
        
        return validation_summary
    
    def get_data_quality_score(self, validation_summary: Dict[str, Any]) -> float:
        """Calculate overall data quality score (0-1)."""
        if validation_summary['batch_size'] == 0:
            return 0.0
        
        base_score = validation_summary['validation_rate']
        
        # Penalize critical errors more heavily
        if validation_summary['invalid_count'] > 0:
            critical_penalty = min(0.2, self.stats['critical_failures'] / validation_summary['batch_size'])
            base_score -= critical_penalty
        
        return max(0.0, base_score)
    
    def generate_data_quality_report(self) -> Dict[str, Any]:
        """Generate comprehensive data quality report."""
        total_validations = self.stats['total_validations']
        
        if total_validations == 0:
            return {
                'report_timestamp': datetime.now(timezone.utc).isoformat(),
                'status': 'NO_DATA',
                'message': 'No validations performed yet'
            }
        
        overall_pass_rate = self.stats['passed_validations'] / total_validations
        critical_failure_rate = self.stats['critical_failures'] / total_validations
        
        # Determine status
        if overall_pass_rate >= self.settings.data_quality.alert_threshold:
            status = 'HEALTHY'
        elif overall_pass_rate >= 0.8:
            status = 'WARNING'
        else:
            status = 'CRITICAL'
        
        return {
            'report_timestamp': datetime.now(timezone.utc).isoformat(),
            'status': status,
            'overall_statistics': {
                'total_validations': total_validations,
                'passed_validations': self.stats['passed_validations'],
                'failed_validations': self.stats['failed_validations'],
                'critical_failures': self.stats['critical_failures'],
                'pass_rate': overall_pass_rate,
                'critical_failure_rate': critical_failure_rate
            },
            'quality_score': overall_pass_rate,
            'recommendations': self._generate_recommendations(overall_pass_rate, critical_failure_rate)
        }
    
    def _generate_recommendations(self, pass_rate: float, critical_rate: float) -> List[str]:
        """Generate data quality improvement recommendations."""
        recommendations = []
        
        if pass_rate < 0.9:
            recommendations.append("Overall data quality is below optimal threshold (90%)")
        
        if critical_rate > 0.01:  # More than 1% critical failures
            recommendations.append("High rate of critical data quality failures detected")
            recommendations.append("Review data pipeline and validation rules")
        
        if pass_rate < 0.8:
            recommendations.append("Consider implementing data quality gates before processing")
            recommendations.append("Review upstream data sources for quality issues")
        
        if not recommendations:
            recommendations.append("Data quality metrics are within acceptable ranges")
        
        return recommendations
    
    def reset_stats(self):
        """Reset validation statistics."""
        self.stats = {
            'total_validations': 0,
            'passed_validations': 0,
            'failed_validations': 0,
            'critical_failures': 0
        }


class DataQualityMonitor:
    """Monitor for tracking data quality metrics over time."""
    
    def __init__(self):
        """Initialize data quality monitor."""
        self.settings = settings
        self.validator = DataQualityValidator()
        self.quality_history = []
    
    def add_quality_metrics(self, validation_summary: Dict[str, Any]):
        """Add quality metrics to history."""
        quality_score = self.validator.get_data_quality_score(validation_summary)
        
        metrics = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'quality_score': quality_score,
            'validation_rate': validation_summary['validation_rate'],
            'batch_size': validation_summary['batch_size'],
            'processing_time': validation_summary['processing_time_seconds'],
            'error_count': validation_summary['invalid_count']
        }
        
        self.quality_history.append(metrics)
        
        # Keep only last 1000 entries
        if len(self.quality_history) > 1000:
            self.quality_history = self.quality_history[-1000:]
    
    def get_quality_trend(self, hours: int = 24) -> Dict[str, Any]:
        """Get data quality trend over specified hours."""
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours)
        
        recent_metrics = [
            m for m in self.quality_history
            if datetime.fromisoformat(m['timestamp']) > cutoff_time
        ]
        
        if not recent_metrics:
            return {
                'trend': 'NO_DATA',
                'message': f'No data available for the last {hours} hours'
            }
        
        # Calculate trend
        quality_scores = [m['quality_score'] for m in recent_metrics]
        avg_quality = sum(quality_scores) / len(quality_scores)
        
        if len(quality_scores) >= 2:
            recent_avg = sum(quality_scores[-10:]) / len(quality_scores[-10:])
            older_avg = sum(quality_scores[:-10]) / len(quality_scores[:-10]) if len(quality_scores) > 10 else recent_avg
            
            if recent_avg > older_avg + 0.02:
                trend = 'IMPROVING'
            elif recent_avg < older_avg - 0.02:
                trend = 'DEGRADING'
            else:
                trend = 'STABLE'
        else:
            trend = 'INSUFFICIENT_DATA'
        
        return {
            'trend': trend,
            'average_quality_score': avg_quality,
            'data_points': len(recent_metrics),
            'time_range_hours': hours,
            'latest_score': quality_scores[-1] if quality_scores else None
        }
    
    def check_quality_alerts(self) -> List[Dict[str, Any]]:
        """Check if any quality alerts should be triggered."""
        alerts = []
        
        if not self.quality_history:
            return alerts
        
        latest_metrics = self.quality_history[-1]
        quality_score = latest_metrics['quality_score']
        
        # Check if quality score is below threshold
        if quality_score < self.settings.data_quality.alert_threshold:
            alerts.append({
                'alert_type': 'QUALITY_THRESHOLD_BREACH',
                'severity': 'HIGH' if quality_score < 0.8 else 'MEDIUM',
                'message': f"Data quality score {quality_score:.3f} is below threshold {self.settings.data_quality.alert_threshold}",
                'timestamp': latest_metrics['timestamp'],
                'quality_score': quality_score
            })
        
        # Check trend
        trend_data = self.get_quality_trend(hours=2)  # Short-term trend
        if trend_data['trend'] == 'DEGRADING':
            alerts.append({
                'alert_type': 'QUALITY_DEGRADATION',
                'severity': 'MEDIUM',
                'message': 'Data quality trend is degrading over the last 2 hours',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'trend': trend_data
            })
        
        return alerts
