"""
Security utilities for PII protection and data masking.
Handles customer data anonymization and secure hashing.
"""
import hashlib
import hmac
import secrets
import re
from typing import Dict, List, Optional, Any, Union
import logging
from functools import lru_cache

from src.config import settings

logger = logging.getLogger(__name__)


class SecurityManager:
    """Central manager for security operations."""
    
    def __init__(self):
        """Initialize security manager."""
        self.settings = settings
        self.hasher = PIIMasker()
    
    def hash_customer_id(self, customer_id: str) -> str:
        """Hash customer ID for privacy protection."""
        return self.hasher.hash_identifier(customer_id)
    
    def mask_card_number(self, card_number: str) -> str:
        """Mask card number showing only last 4 digits."""
        return self.hasher.mask_card_number(card_number)
    
    def validate_data_integrity(self, data: Dict[str, Any]) -> bool:
        """Validate data integrity and required fields."""
        required_fields = [
            'transaction_id', 'timestamp', 'customer_id', 
            'amount', 'merchant_id', 'status'
        ]
        
        for field in required_fields:
            if field not in data or data[field] is None:
                logger.warning(f"Missing required field: {field}")
                return False
        
        return True
    
    def sanitize_merchant_name(self, merchant_name: str) -> str:
        """Sanitize merchant name for security."""
        if not merchant_name:
            return "UNKNOWN_MERCHANT"
        
        # Remove potentially harmful characters
        sanitized = re.sub(r'[<>"\']', '', merchant_name)
        
        # Limit length
        return sanitized[:100]


class PIIMasker:
    """Handles PII masking and anonymization."""
    
    def __init__(self):
        """Initialize PII masker."""
        self.settings = settings
        
        # Use salt from settings for consistent hashing
        try:
            self.salt = self.settings.security.customer_id_salt.encode()
        except AttributeError:
            # Fallback for development
            self.salt = b"default-salt-for-development-only"
            logger.warning("Using default salt - this should not be used in production!")
    
    @lru_cache(maxsize=10000)
    def hash_identifier(self, identifier: str) -> str:
        """
        Hash identifier (customer ID, etc.) with salt.
        
        Args:
            identifier: String to hash
            
        Returns:
            Hexadecimal hash string
        """
        if not identifier:
            return "UNKNOWN"
        
        # Use HMAC for secure hashing with salt
        hash_object = hmac.new(
            self.salt,
            identifier.encode('utf-8'),
            hashlib.sha256
        )
        
        return hash_object.hexdigest()[:16]  # 16 characters for reasonable uniqueness
    
    def mask_card_number(self, card_number: str) -> str:
        """
        Mask card number showing only last 4 digits.
        
        Args:
            card_number: Full card number
            
        Returns:
            Masked card number (****1234)
        """
        if not card_number:
            return "****"
        
        # Remove any non-digits
        digits_only = re.sub(r'\D', '', card_number)
        
        if len(digits_only) < 4:
            return "****"
        
        # Return last 4 digits
        return digits_only[-4:]
    
    def mask_phone_number(self, phone: str) -> str:
        """
        Mask phone number.
        
        Args:
            phone: Phone number
            
        Returns:
            Masked phone (XXX-XXX-1234)
        """
        if not phone:
            return "XXX-XXX-XXXX"
        
        # Remove any non-digits
        digits_only = re.sub(r'\D', '', phone)
        
        if len(digits_only) >= 10:
            last_four = digits_only[-4:]
            return f"XXX-XXX-{last_four}"
        else:
            return "XXX-XXX-XXXX"
    
    def mask_email(self, email: str) -> str:
        """
        Mask email address.
        
        Args:
            email: Email address
            
        Returns:
            Masked email (u***@***.com)
        """
        if not email or '@' not in email:
            return "masked@email.com"
        
        local, domain = email.split('@', 1)
        
        if len(local) > 1:
            masked_local = local[0] + '*' * (len(local) - 1)
        else:
            masked_local = '*'
        
        if '.' in domain:
            domain_parts = domain.split('.')
            masked_domain = '*' * len(domain_parts[0]) + '.' + domain_parts[-1]
        else:
            masked_domain = '***.com'
        
        return f"{masked_local}@{masked_domain}"
    
    def mask_ssn(self, ssn: str) -> str:
        """
        Mask Social Security Number.
        
        Args:
            ssn: SSN
            
        Returns:
            Masked SSN (XXX-XX-1234)
        """
        if not ssn:
            return "XXX-XX-XXXX"
        
        # Remove any non-digits
        digits_only = re.sub(r'\D', '', ssn)
        
        if len(digits_only) >= 9:
            last_four = digits_only[-4:]
            return f"XXX-XX-{last_four}"
        else:
            return "XXX-XX-XXXX"
    
    def mask_address(self, address: str) -> str:
        """
        Mask street address keeping city/state.
        
        Args:
            address: Full address
            
        Returns:
            Masked address
        """
        if not address:
            return "MASKED ADDRESS"
        
        # Simple masking - replace numbers and first part
        masked = re.sub(r'\d+', 'XXX', address)
        masked = re.sub(r'^[^,]*,', 'XXX Street,', masked)
        
        return masked
    
    def apply_transaction_masking(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply PII masking to a transaction record.
        
        Args:
            transaction: Transaction dictionary
            
        Returns:
            Masked transaction
        """
        if not self.settings.security.pii_masking_enabled:
            return transaction
        
        masked_transaction = transaction.copy()
        
        # Hash customer ID
        if 'customer_id' in masked_transaction:
            original_customer_id = masked_transaction['customer_id']
            masked_transaction['customer_id_hash'] = self.hash_identifier(original_customer_id)
            
            # Keep original for development/testing
            if not self.settings.is_production:
                masked_transaction['customer_id_original'] = original_customer_id
            
            # Remove original in production
            if self.settings.is_production:
                del masked_transaction['customer_id']
        
        # Mask card number (keep only last 4)
        if 'card_number' in masked_transaction:
            masked_transaction['card_number_last4'] = self.mask_card_number(
                masked_transaction['card_number']
            )
            del masked_transaction['card_number']
        
        # Mask any email in metadata
        if 'metadata' in masked_transaction and isinstance(masked_transaction['metadata'], dict):
            metadata = masked_transaction['metadata'].copy()
            
            for key, value in metadata.items():
                if isinstance(value, str):
                    if '@' in value and '.' in value:  # Likely email
                        metadata[key] = self.mask_email(value)
                    elif key.lower() in ['email', 'email_address', 'contact_email']:
                        metadata[key] = self.mask_email(value)
                    elif key.lower() in ['phone', 'phone_number', 'mobile']:
                        metadata[key] = self.mask_phone_number(value)
            
            masked_transaction['metadata'] = metadata
        
        # Add masking indicator
        if 'metadata' not in masked_transaction:
            masked_transaction['metadata'] = {}
        
        masked_transaction['metadata']['pii_masked'] = 'true'
        masked_transaction['metadata']['masking_timestamp'] = str(
            __import__('datetime').datetime.utcnow()
        )
        
        return masked_transaction


class DataEncryption:
    """Handles data encryption for sensitive fields."""
    
    def __init__(self):
        """Initialize encryption manager."""
        self.settings = settings
    
    def generate_key(self) -> bytes:
        """Generate a secure encryption key."""
        return secrets.token_bytes(32)  # 256-bit key
    
    def encrypt_sensitive_data(self, data: str, key: bytes) -> bytes:
        """
        Encrypt sensitive data using AES.
        
        Args:
            data: Data to encrypt
            key: Encryption key
            
        Returns:
            Encrypted data
        """
        try:
            from cryptography.fernet import Fernet
            import base64
            
            # Create Fernet key from our key
            fernet_key = base64.urlsafe_b64encode(key)
            fernet = Fernet(fernet_key)
            
            # Encrypt the data
            encrypted_data = fernet.encrypt(data.encode())
            return encrypted_data
            
        except ImportError:
            logger.warning("cryptography package not available, skipping encryption")
            return data.encode()
        except Exception as e:
            logger.error(f"Encryption failed: {e}")
            return data.encode()
    
    def decrypt_sensitive_data(self, encrypted_data: bytes, key: bytes) -> str:
        """
        Decrypt sensitive data.
        
        Args:
            encrypted_data: Encrypted data
            key: Decryption key
            
        Returns:
            Decrypted data
        """
        try:
            from cryptography.fernet import Fernet
            import base64
            
            # Create Fernet key from our key
            fernet_key = base64.urlsafe_b64encode(key)
            fernet = Fernet(fernet_key)
            
            # Decrypt the data
            decrypted_data = fernet.decrypt(encrypted_data)
            return decrypted_data.decode()
            
        except ImportError:
            logger.warning("cryptography package not available, returning as-is")
            return encrypted_data.decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            return "DECRYPTION_FAILED"


class AuditLogger:
    """Handles security audit logging."""
    
    def __init__(self):
        """Initialize audit logger."""
        self.settings = settings
        self.audit_logger = logging.getLogger('security_audit')
    
    def log_pii_access(self, user_id: str, operation: str, data_type: str):
        """Log PII data access."""
        if not self.settings.security.audit_logging_enabled:
            return
        
        audit_entry = {
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
            'user_id': user_id,
            'operation': operation,
            'data_type': data_type,
            'event_type': 'PII_ACCESS'
        }
        
        self.audit_logger.info(f"PII_ACCESS: {audit_entry}")
    
    def log_fraud_investigation(self, investigator_id: str, transaction_id: str, action: str):
        """Log fraud investigation activities."""
        if not self.settings.security.audit_logging_enabled:
            return
        
        audit_entry = {
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
            'investigator_id': investigator_id,
            'transaction_id': transaction_id,
            'action': action,
            'event_type': 'FRAUD_INVESTIGATION'
        }
        
        self.audit_logger.info(f"FRAUD_INVESTIGATION: {audit_entry}")
    
    def log_data_export(self, user_id: str, table_name: str, row_count: int):
        """Log data export activities."""
        if not self.settings.security.audit_logging_enabled:
            return
        
        audit_entry = {
            'timestamp': __import__('datetime').datetime.utcnow().isoformat(),
            'user_id': user_id,
            'table_name': table_name,
            'row_count': row_count,
            'event_type': 'DATA_EXPORT'
        }
        
        self.audit_logger.info(f"DATA_EXPORT: {audit_entry}")


def mask_transaction_for_logging(transaction: Dict[str, Any]) -> Dict[str, Any]:
    """Quick masking function for log output."""
    if not transaction:
        return {}
    
    masked = transaction.copy()
    
    # Mask sensitive fields for logging
    sensitive_fields = ['customer_id', 'card_number', 'authorization_code']
    
    for field in sensitive_fields:
        if field in masked:
            if field == 'customer_id':
                masked[field] = f"{masked[field][:4]}****" if len(str(masked[field])) > 4 else "****"
            elif field == 'card_number':
                masked[field] = f"****{str(masked[field])[-4:]}" if len(str(masked[field])) > 4 else "****"
            else:
                masked[field] = "****"
    
    return masked
