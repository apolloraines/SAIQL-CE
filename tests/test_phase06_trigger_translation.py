#!/usr/bin/env python3
"""
Tests for Phase 06 Workstream 06.2: Trigger Translation (L4)

Tests cover:
- Trigger pattern recognition
- Supported subset translation
- Unsupported pattern detection
- Risk scoring
- Determinism
"""

import pytest
from core.translator import Translator, TranslateMode, ObjectType, RiskLevel


class TestTriggerPatternRecognition:
    """Test trigger pattern recognition."""

    def test_before_insert_normalize_recognized(self):
        """BEFORE INSERT with simple normalization is recognized as supported."""
        trigger_def = """
        CREATE TRIGGER trg_normalize_email
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_normalize_email', trigger_def)

        assert result.sql_output is not None
        assert result.risk_level == RiskLevel.LOW

    def test_before_update_normalize_recognized(self):
        """BEFORE UPDATE with simple normalization is recognized as supported."""
        trigger_def = """
        CREATE TRIGGER trg_normalize_username
        BEFORE UPDATE ON users
        FOR EACH ROW
        BEGIN
            :NEW.username := UPPER(:NEW.username);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_normalize_username', trigger_def)

        assert result.sql_output is not None
        assert result.risk_level == RiskLevel.LOW

    def test_after_insert_rejected(self):
        """AFTER INSERT triggers are not supported."""
        trigger_def = """
        CREATE TRIGGER trg_audit
        AFTER INSERT ON users
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log VALUES (:NEW.id, SYSDATE);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_audit', trigger_def)

        # Should generate stub (not supported)
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()
        assert result.risk_level == RiskLevel.CRITICAL

    def test_trigger_with_select_rejected(self):
        """Triggers with SELECT statements are not supported."""
        trigger_def = """
        CREATE TRIGGER trg_check_balance
        BEFORE INSERT ON transactions
        FOR EACH ROW
        DECLARE
            v_balance NUMBER;
        BEGIN
            SELECT balance INTO v_balance FROM accounts WHERE id = :NEW.account_id;
            IF v_balance < :NEW.amount THEN
                RAISE_APPLICATION_ERROR(-20001, 'Insufficient funds');
            END IF;
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_check_balance', trigger_def)

        # Should generate stub (not supported)
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()
        assert result.risk_level == RiskLevel.CRITICAL

    def test_trigger_with_loop_rejected(self):
        """Triggers with loops are not supported."""
        trigger_def = """
        CREATE TRIGGER trg_process_batch
        AFTER INSERT ON orders
        FOR EACH ROW
        BEGIN
            FOR rec IN (SELECT * FROM order_items WHERE order_id = :NEW.id) LOOP
                UPDATE inventory SET qty = qty - rec.qty WHERE product_id = rec.product_id;
            END LOOP;
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_process_batch', trigger_def)

        # Should generate stub (not supported)
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()
        assert result.risk_level == RiskLevel.CRITICAL

    def test_trigger_with_conditional_rejected(self):
        """Triggers with IF statements are not supported."""
        trigger_def = """
        CREATE TRIGGER trg_conditional
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            IF :NEW.email IS NULL THEN
                :NEW.email := 'default@example.com';
            END IF;
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_conditional', trigger_def)

        # Should generate stub (not supported)
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()


class TestTriggerTranslation:
    """Test trigger translation correctness."""

    def test_oracle_to_postgres_before_insert(self):
        """Oracle BEFORE INSERT trigger translates to Postgres."""
        trigger_def = """
        CREATE TRIGGER trg_normalize_email
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_normalize_email', trigger_def)

        assert result.sql_output is not None
        # Should convert :NEW to NEW (no colon)
        assert 'NEW.email' in result.sql_output
        assert ':NEW' not in result.sql_output
        # Should have Postgres function syntax
        assert 'RETURNS trigger' in result.sql_output or 'RETURN NEW' in result.sql_output

    def test_trim_function_supported(self):
        """TRIM function in normalization is supported."""
        trigger_def = """
        CREATE TRIGGER trg_trim_name
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.name := TRIM(:NEW.name);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_trim_name', trigger_def)

        assert result.sql_output is not None
        assert result.risk_level == RiskLevel.LOW
        assert 'TRIM' in result.sql_output or 'trim' in result.sql_output

    def test_multiple_statements_rejected(self):
        """Triggers with multiple statements are not supported."""
        trigger_def = """
        CREATE TRIGGER trg_multi
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
            :NEW.username := UPPER(:NEW.username);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_multi', trigger_def)

        # Should generate stub (not supported - multiple statements)
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()

    def test_computed_expression_rejected(self):
        """Triggers with computed expressions are not supported."""
        trigger_def = """
        CREATE TRIGGER trg_computed
        BEFORE INSERT ON orders
        FOR EACH ROW
        BEGIN
            :NEW.total := :NEW.quantity * :NEW.price;
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_computed', trigger_def)

        # Should generate stub (not supported - computed expression)
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()


class TestTriggerRiskScoring:
    """Test trigger risk level calculation."""

    def test_simple_normalization_low_risk(self):
        """Simple normalization triggers are LOW risk."""
        trigger_def = """
        CREATE TRIGGER trg_normalize
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_normalize', trigger_def)

        assert result.risk_level == RiskLevel.LOW

    def test_trigger_with_dml_critical_risk(self):
        """Triggers with DML operations are CRITICAL risk."""
        trigger_def = """
        CREATE TRIGGER trg_audit
        AFTER INSERT ON users
        FOR EACH ROW
        BEGIN
            INSERT INTO audit_log VALUES (:NEW.id, SYSDATE);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_audit', trigger_def)

        assert result.risk_level == RiskLevel.CRITICAL

    def test_trigger_with_loop_critical_risk(self):
        """Triggers with loops are CRITICAL risk."""
        trigger_def = """
        CREATE TRIGGER trg_loop
        AFTER INSERT ON orders
        FOR EACH ROW
        BEGIN
            FOR rec IN (SELECT * FROM order_items) LOOP
                NULL;
            END LOOP;
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_loop', trigger_def)

        assert result.risk_level == RiskLevel.CRITICAL


class TestTriggerStubGeneration:
    """Test trigger stub generation."""

    def test_unsupported_trigger_generates_stub(self):
        """Unsupported triggers generate safe stubs."""
        trigger_def = """
        CREATE TRIGGER trg_complex
        AFTER INSERT ON users
        FOR EACH ROW
        BEGIN
            IF :NEW.status = 'active' THEN
                INSERT INTO active_users VALUES (:NEW.id);
            END IF;
        END;
        """
        translator = Translator(mode=TranslateMode.STUB, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_complex', trigger_def)

        assert result.sql_output is not None
        assert 'STUB' in result.sql_output or 'stub' in result.sql_output.lower()
        assert 'RAISE EXCEPTION' in result.sql_output or 'Manual rewrite required' in result.sql_output

    def test_stub_has_critical_warning(self):
        """Trigger stubs have CRITICAL warning."""
        trigger_def = """
        CREATE TRIGGER trg_unsupported
        AFTER DELETE ON users
        FOR EACH ROW
        BEGIN
            DELETE FROM user_sessions WHERE user_id = :OLD.id;
        END;
        """
        translator = Translator(mode=TranslateMode.STUB, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_unsupported', trigger_def)

        assert result.risk_level == RiskLevel.CRITICAL
        assert len(result.warnings) > 0
        assert any(w.severity == RiskLevel.CRITICAL for w in result.warnings)


class TestTriggerDeterminism:
    """Test trigger translation determinism."""

    def test_translation_deterministic(self):
        """Same trigger definition produces identical output."""
        trigger_def = """
        CREATE TRIGGER trg_normalize
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """

        translator1 = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result1 = translator1.translate_object(ObjectType.TRIGGER, 'trg_normalize', trigger_def)

        translator2 = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result2 = translator2.translate_object(ObjectType.TRIGGER, 'trg_normalize', trigger_def)

        assert result1.sql_output == result2.sql_output
        assert result1.risk_level == result2.risk_level

    def test_multiple_triggers_deterministic_report(self):
        """Multiple triggers produce deterministic report ordering."""
        triggers = [
            ("trg_c", "CREATE TRIGGER trg_c BEFORE INSERT ON t1 FOR EACH ROW BEGIN :NEW.c := LOWER(:NEW.c); END;"),
            ("trg_a", "CREATE TRIGGER trg_a BEFORE INSERT ON t2 FOR EACH ROW BEGIN :NEW.a := UPPER(:NEW.a); END;"),
            ("trg_b", "CREATE TRIGGER trg_b AFTER INSERT ON t3 FOR EACH ROW BEGIN NULL; END;"),
        ]

        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        for name, defn in triggers:
            translator.translate_object(ObjectType.TRIGGER, name, defn)

        report = translator.get_report()

        # Warnings should be sorted deterministically (severity → object_name → message)
        if len(report['warnings']) > 1:
            # Check that sorting is deterministic by running twice
            warnings1 = [(w['severity'], w['object'], w['message']) for w in report['warnings']]

            # Re-translate to verify determinism
            translator2 = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
            for name, defn in triggers:
                translator2.translate_object(ObjectType.TRIGGER, name, defn)
            report2 = translator2.get_report()
            warnings2 = [(w['severity'], w['object'], w['message']) for w in report2['warnings']]

            # Both runs should produce identical ordering
            assert warnings1 == warnings2


class TestTriggerAnalyzeMode:
    """Test trigger behavior in analyze mode."""

    def test_analyze_mode_no_sql_output(self):
        """Analyze mode produces no SQL output for triggers."""
        trigger_def = """
        CREATE TRIGGER trg_normalize
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """
        translator = Translator(mode=TranslateMode.ANALYZE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_normalize', trigger_def)

        assert result.sql_output is None
        assert result.mode == TranslateMode.ANALYZE

    def test_analyze_mode_requires_manual_review(self):
        """Analyze mode adds manual review step."""
        trigger_def = """
        CREATE TRIGGER trg_test
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """
        translator = Translator(mode=TranslateMode.ANALYZE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_test', trigger_def)

        assert len(result.manual_steps) > 0
        assert any('Manual review required' in step.action for step in result.manual_steps)


class TestTriggerUnverifiedSyntaxWarning:
    """Test that triggers include unverified syntax warning (Gemini requirement)."""

    def test_translated_trigger_has_unverified_warning(self):
        """Translated triggers have 'Unverified Syntax' warning."""
        trigger_def = """
        CREATE TRIGGER trg_normalize
        BEFORE INSERT ON users
        FOR EACH ROW
        BEGIN
            :NEW.email := LOWER(:NEW.email);
        END;
        """
        translator = Translator(mode=TranslateMode.SUBSET_TRANSLATE, source_dialect='oracle', target_dialect='postgres')
        result = translator.translate_object(ObjectType.TRIGGER, 'trg_normalize', trigger_def)

        # Should have unverified syntax warning (Gemini concession requirement)
        assert any(
            'unverified' in w.message.lower() and 'compile-check' in w.reason.lower()
            for w in result.warnings
        )


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
