import pytest
from core.schema_ir import RoutineIR, RoutineCapability
from core.routine_translator import RoutineTranslator

class TestRoutineSubsetTranslation:
    
    def setup_method(self):
        self.translator = RoutineTranslator(source_dialect="oracle", target_dialect="postgres")

    def test_subset_translation_success(self):
        """Verify simple translation works"""
        ir = RoutineIR(
            name="simple_func",
            arguments=[],
            return_type="DATE",
            body_source="BEGIN RETURN SYSDATE; END;"
        )
        
        result = self.translator.process(ir, RoutineCapability.SUBSET_TRANSLATE)
        
        assert result.outcome == "TRANSLATED"
        assert "CURRENT_TIMESTAMP" in result.generated_code # SYSDATE replaced
        assert "SYSDATE" not in result.generated_code

    def test_subset_fallback_on_risk(self):
        """Verify fallback to stub if risk is high even if SUBSET requested"""
        ir = RoutineIR(
            name="risky_func",
            arguments=[],
            body_source="BEGIN EXECUTE IMMEDIATE 'foo'; END;"
        )
        
        result = self.translator.process(ir, RoutineCapability.SUBSET_TRANSLATE)
        
        assert result.outcome == "STUBBED" # Fallback
        assert any("Fallback to STUB" in w for w in result.warnings)
